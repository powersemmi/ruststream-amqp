//! End-to-end checks against a real `AMQP` 1.0 broker, gated behind `AMQP_TEST_URL`.
//!
//! Start one with `just brokers-up` (`ActiveMQ` Artemis), then:
//! `AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::Duration;

use fe2o3_amqp::{Connection, Sender, Session};
use fe2o3_amqp_types::messaging::Message;
use futures::StreamExt;
use ruststream::{
    AckError, Broker, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher,
    Subscriber,
};
use ruststream_amqp::{
    AmqpAddress, AmqpBroker, AmqpError, ConnectedAmqpBroker, PARTITION_KEY_HEADER, Sasl, Settle,
};

mod live;

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// The broker URL, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing one is a failure.
fn test_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

async fn connect(url: &str) -> ConnectedAmqpBroker {
    AmqpBroker::new(url)
        .container_id(format!("it-{}", std::process::id()))
        .connect()
        .await
        .expect("broker connects")
}

/// Per-test unique address, so runs do not observe each other's leftovers.
fn unique(name: &str) -> String {
    format!("it.{name}.{}", std::process::id())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roundtrip_preserves_payload_headers_and_partition_key() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("roundtrip");
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("subscription opens");

    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    headers.insert(PARTITION_KEY_HEADER, "user-42");
    let publisher = connected.publisher();
    publisher
        .publish(
            OutgoingMessage::new(&address, b"{\"id\":1}".as_slice()).with_headers(headers),
            None,
        )
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(message.payload(), b"{\"id\":1}");
    assert_eq!(
        message.headers().get_str("content-type"),
        Some("application/json")
    );
    assert_eq!(message.headers().get_str("x-tenant"), Some("acme"));
    assert_eq!(message.partition_key(), Some(b"user-42".as_slice()));
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_with_requeue_redelivers() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("requeue");
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&address, b"again".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    first.nack(true).await.expect("release succeeds");

    let second = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("redelivery arrives")
        .expect("stream is open")
        .expect("redelivery is ok");
    assert_eq!(second.payload(), b"again");
    second.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

/// What the broker counts, and what it does not. A requeue is `modified` with `delivery-failed`,
/// so the server counts the attempt and the redelivery carries a `header` section whose
/// `delivery-count` says so. A message this crate published carries no header section at all, and
/// a delivery nothing has counted an attempt for reports nothing rather than claiming to be the
/// first of a series: the framework's own retry-count header is what survives the copy a deferred
/// retry publishes, and it decides the attempt there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requeued_delivery_reports_the_broker_count() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("counted");
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new(&address, b"once".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let first = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(
        first.redelivery_count(),
        None,
        "a message published without a header section reaches the handler uncounted",
    );
    first.nack(true).await.expect("the requeue succeeds");

    let second = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("redelivery arrives")
        .expect("stream is open")
        .expect("redelivery is ok");
    assert_eq!(
        second.redelivery_count(),
        Some(2),
        "the broker counted the failed attempt, so this delivery is the second",
    );
    second.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nack_without_requeue_does_not_redeliver() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("drop");
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&address, b"poison".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let poison = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    poison.nack(false).await.expect("reject succeeds");

    // The follow-up message must be the next delivery; the rejected one must not come back.
    publisher
        .publish(OutgoingMessage::new(&address, b"next".as_slice()), None)
        .await
        .expect("publish succeeds");
    let next = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(next.payload(), b"next");
    next.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn at_most_once_reports_ack_unsupported() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("amo");
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(&address).settle(Settle::AtMostOnce))
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new(&address, b"fire".as_slice()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert!(matches!(message.ack().await, Err(AckError::Unsupported)));

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A publisher handed out for a scoped task (the shape `after_startup` and request/reply use) is
/// dropped while the application keeps running. Every publisher shares one session, so the links
/// it attached must be closed by the connection, not abandoned by the handle: an abandoned link
/// leaves the peer's echoing detach unroutable, which takes the shared session down and makes the
/// later shutdown fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_publisher_leaves_the_connection_usable() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("dropped-publisher");
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("subscription opens");

    {
        let scoped = connected.publisher();
        scoped
            .publish(OutgoingMessage::new(&address, b"scoped".as_slice()), None)
            .await
            .expect("publish succeeds");
    }

    // The round trip through the peer is the synchronisation point. It is bounded because a
    // session killed by the dropped link's unroutable detach echo never answers the attach at
    // all, so the failure has to be a timeout rather than a hang.
    let survivor = connected.publisher();
    tokio::time::timeout(
        RECV_TIMEOUT,
        survivor.publish(OutgoingMessage::new(&address, b"survivor".as_slice()), None),
    )
    .await
    .expect("the shared session still answers after the dropped publisher")
    .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    for expected in [b"scoped".as_slice(), b"survivor".as_slice()] {
        let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("delivery arrives")
            .expect("stream is open")
            .expect("delivery is ok");
        assert_eq!(message.payload(), expected);
        message.ack().await.expect("ack succeeds");
    }

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The address Artemis carries an undeliverable message to when the deployment configures no
/// other, which is the policy behind `nack(requeue = false)` on this stand.
const DEAD_LETTER_QUEUE: &str = "DLQ";

/// The crate documents a reject as terminal, with the broker's dead-letter policy deciding what
/// happens next, and the in-process stand-in has no such policy at all. On a server the decision
/// is real: the rejected delivery is carried to the dead-letter address rather than dropped, so a
/// service that rejects poison keeps it for an operator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_delivery_is_carried_to_the_dead_letter_address() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("dead-lettered");
    let payload = format!("poison-{}", std::process::id());
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("subscription opens");
    // Opened before the reject, so the delivery cannot land before anything is watching. The
    // dead-letter address is the deployment's, shared with every other case that rejects, so the
    // watcher settles on receipt: a reject issued from here would carry the message back to the
    // same address and the search would circle on it.
    let mut dead_letters = connected
        .subscribe_address(AmqpAddress::queue(DEAD_LETTER_QUEUE).settle(Settle::AtMostOnce))
        .await
        .expect("the dead-letter subscription opens");

    connected
        .publisher()
        .publish(OutgoingMessage::new(&address, payload.as_bytes()), None)
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let poison = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(poison.payload(), payload.as_bytes());
    poison.nack(false).await.expect("reject succeeds");

    // The queue is shared with every other case that rejects, so the search is for this payload.
    let mut dead_stream = pin!(dead_letters.stream());
    let found = tokio::time::timeout(RECV_TIMEOUT, async {
        loop {
            let delivery = dead_stream
                .next()
                .await
                .expect("the dead-letter stream is open");
            // A delivery another case rejected may carry a body this crate cannot decode; it is
            // settled on receipt either way, so the search simply moves past it.
            if delivery.is_ok_and(|message| message.payload() == payload.as_bytes()) {
                return;
            }
        }
    })
    .await;
    assert!(
        found.is_ok(),
        "a rejected delivery must reach {DEAD_LETTER_QUEUE}",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A body this crate has no byte form for is what a foreign `AMQP` peer produces, so the case
/// sends one with the bare client. The subscription reports [`AmqpError::UnsupportedBody`] and the
/// delivery is rejected rather than handed back: a message no handler can ever decode must not
/// circulate forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_with_no_byte_form_is_reported_and_not_redelivered() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("foreign-body");
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("subscription opens");

    let mut foreign = Connection::builder()
        .container_id(format!("foreign-{}", std::process::id()))
        .open(url.as_str())
        .await
        .expect("the foreign peer connects");
    let mut foreign_session = Session::begin(&mut foreign)
        .await
        .expect("the foreign session begins");
    let mut foreign_sender = Sender::attach(&mut foreign_session, "foreign-sender", &address)
        .await
        .expect("the foreign sender attaches");
    foreign_sender
        .send(Message::builder().value(42_i32).build())
        .await
        .expect("the foreign peer sends")
        .accepted_or_else(|outcome| outcome)
        .expect("the broker accepts the foreign message");

    let mut stream = pin!(subscriber.stream());
    let reported = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the undecodable delivery is reported")
        .expect("stream is open")
        .expect_err("a value body that is neither binary nor a string has no payload");
    assert!(
        matches!(reported, AmqpError::UnsupportedBody { .. }),
        "got {reported}",
    );

    // The next delivery must be the follow-up: the rejected one is gone, not requeued.
    connected
        .publisher()
        .publish(
            OutgoingMessage::new(&address, b"decodable".as_slice()),
            None,
        )
        .await
        .expect("publish succeeds");
    let next = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(next.payload(), b"decodable");
    next.ack().await.expect("ack succeeds");

    foreign_sender
        .close()
        .await
        .expect("the foreign sender closes");
    foreign_session
        .end()
        .await
        .expect("the foreign session ends");
    foreign
        .close()
        .await
        .expect("the foreign connection closes");
    connected.shutdown().await.expect("shutdown succeeds");
}

/// Splits `amqp://user:password@host:port` into the endpoint without the userinfo and the
/// credentials it carried, so the SASL cases can present them explicitly instead.
fn endpoint_and_credentials(url: &str) -> (String, String, String) {
    let (scheme, rest) = url.split_once("://").expect("the URL names a scheme");
    let (userinfo, host) = rest
        .rsplit_once('@')
        .expect("the live URL carries the stand's credentials");
    let (user, password) = userinfo
        .split_once(':')
        .expect("the userinfo carries a password");
    (
        format!("{scheme}://{host}"),
        user.to_owned(),
        password.to_owned(),
    )
}

/// The profile passed to [`AmqpBroker::sasl`] is what authenticates the connection, and the
/// endpoint here carries no credentials of its own to fall back on. The wrong password proves the
/// same in the other direction: the broker refuses, and the refusal is an error from `connect`
/// rather than a connection that fails later.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_explicit_sasl_profile_authenticates_and_a_wrong_password_is_refused() {
    let Some(url) = test_url() else { return };
    let (endpoint, user, password) = endpoint_and_credentials(&url);

    let connected = AmqpBroker::new(&endpoint)
        .container_id(format!("sasl-{}", std::process::id()))
        .sasl(Sasl::plain(&user, &password))
        .connect()
        .await
        .expect("an explicit PLAIN profile authenticates");

    let address = unique("sasl");
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(
            OutgoingMessage::new(&address, b"authenticated".as_slice()),
            None,
        )
        .await
        .expect("publish succeeds");
    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(message.payload(), b"authenticated");
    message.ack().await.expect("ack succeeds");
    connected.shutdown().await.expect("shutdown succeeds");

    let refused = AmqpBroker::new(&endpoint)
        .sasl(Sasl::plain(&user, format!("{password}-wrong")))
        .connect()
        .await
        .expect_err("a wrong password must not open a connection");
    assert!(matches!(refused, AmqpError::Connect(_)), "got {refused}");
}
