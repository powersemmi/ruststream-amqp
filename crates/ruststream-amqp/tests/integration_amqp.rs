//! End-to-end checks against a real `AMQP` 1.0 broker, gated behind `AMQP_TEST_URL`.
//!
//! Start one with `just brokers-up` (`ActiveMQ` Artemis), then:
//! `AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    AckError, Broker, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher,
    Subscriber,
};
use ruststream_amqp::{AmqpAddress, AmqpBroker, ConnectedAmqpBroker, PARTITION_KEY_HEADER, Settle};

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
