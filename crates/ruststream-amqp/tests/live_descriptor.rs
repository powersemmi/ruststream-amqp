//! What a subscription descriptor's settings do on a real `AMQP` 1.0 broker.
//!
//! [`AmqpAddress`] carries three things a server decides: the terminus capability, which says
//! whether consumers on one address compete or each get a copy; the link credit, which is the
//! protocol's own flow control; and the delivery guarantee, where an at-most-once receiver settles
//! on receipt and the broker forgets the message. The in-process stand-in reproduces the terminus
//! and states that it has neither flow control nor storage, so these are the cases that hold the
//! crate's documentation to what a broker actually does.
//!
//! Start one with `just brokers-up` (`ActiveMQ` Artemis), then:
//! `AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscribe, Subscriber,
    nonzero,
};
use ruststream_amqp::{
    AmqpAddress, AmqpBroker, AmqpError, AmqpMessage, ConnectedAmqpBroker, Settle,
};

mod live;

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a case waits before calling a delivery absent. Long enough that anything the broker
/// was willing to hand over has arrived over a loopback connection, short enough that six cases
/// of it stay quick.
const QUIET: Duration = Duration::from_millis(500);

/// The broker URL, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing one is a failure.
fn test_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

async fn connect(url: &str) -> ConnectedAmqpBroker {
    AmqpBroker::new(url)
        .container_id(format!("descriptor-{}", std::process::id()))
        .connect()
        .await
        .expect("broker connects")
}

/// Per-test unique address, so runs do not observe each other's leftovers.
fn unique(name: &str) -> String {
    format!("desc.{name}.{}", std::process::id())
}

async fn publish(connected: &ConnectedAmqpBroker, address: &str, payloads: &[&[u8]]) {
    let publisher = connected.publisher();
    for payload in payloads {
        publisher
            .publish(OutgoingMessage::new(address, payload), None)
            .await
            .expect("publish succeeds");
    }
}

/// The next delivery, or a failure naming what was waited for.
async fn next_delivery<S>(stream: &mut S, what: &str) -> AmqpMessage
where
    S: Stream<Item = Result<AmqpMessage, AmqpError>> + Unpin,
{
    tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{what} must arrive"))
        .unwrap_or_else(|| panic!("{what}: the stream closed"))
        .unwrap_or_else(|err| panic!("{what}: {err}"))
}

/// The next delivery's payload, acknowledged so it leaves the broker.
async fn next_payload<S>(stream: &mut S, what: &str) -> Vec<u8>
where
    S: Stream<Item = Result<AmqpMessage, AmqpError>> + Unpin,
{
    let message = next_delivery(stream, what).await;
    let payload = message.payload().to_vec();
    message.ack().await.expect("ack succeeds");
    payload
}

/// Asserts that nothing more is delivered within [`QUIET`].
async fn stays_quiet<S>(stream: &mut S, what: &str)
where
    S: Stream<Item = Result<AmqpMessage, AmqpError>> + Unpin,
{
    if let Ok(extra) = tokio::time::timeout(QUIET, stream.next()).await {
        let payload = extra.map(|item| item.map(|message| message.payload().to_vec()));
        panic!("{what}, got {payload:?}");
    }
}

/// A `queue` terminus is the capability Artemis and its peers read as anycast, so two
/// subscriptions on one address share the traffic instead of each taking a copy. Without the
/// capability the deployment's own default decides, which is the bug this pins: a work queue that
/// silently fans out processes every order twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_consumers_share_a_queue_address() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("queue-anycast");
    let mut first = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("the first subscription opens");
    let mut second = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("the second subscription opens");

    let sent: [&[u8]; 4] = [b"a", b"b", b"c", b"d"];
    publish(&connected, &address, &sent).await;

    let mut first_stream = pin!(first.stream());
    let mut second_stream = pin!(second.stream());
    let mut seen: Vec<Vec<u8>> = Vec::new();
    while seen.len() < sent.len() {
        let message = tokio::time::timeout(RECV_TIMEOUT, async {
            tokio::select! {
                item = first_stream.next() => item,
                item = second_stream.next() => item,
            }
        })
        .await
        .expect("every message reaches one of the two consumers")
        .expect("the stream is open")
        .expect("the delivery is ok");
        seen.push(message.payload().to_vec());
        message.ack().await.expect("ack succeeds");
    }
    seen.sort();
    assert_eq!(
        seen,
        sent.iter().map(|p| p.to_vec()).collect::<Vec<_>>(),
        "every message must be delivered exactly once across the two consumers",
    );

    // A second copy is what a multicast terminus would produce, so its absence is the assertion.
    stays_quiet(&mut first_stream, "no message may be delivered twice").await;
    stays_quiet(&mut second_stream, "no message may be delivered twice").await;

    connected.shutdown().await.expect("shutdown succeeds");
}

/// A `topic` terminus is the capability read as multicast, so every subscription on the address
/// gets its own copy. This is the other half of the same claim: the constructor is the intent, and
/// the server honours it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_topic_subscription_gets_its_own_copy() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("topic-multicast");
    let mut first = connected
        .subscribe_address(AmqpAddress::topic(&address))
        .await
        .expect("the first subscription opens");
    let mut second = connected
        .subscribe_address(AmqpAddress::topic(&address))
        .await
        .expect("the second subscription opens");

    let sent: [&[u8]; 3] = [b"one", b"two", b"three"];
    publish(&connected, &address, &sent).await;

    let mut first_stream = pin!(first.stream());
    let mut second_stream = pin!(second.stream());
    for expected in sent {
        assert_eq!(
            next_payload(&mut first_stream, "the first subscriber's copy").await,
            expected,
            "a multicast subscription receives every message, in order",
        );
        assert_eq!(
            next_payload(&mut second_stream, "the second subscriber's copy").await,
            expected,
            "a multicast subscription receives every message, in order",
        );
    }
    stays_quiet(&mut first_stream, "the fan-out is three messages wide").await;
    stays_quiet(&mut second_stream, "the fan-out is three messages wide").await;

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The bare-name form of a subscription, which maps to [`AmqpAddress::raw`]: the name goes to the
/// broker verbatim, with no capability, so the node it opens is the one the service wrote and not
/// a variant of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_name_opens_the_node_it_spells() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("raw-verbatim");
    let mut subscriber = connected
        .subscribe(&address)
        .await
        .expect("the subscription opens");

    publish(&connected, &address, &[b"verbatim"]).await;
    let mut stream = pin!(subscriber.stream());
    assert_eq!(
        next_payload(&mut stream, "the delivery on the verbatim address").await,
        b"verbatim",
    );

    // A neighbouring node is a different node: nothing about the name is a pattern.
    publish(&connected, &format!("{address}.other"), &[b"elsewhere"]).await;
    stays_quiet(
        &mut stream,
        "a publish to another address must not arrive here",
    )
    .await;

    connected.shutdown().await.expect("shutdown succeeds");
}

/// Credit is the protocol's flow control and the client refreshes it on a disposition, so a
/// subscription that has not settled anything holds the broker to the count the descriptor named.
/// The stand-in has no counterpart for this - its subscriptions are unbounded - so a service that
/// sizes its prefetch has nothing but this case telling it the number reaches the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn credit_bounds_what_the_broker_hands_over_before_a_settlement() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("credit");
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(&address).credit(nonzero!(2)))
        .await
        .expect("the subscription opens");

    let sent: [&[u8]; 5] = [b"m0", b"m1", b"m2", b"m3", b"m4"];
    publish(&connected, &address, &sent).await;

    let mut stream = pin!(subscriber.stream());
    let first = next_delivery(&mut stream, "the first delivery").await;
    let second = next_delivery(&mut stream, "the second delivery").await;
    assert_eq!(first.payload(), b"m0");
    assert_eq!(second.payload(), b"m1");

    // Two deliveries are outstanding and the credit is spent, so the queue holds the rest.
    stays_quiet(&mut stream, "the link may not exceed its credit").await;

    first.ack().await.expect("ack succeeds");
    second.ack().await.expect("ack succeeds");

    for expected in &sent[2..] {
        assert_eq!(
            next_payload(&mut stream, "the delivery a settlement released").await,
            *expected,
            "settling refreshes the credit and the queue resumes, in order",
        );
    }

    connected.shutdown().await.expect("shutdown succeeds");
}

/// At-most-once settles on receipt, so the broker is done with the message the moment it hands it
/// over: the handler may never run, and nothing brings the delivery back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_at_most_once_delivery_leaves_the_broker_on_receipt() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("amo-drain");
    {
        let mut subscriber = connected
            .subscribe_address(AmqpAddress::queue(&address).settle(Settle::AtMostOnce))
            .await
            .expect("the subscription opens");
        publish(&connected, &address, &[b"fire"]).await;
        let mut stream = pin!(subscriber.stream());
        let message = next_delivery(&mut stream, "the at-most-once delivery").await;
        assert_eq!(message.payload(), b"fire");
        // Nothing settles it here: the receiver already did, on receipt.
    }

    let mut again = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("the second subscription opens");
    let mut stream = pin!(again.stream());
    stays_quiet(&mut stream, "a settled delivery does not come back").await;

    connected.shutdown().await.expect("shutdown succeeds");
}

/// The contrast that makes the case above a statement about settlement rather than about a lost
/// message: the same script under the default guarantee ends with the delivery back on the queue,
/// because the link detached without settling it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsettled_at_least_once_delivery_returns_to_the_queue() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;

    let address = unique("alo-return");
    {
        let mut subscriber = connected
            .subscribe_address(AmqpAddress::queue(&address))
            .await
            .expect("the subscription opens");
        publish(&connected, &address, &[b"unsettled"]).await;
        let mut stream = pin!(subscriber.stream());
        let message = next_delivery(&mut stream, "the at-least-once delivery").await;
        assert_eq!(message.payload(), b"unsettled");
        // Dropped without a disposition, which is what a crashing consumer looks like.
    }

    let mut again = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("the second subscription opens");
    let mut stream = pin!(again.stream());
    assert_eq!(
        next_payload(&mut stream, "the unsettled delivery returning").await,
        b"unsettled",
    );

    connected.shutdown().await.expect("shutdown succeeds");
}
