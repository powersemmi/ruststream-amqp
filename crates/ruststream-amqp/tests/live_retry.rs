//! The runtime's redelivery fallback, running against a real `AMQP` 1.0 broker.
//!
//! `AMQP` 1.0 has no delayed redelivery and no way to declare a node with a delivery limit and a
//! dead-letter address, so `retry_after`, `max_attempts` and `dead_letter` are all served by the
//! runtime publishing copies to the address the subscription reports. That answer is this crate's,
//! and in process there is nothing to get wrong about it: the stand-in routes by name. These cases
//! run the service against a server, where the copy has to reach a node the broker owns and come
//! back to the same consumer.
//!
//! Start one with `just brokers-up` (`ActiveMQ` Artemis), then:
//! `AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::runtime::RETRY_COUNT_HEADER;
use ruststream::{Broker, ConnectedBroker, IncomingMessage, Subscriber};
use ruststream_amqp::prelude::*;
use ruststream_amqp::{AmqpMessage, ConnectedAmqpBroker};
use serde::{Deserialize, Serialize};

mod live;

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// Short enough to keep the suite quick, long enough to be a delay the runtime actually waits out
/// against a broker on the loopback.
const RETRY_DELAY: Duration = Duration::from_millis(200);

/// How long a case waits before calling a further attempt absent. Several times the delay above,
/// so a copy the runtime was going to publish has been published.
const QUIET: Duration = Duration::from_millis(800);

/// The broker URL, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing one is a failure.
fn test_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

async fn connect(url: &str) -> ConnectedAmqpBroker {
    AmqpBroker::new(url)
        .container_id(format!("retry-watch-{}", std::process::id()))
        .connect()
        .await
        .expect("broker connects")
}

// The addresses are per process, so a stand a developer keeps running does not hand one run the
// leftovers of another. A `#[subscriber(..)]` argument is evaluated where the subscription opens,
// which is why these are functions rather than constants.
fn deferred_orders() -> String {
    format!("retry.deferred.orders.{}", std::process::id())
}

fn deferred_attempts() -> String {
    format!("retry.deferred.attempts.{}", std::process::id())
}

fn capped_orders() -> String {
    format!("retry.capped.orders.{}", std::process::id())
}

fn capped_attempts() -> String {
    format!("retry.capped.attempts.{}", std::process::id())
}

fn capped_dead() -> String {
    format!("retry.capped.dead.{}", std::process::id())
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// What a handler call reports about itself: which order it was handed, and which attempt this is
/// according to the framework's own retry-count header.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Outgoing)]
struct Attempt {
    id: u64,
    count: u64,
}

/// The attempt this delivery is, as the deferred copy carries it. A delivery nobody has deferred
/// yet carries no such header and is attempt zero.
fn attempt_of(headers: &HeaderMap) -> u64 {
    headers
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse().ok())
        .unwrap_or(0)
}

/// Defers the first delivery and settles the copy that comes back. The marker it publishes is how
/// the case sees the handler from outside the service.
#[subscriber(AmqpAddress::queue(deferred_orders()))]
async fn defer_once(
    order: &Order,
    ctx: &mut Context<'_>,
    Out(out): Out<impl Publisher>,
) -> HandlerOutcome {
    let count = attempt_of(ctx.headers());
    if out
        .message(&Attempt {
            id: order.id,
            count,
        })
        .to(deferred_attempts())
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::drop();
    }
    if count == 0 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

/// A handler that never settles: every delivery asks for another try, so only the registration's
/// declaration ends the circulation.
#[subscriber(AmqpAddress::queue(capped_orders()))]
async fn never_settles(
    order: &Order,
    ctx: &mut Context<'_>,
    Out(out): Out<impl Publisher>,
) -> HandlerOutcome {
    let count = attempt_of(ctx.headers());
    if out
        .message(&Attempt {
            id: order.id,
            count,
        })
        .to(capped_attempts())
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::retry_after(RETRY_DELAY)
}

/// The next message on a watching subscription, decoded.
async fn next_value<T, S>(stream: &mut S, what: &str) -> T
where
    T: serde::de::DeserializeOwned,
    S: Stream<Item = Result<AmqpMessage, AmqpError>> + Unpin,
{
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| panic!("{what} must arrive"))
        .unwrap_or_else(|| panic!("{what}: the stream closed"))
        .unwrap_or_else(|err| panic!("{what}: {err}"));
    let value = serde_json::from_slice(message.payload())
        .unwrap_or_else(|err| panic!("{what}: the payload does not decode: {err}"));
    message.ack().await.expect("ack succeeds");
    value
}

/// Asserts that nothing more arrives within [`QUIET`].
async fn stays_quiet<S>(stream: &mut S, what: &str)
where
    S: Stream<Item = Result<AmqpMessage, AmqpError>> + Unpin,
{
    if let Ok(extra) = tokio::time::timeout(QUIET, stream.next()).await {
        let payload = extra.map(|item| item.map(|message| message.payload().to_vec()));
        panic!("{what}, got {payload:?}");
    }
}

/// The deferred copy is published to the node the subscription reported and comes back to the
/// same handler, carrying the framework's retry count. Nothing about the node is this process's to
/// arrange here: the address a copy is sent to has to be one a broker routes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deferred_retry_comes_back_to_the_subscription_that_asked_for_it() {
    let Some(url) = test_url() else { return };
    let watcher = connect(&url).await;
    // Opened before the service starts, so no marker is published where nothing is listening.
    let mut markers = watcher
        .subscribe_address(AmqpAddress::queue(deferred_attempts()))
        .await
        .expect("the marker subscription opens");

    let app = RustStream::new(AppInfo::new("retry", "0.1.0"))
        .with_broker(AmqpBroker::new(url.clone()), |b| {
            b.include(defer_once).out(DefaultSlot, Publish).build();
        })
        .start()
        .await
        .expect("the service starts");

    watcher
        .publisher()
        .message(&Order { id: 7 })
        .to(deferred_orders())
        .publish()
        .await
        .expect("publish succeeds");

    let mut stream = pin!(markers.stream());
    assert_eq!(
        next_value::<Attempt, _>(&mut stream, "the first attempt").await,
        Attempt { id: 7, count: 0 },
        "the delivery the broker handed over carries no retry count",
    );
    assert_eq!(
        next_value::<Attempt, _>(&mut stream, "the deferred copy").await,
        Attempt { id: 7, count: 1 },
        "the copy comes back to the same handler, counted",
    );
    stays_quiet(&mut stream, "the settled copy ends the circulation").await;

    app.shutdown().await.expect("the service shuts down");
    watcher.shutdown().await.expect("shutdown succeeds");
}

/// The cap and the dead-letter destination are the runtime's on this broker, and both ends of them
/// are publishes to real nodes: the spent delivery is carried to the address the registration
/// named, and it does not come back to the handler a third time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_capped_registration_carries_the_spent_delivery_to_its_dead_letter_address() {
    let Some(url) = test_url() else { return };
    let watcher = connect(&url).await;
    let mut markers = watcher
        .subscribe_address(AmqpAddress::queue(capped_attempts()))
        .await
        .expect("the marker subscription opens");
    let mut dead_letters = watcher
        .subscribe_address(AmqpAddress::queue(capped_dead()))
        .await
        .expect("the dead-letter subscription opens");

    let app = RustStream::new(AppInfo::new("retry", "0.1.0"))
        .with_broker(AmqpBroker::new(url.clone()), |b| {
            b.include(never_settles)
                .max_attempts(nonzero!(2u32))
                .dead_letter(capped_dead())
                .out(DefaultSlot, Publish)
                .build();
        })
        .start()
        .await
        .expect("the service starts");

    watcher
        .publisher()
        .message(&Order { id: 9 })
        .to(capped_orders())
        .publish()
        .await
        .expect("publish succeeds");

    let mut marker_stream = pin!(markers.stream());
    for count in 0..2 {
        assert_eq!(
            next_value::<Attempt, _>(&mut marker_stream, "an attempt").await,
            Attempt { id: 9, count },
            "the cap is two deliveries, counting the first",
        );
    }

    let mut dead_stream = pin!(dead_letters.stream());
    assert_eq!(
        next_value::<Order, _>(&mut dead_stream, "the spent delivery").await,
        Order { id: 9 },
        "the delivery at the cap is carried to the destination the registration named",
    );
    stays_quiet(&mut marker_stream, "a spent delivery does not come back").await;

    app.shutdown().await.expect("the service shuts down");
    watcher.shutdown().await.expect("shutdown succeeds");
}
