//! The service's publish policies, in its own app under the harness.
//!
//! A routes file names policies (`.out_reply(Publish)`), and a handler body names the capability
//! its slot needs. These cases mount each of them in one app, run it through `TestApp` with the
//! broker in process, and assert the behaviour behind them: what a committed transaction
//! publishes, what an aborted one does not, and what a request gets back.
//!
//! The framework's own contract suites for the same capabilities run against this broker in
//! `conformance_amqp.rs`; what is here is the mount site, which those suites do not exercise.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::testing::{InProcess, TestApp};
use ruststream::{Broker, ConnectedBroker, OutgoingMessage};
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

/// The broker's address; the in-process mode dials nothing.
const URL: &str = "amqp://broker.example.com:5672";

const WAIT: Duration = Duration::from_secs(1);
/// Long enough that a reply in this process would have arrived, short enough to keep the negative
/// case quick.
const MISS: Duration = Duration::from_millis(150);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber(AmqpAddress::queue("orders"), publish("confirmations"))]
async fn confirm(order: &Order) -> Result<Order, HandlerOutcome> {
    Ok(Order { id: order.id })
}

#[subscriber(AmqpAddress::queue("orders.default"), publish("confirmations.default"))]
async fn confirm_by_default(order: &Order) -> Result<Order, HandlerOutcome> {
    Ok(Order { id: order.id })
}

/// The request payload rides the byte lane, as it does in the request/reply example.
#[derive(Deserialized)]
struct Who<'a>(&'a [u8]);

/// The reply carries its own bytes and names no destination, so the per-request reply address
/// fills the `to(..)` position.
#[derive(Outgoing, Serialized)]
struct Greeting(Vec<u8>);

#[subscriber(AmqpAddress::queue("greeter"))]
async fn greet(
    who: &Who<'_>,
    ctx: &mut Context<'_>,
    Out(out): Out<impl Publisher>,
) -> HandlerOutcome {
    let Some(reply_to) = ctx.headers().reply_to().map(str::to_owned) else {
        return HandlerOutcome::drop();
    };
    let mut headers = HeaderMap::new();
    if let Some(correlation_id) = ctx.headers().correlation_id() {
        headers.insert("correlation-id", correlation_id.to_owned());
    }
    let greeting = Greeting(format!("hello, {}", String::from_utf8_lossy(who.0)).into_bytes());
    if out
        .message(&greeting)
        .to(reply_to)
        .with_headers(headers)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[subscriber(AmqpAddress::queue("relays"))]
async fn relay(order: &Order, Out(out): Out<impl RequestReply>) -> HandlerOutcome {
    let _ = order.id;
    // Nothing is mounted on the requested address, so this handler settles on the timeout: the
    // point is that the slot binds and the failure surfaces, in process as on a server.
    match out
        .request(OutgoingMessage::new("nobody", b"ping".as_slice()), MISS)
        .await
    {
        Ok(_) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::drop(),
    }
}

#[subscriber(AmqpAddress::queue("relays.live"))]
async fn relay_to_greeter(order: &Order, Out(out): Out<impl RequestReply>) -> HandlerOutcome {
    let _ = order.id;
    match out
        .request(OutgoingMessage::new("greeter", b"world".as_slice()), WAIT)
        .await
    {
        Ok(reply) if reply.payload() == b"hello, world" => HandlerOutcome::ack(),
        _ => HandlerOutcome::drop(),
    }
}

/// The service's app, on the broker it is handed.
fn app_on(broker: AmqpBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(confirm).out_reply(Publish);
        // No publisher named: the broker's default policy carries the reply.
        b.include(confirm_by_default);
        b.include(greet).out(DefaultSlot, Publish).build();
        b.include(relay).out(DefaultSlot, Publish).build();
        b.include(relay_to_greeter)
            .out(DefaultSlot, Publish)
            .build();
        #[cfg(feature = "transaction")]
        b.include(transactional::post_batch)
            .out(DefaultSlot, TransactionalPublish)
            .build();
    })
}

/// The service's app, the one `main` runs.
fn app() -> RustStream {
    app_on(AmqpBroker::new(URL))
}

// The mount site a routes file writes in production.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_production_policy_carries_the_reply() {
    let tb = TestApp::start(app()).await.expect("startup failed");

    tb.broker::<AmqpBroker>()
        .message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish failed");

    tb.broker::<AmqpBroker>()
        .published::<Order>("confirmations")
        .assert_called_once()
        .with(&Order { id: 1 });

    tb.shutdown().await.expect("shutdown failed");
}

// A mount site that names no publisher falls back to the broker's default policy.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_default_reply_publisher_is_the_production_policy() {
    let tb = TestApp::start(app()).await.expect("startup failed");

    tb.broker::<AmqpBroker>()
        .message(&Order { id: 2 })
        .to("orders.default")
        .publish()
        .await
        .expect("publish failed");

    tb.broker::<AmqpBroker>()
        .published::<Order>("confirmations.default")
        .assert_called_once()
        .with(&Order { id: 2 });

    tb.shutdown().await.expect("shutdown failed");
}

#[cfg(feature = "transaction")]
mod transactional {
    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::{Order, app};

    /// Whether the handler settles its batch with a commit or an abort, so one handler covers both
    /// halves of the contract from the same mount.
    #[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Outgoing)]
    pub(super) struct Batch {
        pub(super) keep: bool,
    }

    #[subscriber(AmqpAddress::queue("batches"))]
    pub(super) async fn post_batch(
        batch: &Batch,
        Out(out): Out<impl TransactionalPublisher>,
    ) -> HandlerOutcome {
        if out.begin_transaction().await.is_err() {
            return HandlerOutcome::drop();
        }
        for id in 0..2 {
            if out
                .message(&Order { id })
                .to("ledger")
                .publish()
                .await
                .is_err()
            {
                let _ = out.abort().await;
                return HandlerOutcome::drop();
            }
        }
        let settled = if batch.keep {
            out.commit().await
        } else {
            out.abort().await
        };
        if settled.is_err() {
            return HandlerOutcome::drop();
        }
        HandlerOutcome::ack()
    }

    // The handler binds the capability, the mount site binds the policy that carries it, and the
    // ledger shows only what was committed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_commit_publishes_and_an_abort_publishes_nothing() {
        let tb = TestApp::start(app()).await.expect("startup failed");

        tb.broker::<AmqpBroker>()
            .message(&Batch { keep: true })
            .to("batches")
            .publish()
            .await
            .expect("publish failed");
        let committed: Vec<Order> = tb
            .broker::<AmqpBroker>()
            .published::<Order>("ledger")
            .decoded();
        assert_eq!(
            committed,
            vec![Order { id: 0 }, Order { id: 1 }],
            "a commit must publish the whole buffer, in publish order",
        );

        tb.broker::<AmqpBroker>()
            .message(&Batch { keep: false })
            .to("batches")
            .publish()
            .await
            .expect("publish failed");
        let after_abort: Vec<Order> = tb
            .broker::<AmqpBroker>()
            .published::<Order>("ledger")
            .decoded();
        assert_eq!(
            after_abort, committed,
            "an aborted batch must add nothing to what the ledger already had",
        );

        tb.broker::<AmqpBroker>()
            .subscriber("batches")
            .assert_called(2)
            .settled(HandlerOutcome::ack());

        tb.shutdown().await.expect("shutdown failed");
    }
}

// The requester is a publisher the test took off the service's broker before the harness
// connected it, as an external client of the service would hold one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_reaches_a_mounted_responder_and_comes_back_correlated() {
    let broker = AmqpBroker::new(URL);
    let requester = broker.publisher();
    let tb = TestApp::start(app_on(broker))
        .await
        .expect("startup failed");

    let reply = requester
        .request(OutgoingMessage::new("greeter", b"world".as_slice()), WAIT)
        .await
        .expect("the mounted responder must answer");
    assert_eq!(reply.payload(), b"hello, world");

    tb.shutdown().await.expect("shutdown failed");
}

// A handler body that binds the request capability mounts, and the timeout it settles on is the
// transport's, not a stub that always succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_binding_the_request_capability_settles_on_the_timeout() {
    let tb = TestApp::start(app()).await.expect("startup failed");

    tb.broker::<AmqpBroker>()
        .message(&Order { id: 3 })
        .to("relays")
        .publish()
        .await
        .expect("publish failed");

    tb.broker::<AmqpBroker>()
        .subscriber("relays")
        .assert_called_once()
        .settled(HandlerOutcome::drop());

    tb.shutdown().await.expect("shutdown failed");
}

// A whole exchange inside one run: a handler requests, another handler answers, and the harness
// still reaches a standstill - the reply is counted in flight and consumed like any other
// delivery, so a service doing request/reply from a handler is testable at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_can_complete_a_request_against_a_handler_next_to_it() {
    let tb = TestApp::start(app()).await.expect("startup failed");

    tb.broker::<AmqpBroker>()
        .message(&Order { id: 4 })
        .to("relays.live")
        .publish()
        .await
        .expect("the exchange must drive the run to a standstill");

    tb.broker::<AmqpBroker>()
        .subscriber("relays.live")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    tb.broker::<AmqpBroker>()
        .subscriber("greeter")
        .assert_called_once()
        .settled(HandlerOutcome::ack());

    tb.shutdown().await.expect("shutdown failed");
}

// The negative half at the transport: nothing answers, so the request must fail once its timeout
// elapses rather than hang or resolve with an unrelated message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_nobody_answers_times_out() {
    let broker = AmqpBroker::new(URL)
        .connect_in_process()
        .await
        .expect("connect failed");

    let err = broker
        .publisher()
        .request(OutgoingMessage::new("nobody", b"ping".as_slice()), MISS)
        .await
        .expect_err("an unanswered request must not resolve");
    assert!(matches!(err, AmqpError::RequestTimeout), "got {err}");
}

// A clone connected in process holds the test transport in the shared cell; a live connect of
// another clone is refused rather than reported as a connection that never opened a socket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_connect_after_an_in_process_one_is_refused() {
    let broker = AmqpBroker::new(URL);
    let live = broker.clone();
    let _in_process = broker.connect_in_process().await.expect("connect failed");

    let refused = live.connect().await;
    assert!(
        matches!(refused, Err(AmqpError::Connect(_))),
        "got {refused:?}"
    );
}

// The ladder makes the owner's misuse a compile error; what stays checkable at runtime is a handle
// that aliases the transport. Both ends of its life must refuse, because a live publisher does: it
// has no connection to send on before `connect`, and none after `shutdown`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aliasing_publisher_refuses_outside_the_connection() {
    let broker = AmqpBroker::new(URL);
    let early = broker.publisher();

    let before = early
        .publish(OutgoingMessage::new("orders", b"early".as_slice()), None)
        .await
        .expect_err("a publish before connect must not report success");
    assert!(matches!(before, AmqpError::NotConnected), "got {before}");

    let connected = broker.connect_in_process().await.expect("connect failed");
    early
        .publish(OutgoingMessage::new("orders", b"live".as_slice()), None)
        .await
        .expect("the same handle publishes once the broker is connected");

    connected.shutdown().await.expect("shutdown failed");
    let after = early
        .publish(OutgoingMessage::new("orders", b"late".as_slice()), None)
        .await
        .expect_err("a publish after shutdown must not report success");
    assert!(matches!(after, AmqpError::NotConnected), "got {after}");
}

// A sender with no target address is an anonymous one, and the peer rejects a message that names
// no destination: the in-process transport refuses it the same way rather than logging it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_to_the_empty_address_is_refused() {
    let broker = AmqpBroker::new(URL)
        .connect_in_process()
        .await
        .expect("connect failed");

    let err = broker
        .publisher()
        .publish(OutgoingMessage::new("", b"nowhere".as_slice()), None)
        .await
        .expect_err("a message to the empty address must not be accepted");
    assert!(
        matches!(err, AmqpError::PublishNotAccepted { .. }),
        "got {err}"
    );
}

// A transaction is declared over the connection, so one begun after the shutdown fails, as on the
// live publisher.
#[cfg(feature = "transaction")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_begun_after_shutdown_is_refused() {
    let broker = AmqpBroker::new(URL)
        .connect_in_process()
        .await
        .expect("connect failed");
    let publisher = broker.transactional_publisher();
    broker.shutdown().await.expect("shutdown failed");

    let err = publisher
        .begin_transaction()
        .await
        .expect_err("a transaction on a closed connection must not begin");
    assert!(matches!(err, AmqpError::NotConnected), "got {err}");
}

// The in-process mode reads the broker's URL as `connect` does, so a broker the service could not
// connect is not one a test connects either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_url_connect_refuses_is_refused_in_process() {
    for url in ["not a url", "http://broker.example.com:5672"] {
        let err = AmqpBroker::new(url)
            .connect_in_process()
            .await
            .expect_err("a URL the client cannot open must not connect");
        assert!(matches!(err, AmqpError::Connect(_)), "{url}: got {err}");
    }
}
