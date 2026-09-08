//! The production publish policies, mounted on the in-process broker.
//!
//! A routes file names policies (`.out(Reply, Publish)`), and a handler body names the capability
//! its slot needs. Both spellings must reach the stand-in unchanged, or the wiring a service ships
//! is not the wiring its tests run. These cases mount each of them through `TestApp` and assert the
//! behaviour behind them: what a committed transaction publishes, what an aborted one does not, and
//! what a request gets back.
//!
//! The framework's own contract suites for the same capabilities run against this broker in
//! `conformance_amqp.rs`; what is here is the mount site, which those suites do not exercise.

#![cfg(feature = "testing")]

use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::testing::TestApp;
use ruststream_amqp::AmqpError;
use ruststream_amqp::prelude::*;
use ruststream_amqp::testing::AmqpTestBroker;
use serde::{Deserialize, Serialize};

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

// The mount site a routes file writes in production, on the stand-in unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_production_policy_carries_the_reply() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(AmqpTestBroker::new(), |b| {
            b.include(confirm).out(Reply, Publish);
        });
    let app = TestApp::start(app).await.expect("startup failed");

    app.broker::<AmqpTestBroker>()
        .publish("orders", &Order { id: 1 })
        .await
        .expect("publish failed");

    app.broker::<AmqpTestBroker>()
        .published::<Order>("confirmations")
        .assert_called_once()
        .with(&Order { id: 1 });

    app.shutdown().await.expect("shutdown failed");
}

#[subscriber(AmqpAddress::queue("orders.default"), publish("confirmations.default"))]
async fn confirm_by_default(order: &Order) -> Result<Order, HandlerOutcome> {
    Ok(Order { id: order.id })
}

// A mount site that names no publisher falls back to the broker's default policy, which is the
// production one here as well - so the fallback is not a different code path in a test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_default_reply_publisher_is_the_production_policy() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(AmqpTestBroker::new(), |b| {
            b.include(confirm_by_default);
        });
    let app = TestApp::start(app).await.expect("startup failed");

    app.broker::<AmqpTestBroker>()
        .publish("orders.default", &Order { id: 2 })
        .await
        .expect("publish failed");

    app.broker::<AmqpTestBroker>()
        .published::<Order>("confirmations.default")
        .assert_called_once()
        .with(&Order { id: 2 });

    app.shutdown().await.expect("shutdown failed");
}

#[cfg(feature = "transaction")]
mod transactional {
    use super::Order;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;
    use ruststream_amqp::testing::AmqpTestBroker;
    use serde::{Deserialize, Serialize};

    /// Whether the handler settles its batch with a commit or an abort, so one handler covers both
    /// halves of the contract from the same mount.
    #[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Outgoing)]
    struct Batch {
        keep: bool,
    }

    #[subscriber(AmqpAddress::queue("batches"))]
    async fn post_batch(
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
        let app = RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
            AmqpTestBroker::new(),
            |b| {
                b.include(post_batch)
                    .out(DefaultSlot, TransactionalPublish)
                    .build();
            },
        );
        let app = TestApp::start(app).await.expect("startup failed");

        app.broker::<AmqpTestBroker>()
            .publish("batches", &Batch { keep: true })
            .await
            .expect("publish failed");
        let committed: Vec<Order> = app
            .broker::<AmqpTestBroker>()
            .published::<Order>("ledger")
            .decoded();
        assert_eq!(
            committed,
            vec![Order { id: 0 }, Order { id: 1 }],
            "a commit must publish the whole buffer, in publish order",
        );

        app.broker::<AmqpTestBroker>()
            .publish("batches", &Batch { keep: false })
            .await
            .expect("publish failed");
        let after_abort: Vec<Order> = app
            .broker::<AmqpTestBroker>()
            .published::<Order>("ledger")
            .decoded();
        assert_eq!(
            after_abort, committed,
            "an aborted batch must add nothing to what the ledger already had",
        );

        app.broker::<AmqpTestBroker>()
            .subscriber("batches")
            .assert_called(2)
            .settled(HandlerOutcome::ack());

        app.shutdown().await.expect("shutdown failed");
    }
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

// The responder is the example's, unchanged; the requester is the production publisher, which now
// carries `RequestReply` in process too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_reaches_a_mounted_responder_and_comes_back_correlated() {
    let broker = AmqpTestBroker::new();
    // A second handle on the same transport: the app owns one end of the ladder, the test the
    // other, as an external requester would be.
    let requester = broker.clone().connect().await.expect("connect failed");

    let app = RustStream::new(AppInfo::new("greeter", "0.1.0")).with_broker(broker, |b| {
        b.include(greet).out(DefaultSlot, Publish).build();
    });
    let app = TestApp::start(app).await.expect("startup failed");

    let reply = requester
        .publisher()
        .request(OutgoingMessage::new("greeter", b"world".as_slice()), WAIT)
        .await
        .expect("the mounted responder must answer");
    assert_eq!(reply.payload(), b"hello, world");

    app.shutdown().await.expect("shutdown failed");
}

// The negative half: nothing answers, so the request must fail once its timeout elapses rather
// than hang or resolve with an unrelated message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_nobody_answers_times_out() {
    let broker = AmqpTestBroker::new()
        .connect()
        .await
        .expect("connect failed");

    let err = broker
        .publisher()
        .request(OutgoingMessage::new("nobody", b"ping".as_slice()), MISS)
        .await
        .expect_err("an unanswered request must not resolve");
    assert!(matches!(err, AmqpError::RequestTimeout), "got {err}");
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

// A handler body that binds the request capability mounts on the stand-in, and the timeout it
// settles on is the stand-in's, not a stub that always succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_binding_the_request_capability_mounts() {
    let app =
        RustStream::new(AppInfo::new("relay", "0.1.0")).with_broker(AmqpTestBroker::new(), |b| {
            b.include(relay).out(DefaultSlot, Publish).build();
        });
    let app = TestApp::start(app).await.expect("startup failed");

    app.broker::<AmqpTestBroker>()
        .publish("relays", &Order { id: 3 })
        .await
        .expect("publish failed");

    app.broker::<AmqpTestBroker>()
        .subscriber("relays")
        .assert_called_once()
        .settled(HandlerOutcome::drop());

    app.shutdown().await.expect("shutdown failed");
}
