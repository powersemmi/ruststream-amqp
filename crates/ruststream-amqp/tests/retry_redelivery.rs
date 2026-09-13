//! Where a deferred `retry_after` lands on this broker.
//!
//! `AMQP` 1.0 has no delayed redelivery of its own, so `HandlerOutcome::retry_after` is the
//! framework's fallback: the delivery is dropped and a copy is published once the delay is over,
//! to the address the subscription reports. This crate reports the node address, because one
//! `AMQP` node is both what a receiver attaches to and what a sender publishes to. These cases
//! hold that answer to its promise from the service's side - the copy has to come back to the
//! same handler - for both ways a subscription is named.

#![cfg(feature = "testing")]

use std::time::Duration;

// The derive and the value a transform edits share the name in different namespaces: the
// derive is the macro `ruststream::Outgoing` the prelude carries, the value is the type
// `ruststream::runtime::Outgoing`.
use ruststream::runtime::{Outgoing, RETRY_COUNT_HEADER, SlotContext};
use ruststream::testing::{Outcome, TestApp};
use ruststream_amqp::prelude::*;
use ruststream_amqp::testing::AmqpTestBroker;
use serde::{Deserialize, Serialize};

const RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// Defers the first delivery and acks the copy that comes back, so the outcomes say whether the
/// copy arrived at all. The retry count only appears on a copy the fallback published.
fn defer_once(order: &Order, headers: &HeaderMap) -> HandlerOutcome {
    let attempt = headers
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .unwrap_or(0);
    assert_eq!(order.id, 1, "only the deferred order is published here");
    if attempt == 0 {
        HandlerOutcome::retry_after(RETRY_DELAY)
    } else {
        HandlerOutcome::ack()
    }
}

#[subscriber(AmqpAddress::queue("retry.orders"))]
async fn from_a_descriptor(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    defer_once(order, ctx.headers())
}

/// The descriptor's answer: a publish to the node address reaches the subscription opened on it,
/// so the deferred copy comes back to the handler that asked for the delay.
#[tokio::test(start_paused = true)]
async fn a_deferred_retry_comes_back_to_a_descriptor_subscription() {
    let app =
        RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(AmqpTestBroker::new(), |b| {
            b.include(from_a_descriptor).out_retry(Publish);
        });
    let app = TestApp::start(app).await.expect("startup failed");

    app.broker::<AmqpTestBroker>()
        .publish("retry.orders", &Order { id: 1 })
        .await
        .expect("publish failed");

    // The delay is real: nothing comes back before it elapses.
    app.advance(RETRY_DELAY.saturating_sub(Duration::from_millis(1)))
        .await
        .expect("the run settles");
    app.broker::<AmqpTestBroker>()
        .subscriber("retry.orders")
        .assert_called_once();

    app.advance(Duration::from_millis(1))
        .await
        .expect("the run settles");
    assert_eq!(
        app.broker::<AmqpTestBroker>()
            .subscriber("retry.orders")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the deferred copy must reach the handler and settle",
    );
}

#[subscriber("retry.by-name")]
async fn from_a_name(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    defer_once(order, ctx.headers())
}

/// The bare-name answer, which is the connected broker's rather than the descriptor's: a name is a
/// verbatim address on this broker, so `#[subscriber("name")]` takes the retry position too.
#[tokio::test(start_paused = true)]
async fn a_deferred_retry_comes_back_to_a_named_subscription() {
    let app =
        RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(AmqpTestBroker::new(), |b| {
            b.include(from_a_name).out_retry(Publish);
        });
    let app = TestApp::start(app).await.expect("startup failed");

    app.broker::<AmqpTestBroker>()
        .publish("retry.by-name", &Order { id: 1 })
        .await
        .expect("publish failed");

    app.advance(RETRY_DELAY).await.expect("the run settles");
    assert_eq!(
        app.broker::<AmqpTestBroker>()
            .subscriber("retry.by-name")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the deferred copy must reach the handler and settle",
    );
}

/// Stamps every copy leaving the position it is mounted on with that position's name. The retry
/// position is an `Out` slot, so its transforms read a slot's view; the transform sets no
/// per-message setting, so it stays generic over the options type and mounts on any publisher.
#[derive(Debug, Clone, Copy)]
struct DeferredStamp;

impl<Options> PublishTransform<ForSlot, Options> for DeferredStamp {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, cx: &SlotContext<'_>) {
        out.headers_mut()
            .insert("x-left-through", cx.slot().to_owned());
    }
}

#[subscriber(AmqpAddress::queue("retry.stamped"))]
async fn from_a_stamped_position(order: &Order, ctx: &mut Context<'_>) -> HandlerOutcome {
    defer_once(order, ctx.headers())
}

/// The deferred copy has no call site of its own, so a transform on the retry position is the one
/// place a service reaches it. The copy still lands on the subscription's own address, stamped.
#[tokio::test(start_paused = true)]
async fn a_transform_on_the_retry_position_stamps_the_deferred_copy() {
    let app =
        RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(AmqpTestBroker::new(), |b| {
            b.include(from_a_stamped_position)
                .out_retry(Publish)
                .transform(DeferredStamp);
        });
    let app = TestApp::start(app).await.expect("startup failed");

    app.broker::<AmqpTestBroker>()
        .publish("retry.stamped", &Order { id: 1 })
        .await
        .expect("publish failed");
    app.advance(RETRY_DELAY).await.expect("the run settles");

    app.broker::<AmqpTestBroker>()
        .published::<Order>("retry.stamped")
        .with_header("x-left-through", "Retry");
    assert_eq!(
        app.broker::<AmqpTestBroker>()
            .subscriber("retry.stamped")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the stamped copy must still reach the handler and settle",
    );
}
