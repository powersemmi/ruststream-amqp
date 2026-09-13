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
use ruststream::runtime::{Outgoing, PublishContext, RETRY_COUNT_HEADER};
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

/// Stamps every copy with the subscription the delivery came from. A transform on the retry
/// position reads the delivery being retried, the way a reply's does; the transform sets no
/// per-message setting, so it stays generic over the options type and mounts on any publisher.
#[derive(Debug, Clone, Copy)]
struct DeferredStamp;

impl<C, Options> PublishTransform<ForReply<C>, Options> for DeferredStamp {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut()
            .insert("x-retried-from", cx.name().to_owned());
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
        .with_header("x-retried-from", "retry.stamped");
    assert_eq!(
        app.broker::<AmqpTestBroker>()
            .subscriber("retry.stamped")
            .outcomes(),
        [Outcome::Nack, Outcome::Ack],
        "the stamped copy must still reach the handler and settle",
    );
}

/// A handler that never settles: every delivery asks for another try, so nothing but the
/// registration's declaration ends the circulation.
fn never_settles(order: &Order) -> HandlerOutcome {
    assert_eq!(order.id, 1, "only the capped order is published here");
    HandlerOutcome::retry_after(RETRY_DELAY)
}

#[subscriber(AmqpAddress::queue("capped.orders"))]
async fn capped_from_a_descriptor(order: &Order) -> HandlerOutcome {
    never_settles(order)
}

/// The declaration ends a poison message on this broker: three deliveries, then the delivery is
/// carried to the dead-letter address instead of coming back a fourth time.
#[tokio::test(start_paused = true)]
async fn a_capped_registration_dead_letters_the_spent_delivery() {
    let app =
        RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(AmqpTestBroker::new(), |b| {
            b.include(capped_from_a_descriptor)
                .max_attempts(nonzero!(3u32))
                .dead_letter("capped.dead");
        });
    let app = TestApp::start(app).await.expect("startup failed");

    app.broker::<AmqpTestBroker>()
        .publish("capped.orders", &Order { id: 1 })
        .await
        .expect("publish failed");

    // Two delays carry the delivery from its first attempt to its third; the third is at the cap,
    // so it leaves for the dead-letter address at once rather than after another delay.
    app.advance(RETRY_DELAY).await.expect("the run settles");
    app.advance(RETRY_DELAY).await.expect("the run settles");

    app.broker::<AmqpTestBroker>()
        .subscriber("capped.orders")
        .assert_called(3);
    app.broker::<AmqpTestBroker>()
        .published::<Order>("capped.dead")
        .assert_called_once()
        .with(&Order { id: 1 });

    // And it stays gone: a fourth delay brings nothing back.
    app.advance(RETRY_DELAY).await.expect("the run settles");
    app.broker::<AmqpTestBroker>()
        .subscriber("capped.orders")
        .assert_called(3);
}

#[subscriber("capped.by-name")]
async fn capped_from_a_name(order: &Order) -> HandlerOutcome {
    never_settles(order)
}

/// The bare-name form takes the same declaration: a name is a verbatim address here, so its
/// copies are this process's to publish and the dead-letter destination is reached the same way.
#[tokio::test(start_paused = true)]
async fn a_named_subscription_takes_the_same_declaration() {
    let app =
        RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(AmqpTestBroker::new(), |b| {
            b.include(capped_from_a_name)
                .max_attempts(nonzero!(2u32))
                .dead_letter("capped.dead");
        });
    let app = TestApp::start(app).await.expect("startup failed");

    app.broker::<AmqpTestBroker>()
        .publish("capped.by-name", &Order { id: 1 })
        .await
        .expect("publish failed");
    app.advance(RETRY_DELAY).await.expect("the run settles");

    app.broker::<AmqpTestBroker>()
        .subscriber("capped.by-name")
        .assert_called(2);
    app.broker::<AmqpTestBroker>()
        .published::<Order>("capped.dead")
        .assert_called_once()
        .with(&Order { id: 1 });
}

#[subscriber(AmqpAddress::queue("rejected.orders"))]
async fn capped_without_a_destination(order: &Order) -> HandlerOutcome {
    never_settles(order)
}

/// A cap with no destination rejects the spent delivery instead of republishing it, which leaves
/// the broker's own dead-letter policy in play where the deployment configured one.
#[tokio::test(start_paused = true)]
async fn a_cap_without_a_destination_stops_the_circulation() {
    let app =
        RustStream::new(AppInfo::new("retry", "0.1.0")).with_broker(AmqpTestBroker::new(), |b| {
            b.include(capped_without_a_destination)
                .max_attempts(nonzero!(2u32));
        });
    let app = TestApp::start(app).await.expect("startup failed");

    app.broker::<AmqpTestBroker>()
        .publish("rejected.orders", &Order { id: 1 })
        .await
        .expect("publish failed");
    app.advance(RETRY_DELAY).await.expect("the run settles");
    app.advance(RETRY_DELAY).await.expect("the run settles");

    assert_eq!(
        app.broker::<AmqpTestBroker>()
            .subscriber("rejected.orders")
            .outcomes(),
        [Outcome::Nack, Outcome::Nack],
        "the second delivery is at the cap, so it is rejected rather than copied back",
    );
}
