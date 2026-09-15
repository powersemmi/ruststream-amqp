//! Capping the retries of a subscription and saying where a spent delivery goes.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example amqp_retry -- run`

use std::time::Duration;

use ruststream::runtime::{Outgoing, PublishContext};
use ruststream_amqp::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:handler]
#[subscriber(AmqpAddress::queue("orders"))]
async fn settle_order(order: &Order) -> HandlerOutcome {
    if order.id == 0 {
        // Not ready yet: ask for the delivery again in half a minute.
        return HandlerOutcome::retry_after(Duration::from_secs(30));
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

#[subscriber(AmqpAddress::queue("audit"))]
async fn audit_order(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// --8<-- [start:transform]
/// Marks every copy with the address the delivery came from, so a redelivery is recognisable
/// downstream. A transform on the retry position reads the delivery being retried.
struct Retried;

impl<C, Options> PublishTransform<ForReply<C>, Options> for Retried {
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
// --8<-- [end:transform]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            // --8<-- [start:declaration]
            // Five deliveries, counting the first; after that the order goes to the dead-letter
            // address instead of coming back.
            b.include(settle_order)
                .max_attempts(nonzero!(5u32))
                .dead_letter("orders.dead");
            // --8<-- [end:declaration]

            // --8<-- [start:customised]
            // Every registration already has a publisher for its copies. Naming one replaces it,
            // and the steps after it are the slot steps.
            b.include(audit_order)
                .max_attempts(nonzero!(5u32))
                .dead_letter("orders.dead")
                .out_retry(Publish)
                .transform(Retried);
            // --8<-- [end:customised]
        },
    )
}
