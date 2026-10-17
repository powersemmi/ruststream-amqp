//! The service of the Queues and topics guide, written without the macros.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_queues_and_topics --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:queue]
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    amount_cents: u64,
}

/// What every service that follows orders hears about a taken order.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct OrderTaken {
    order: u64,
    amount_cents: u64,
}

impl OutgoingDestination for OrderTaken {
    type Form = FixedName;

    const DESTINATION: &'static str = "order-events";
}

impl MessageHeaders for OrderTaken {
    type Contract = NoHeaders;
}

impl MessageInfo for OrderTaken {
    const NAME: &'static str = "OrderTaken";
}

struct TakeOrder;

impl Handle<Order, OrderTaken> for TakeOrder {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<OrderTaken, HandlerOutcome>> {
        tracing::info!(order.id, "order taken");
        ready(Ok(OrderTaken {
            order: order.id,
            amount_cents: order.amount_cents,
        }))
    }
}
// --8<-- [end:queue]

// --8<-- [start:topic]
struct Bill;

impl Handle<OrderTaken> for Bill {
    fn handle(
        &self,
        event: &OrderTaken,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(event.order, event.amount_cents, "invoice issued");
        ready(Ok(()))
    }
}

struct Audit;

impl Handle<OrderTaken> for Audit {
    fn handle(
        &self,
        event: &OrderTaken,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(event.order, "order recorded");
        ready(Ok(()))
    }
}
// --8<-- [end:topic]

// --8<-- [start:durable]
/// Reads the queue `ledger` that the operator created on the `order-events` address.
struct PostToLedger;

impl Handle<OrderTaken> for PostToLedger {
    fn handle(
        &self,
        event: &OrderTaken,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(event.order, "order posted to the ledger");
        ready(Ok(()))
    }
}
// --8<-- [end:durable]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(
                subscriber(AmqpAddress::queue("orders"), TakeOrder)
                    .reply()
                    .build(),
            )
            .out_reply(Publish);
            b.include(subscriber(AmqpAddress::topic("order-events"), Bill).build());
            b.include(subscriber(AmqpAddress::topic("order-events"), Audit).build());
            let ledger = AmqpAddress::raw("order-events::ledger");
            b.include(subscriber(ledger, PostToLedger).build());
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;
    use serde::Serialize;

    use super::{OrderTaken, app};

    /// An order as the shop that places it publishes it.
    #[derive(Serialize)]
    struct Placed {
        id: u64,
        amount_cents: u64,
    }

    impl OutgoingDestination for Placed {
        type Form = FixedName;

        const DESTINATION: &'static str = "orders";
    }

    impl MessageHeaders for Placed {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Placed {
        const NAME: &'static str = "Placed";
    }

    /// An event as the operator replays it straight onto the ledger queue.
    #[derive(Serialize)]
    struct Replayed {
        order: u64,
        amount_cents: u64,
    }

    impl OutgoingDestination for Replayed {
        type Form = FixedName;

        const DESTINATION: &'static str = "order-events::ledger";
    }

    impl MessageHeaders for Replayed {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Replayed {
        const NAME: &'static str = "Replayed";
    }

    #[tokio::test]
    async fn one_order_is_taken_once_and_heard_by_every_follower() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&Placed {
                id: 1,
                amount_cents: 1250,
            })
            .publish()
            .await?;

        // The queue hands the order to one consumer.
        tb.broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        // The topic hands a copy of the event to each of its two subscriptions.
        let event = OrderTaken {
            order: 1,
            amount_cents: 1250,
        };
        tb.broker::<AmqpBroker>()
            .published::<OrderTaken>("order-events")
            .assert_called_once()
            .with(&event);
        tb.broker::<AmqpBroker>()
            .subscriber("order-events")
            .assert_called(2)
            .with(&event)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_ledger_reads_its_queue_by_its_full_name() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let event = OrderTaken {
            order: 2,
            amount_cents: 150,
        };

        tb.broker::<AmqpBroker>()
            .message(&Replayed {
                order: 2,
                amount_cents: 150,
            })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("order-events::ledger")
            .assert_called_once()
            .with(&event)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
