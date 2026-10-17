//! The service of the Queues and topics guide: orders arrive on a work queue, and each taken order
//! is broadcast on a topic that billing and the audit log both follow.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_queues_and_topics --features testing -- run
//! ```

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:queue]
#[derive(Debug, PartialEq, Deserialize)]
struct Order {
    id: u64,
    amount_cents: u64,
}

/// What every service that follows orders hears about a taken order.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "order-events")]
struct OrderTaken {
    order: u64,
    amount_cents: u64,
}

#[subscriber(AmqpAddress::queue("orders"), reply)]
async fn take_order(order: &Order) -> OrderTaken {
    tracing::info!(order.id, "order taken");
    OrderTaken {
        order: order.id,
        amount_cents: order.amount_cents,
    }
}
// --8<-- [end:queue]

// --8<-- [start:topic]
#[subscriber(AmqpAddress::topic("order-events"))]
async fn bill(event: &OrderTaken) -> HandlerOutcome {
    tracing::info!(event.order, event.amount_cents, "invoice issued");
    HandlerOutcome::ack()
}

#[subscriber(AmqpAddress::topic("order-events"))]
async fn audit(event: &OrderTaken) -> HandlerOutcome {
    tracing::info!(event.order, "order recorded");
    HandlerOutcome::ack()
}
// --8<-- [end:topic]

// --8<-- [start:durable]
/// Reads the queue `ledger` that the operator created on the `order-events` address.
#[subscriber(AmqpAddress::raw("order-events::ledger"))]
async fn post_to_ledger(event: &OrderTaken) -> HandlerOutcome {
    tracing::info!(event.order, "order posted to the ledger");
    HandlerOutcome::ack()
}
// --8<-- [end:durable]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(take_order).out_reply(Publish);
            b.include(bill);
            b.include(audit);
            b.include(post_to_ledger);
        },
    )
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
    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "orders")]
    struct Placed {
        id: u64,
        amount_cents: u64,
    }

    /// An event as the operator replays it straight onto the ledger queue.
    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "order-events::ledger")]
    struct Replayed {
        order: u64,
        amount_cents: u64,
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
