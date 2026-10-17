//! The service of the Acknowledgement and delivery guarantees guide: a warehouse that picks
//! shipments, logs every pick at most once, and restocks in batches.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_acknowledgement --features testing -- run
//! ```

use std::time::Duration;

use ruststream_amqp::prelude::*;
use serde::Deserialize;

// --8<-- [start:pick]
#[derive(Debug, PartialEq, Deserialize)]
struct Shipment {
    id: u64,
    sku: String,
    quantity: u32,
}

/// The goods the warehouse has on its shelves.
fn in_stock(sku: &str) -> bool {
    matches!(sku, "book" | "pen")
}

#[subscriber(AmqpAddress::queue("shipments").credit(nonzero!(32)))]
async fn pick(shipment: &Shipment) -> HandlerOutcome {
    if shipment.quantity == 0 {
        tracing::warn!(shipment.id, "an empty shipment rejected");
        return HandlerOutcome::drop();
    }
    if !in_stock(&shipment.sku) {
        tracing::info!(
            shipment.id,
            shipment.sku,
            "out of stock, the shipment comes back"
        );
        return HandlerOutcome::retry();
    }
    tracing::info!(shipment.id, shipment.sku, "shipment picked");
    HandlerOutcome::ack()
}
// --8<-- [end:pick]

// --8<-- [start:audit]
#[derive(Debug, PartialEq, Deserialize)]
struct Picked {
    shipment: u64,
}

#[subscriber(AmqpAddress::queue("pick-log").settle(Settle::AtMostOnce))]
async fn log_pick(picked: &Picked) -> HandlerOutcome {
    tracing::info!(picked.shipment, "pick logged");
    HandlerOutcome::ack()
}
// --8<-- [end:audit]

// --8<-- [start:restock]
#[derive(Debug, PartialEq, Deserialize)]
struct Restock {
    sku: String,
    quantity: u32,
}

#[subscriber(AmqpAddress::queue("restock").batch_wait(Duration::from_millis(50)))]
async fn restock(items: &[Restock]) -> HandlerOutcome {
    let units: u32 = items.iter().map(|item| item.quantity).sum();
    tracing::info!(lines = items.len(), units, "shelves restocked");
    HandlerOutcome::ack()
}
// --8<-- [end:restock]

// --8<-- [start:app]
fn service(broker: AmqpBroker) -> RustStream {
    RustStream::new(AppInfo::new("warehouse", "0.1.0")).with_broker(broker, |b| {
        b.include(pick).max_attempts(nonzero!(5u32));
        b.include(log_pick);
        b.include(restock.batch(nonzero!(20usize)));
    })
}

#[ruststream::app]
fn app() -> impl App {
    service(AmqpBroker::new("amqp://artemis:artemis@localhost:5672"))
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_amqp::prelude::*;
    use serde::Serialize;

    use super::service;

    const URL: &str = "amqp://artemis:artemis@localhost:5672";

    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "shipments")]
    struct Shipment {
        id: u64,
        sku: &'static str,
        quantity: u32,
    }

    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "pick-log")]
    struct Picked {
        shipment: u64,
    }

    #[derive(Serialize, Outgoing)]
    #[outgoing(name = "restock")]
    struct Restock {
        sku: &'static str,
        quantity: u32,
    }

    #[tokio::test]
    async fn a_shipment_in_stock_is_accepted() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(AmqpBroker::new(URL))).await?;

        tb.broker::<AmqpBroker>()
            .message(&Shipment {
                id: 1,
                sku: "book",
                quantity: 2,
            })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("shipments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_shipment_is_rejected() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(AmqpBroker::new(URL))).await?;

        tb.broker::<AmqpBroker>()
            .message(&Shipment {
                id: 2,
                sku: "book",
                quantity: 0,
            })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("shipments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_shipment_out_of_stock_comes_back_until_the_cap() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(AmqpBroker::new(URL))).await?;

        tb.broker::<AmqpBroker>()
            .message(&Shipment {
                id: 3,
                sku: "kite",
                quantity: 1,
            })
            .publish()
            .await?;

        assert_eq!(
            tb.broker::<AmqpBroker>().subscriber("shipments").outcomes(),
            [Outcome::Nack; 5],
        );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_pick_is_logged_at_most_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(AmqpBroker::new(URL))).await?;

        tb.broker::<AmqpBroker>()
            .message(&Picked { shipment: 1 })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("pick-log")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restocking_arrives_in_batches() -> Result<(), Box<dyn Error>> {
        // A publisher taken off the broker before it starts publishes as another service does,
        // so the deliveries queue up and the subscription batches them.
        let broker = AmqpBroker::new(URL);
        let supplier = broker.publisher();
        let tb = TestApp::start(service(broker)).await?;

        for quantity in 1..=3 {
            supplier
                .message(&Restock {
                    sku: "pen",
                    quantity,
                })
                .publish()
                .await?;
        }
        tb.settle().await?;

        let batches = tb
            .broker::<AmqpBroker>()
            .subscriber("restock")
            .batches_raw();
        assert!(batches.iter().all(|batch| (1..=20).contains(&batch.len())));
        assert_eq!(batches.concat().len(), 3);
        tb.broker::<AmqpBroker>()
            .subscriber("restock")
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }
}
