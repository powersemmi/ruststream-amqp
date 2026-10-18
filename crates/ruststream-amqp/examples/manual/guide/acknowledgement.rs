//! The service of the Acknowledgement and delivery guarantees guide, written without the macros.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_acknowledgement --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_amqp::prelude::*;
use serde::Deserialize;

// --8<-- [start:pick]
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipment {
    id: u64,
    sku: String,
    quantity: u32,
}

/// The goods the warehouse has on its shelves.
fn in_stock(sku: &str) -> bool {
    matches!(sku, "book" | "pen")
}

struct Pick;

impl Handle<Shipment> for Pick {
    fn handle(
        &self,
        shipment: &Shipment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if shipment.quantity == 0 {
            tracing::warn!(shipment.id, "an empty shipment rejected");
            return ready(Err(HandlerOutcome::drop()));
        }
        if !in_stock(&shipment.sku) {
            tracing::info!(
                shipment.id,
                shipment.sku,
                "out of stock, the shipment comes back"
            );
            return ready(Err(HandlerOutcome::retry()));
        }
        tracing::info!(shipment.id, shipment.sku, "shipment picked");
        ready(Ok(()))
    }
}
// --8<-- [end:pick]

// --8<-- [start:audit]
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Picked {
    shipment: u64,
}

struct LogPick;

impl Handle<Picked> for LogPick {
    fn handle(
        &self,
        picked: &Picked,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(picked.shipment, "pick logged");
        ready(Ok(()))
    }
}
// --8<-- [end:audit]

// --8<-- [start:restock]
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Restock {
    sku: String,
    quantity: u32,
}

struct Restocking;

impl Handle<[Restock]> for Restocking {
    fn handle(
        &self,
        items: &[Restock],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let units: u32 = items.iter().map(|item| item.quantity).sum();
        tracing::info!(lines = items.len(), units, "shelves restocked");
        ready(Ok(()))
    }
}
// --8<-- [end:restock]

// --8<-- [start:app]
fn service(broker: AmqpBroker) -> RustStream {
    RustStream::new(AppInfo::new("warehouse", "0.1.0")).with_broker(broker, |b| {
        let shipments = AmqpAddress::queue("shipments").credit(nonzero!(32));
        b.include(subscriber(shipments, Pick).build())
            .max_attempts(nonzero!(5u32));
        let pick_log = AmqpAddress::queue("pick-log").settle(Settle::AtMostOnce);
        b.include(subscriber(pick_log, LogPick).build());
        let restock = AmqpAddress::queue("restock").batch_wait(Duration::from_millis(50));
        b.include(
            subscriber(restock, Restocking)
                .batch(nonzero!(20usize))
                .build(),
        );
    })
}

fn app() -> impl App {
    service(AmqpBroker::new("amqp://artemis:artemis@localhost:5672"))
}

fn main() -> ExitCode {
    run_main(app)
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

    #[derive(Serialize)]
    struct Shipment {
        id: u64,
        sku: &'static str,
        quantity: u32,
    }

    impl OutgoingDestination for Shipment {
        type Form = FixedName;

        const DESTINATION: &'static str = "shipments";
    }

    impl MessageHeaders for Shipment {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Shipment {
        const NAME: &'static str = "Shipment";
    }

    #[derive(Serialize)]
    struct Picked {
        shipment: u64,
    }

    impl OutgoingDestination for Picked {
        type Form = FixedName;

        const DESTINATION: &'static str = "pick-log";
    }

    impl MessageHeaders for Picked {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Picked {
        const NAME: &'static str = "Picked";
    }

    #[derive(Serialize)]
    struct Restock {
        sku: &'static str,
        quantity: u32,
    }

    impl OutgoingDestination for Restock {
        type Form = FixedName;

        const DESTINATION: &'static str = "restock";
    }

    impl MessageHeaders for Restock {
        type Contract = NoHeaders;
    }

    impl MessageInfo for Restock {
        const NAME: &'static str = "Restock";
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
