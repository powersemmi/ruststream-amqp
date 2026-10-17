//! The billing service of the Transactions guide: each order becomes ledger entries and an
//! invoice, published together or not at all.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_transactions --features testing,transaction -- run
//! ```

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:bill]
#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "billing.orders")]
struct Order {
    id: u64,
    lines: Vec<Line>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Line {
    sku: String,
    cents: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "ledger")]
struct LedgerEntry {
    order: u64,
    sku: String,
    cents: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    order: u64,
    total_cents: u64,
}

#[derive(OutSlot)]
#[publishes(LedgerEntry, Invoice)]
struct Billing;

#[subscriber(AmqpAddress::queue("billing.orders"))]
async fn bill(
    order: &Order,
    Out(billing): Out<impl TransactionalPublisher, Billing>,
) -> HandlerOutcome {
    if billing.begin_transaction().await.is_err() {
        return HandlerOutcome::retry();
    }
    for line in &order.lines {
        if line.cents == 0 {
            tracing::warn!(
                order.id,
                line.sku,
                "a line without a price, nothing is billed"
            );
            // A failed abort has still ended the transaction, so there is nothing left to undo.
            let _ = billing.abort().await;
            return HandlerOutcome::drop();
        }
        let entry = LedgerEntry {
            order: order.id,
            sku: line.sku.clone(),
            cents: line.cents,
        };
        if billing.message(&entry).publish().await.is_err() {
            let _ = billing.abort().await;
            return HandlerOutcome::retry();
        }
    }
    let invoice = Invoice {
        order: order.id,
        total_cents: order.lines.iter().map(|line| line.cents).sum(),
    };
    if billing.message(&invoice).publish().await.is_err() {
        let _ = billing.abort().await;
        return HandlerOutcome::retry();
    }
    if billing.commit().await.is_err() {
        return HandlerOutcome::retry();
    }
    tracing::info!(order.id, invoice.total_cents, "order billed");
    HandlerOutcome::ack()
}
// --8<-- [end:bill]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(bill).out(Billing, TransactionalPublish).build();
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Invoice, LedgerEntry, Line, Order, app};

    fn line(sku: &str, cents: u64) -> Line {
        Line {
            sku: sku.to_owned(),
            cents,
        }
    }

    #[tokio::test]
    async fn an_order_is_billed_in_one_transaction() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&Order {
                id: 1,
                lines: vec![line("book", 1250), line("pen", 150)],
            })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("billing.orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<AmqpBroker>()
            .published::<LedgerEntry>("ledger")
            .assert_called(2)
            .with(&LedgerEntry {
                order: 1,
                sku: "pen".to_owned(),
                cents: 150,
            });
        tb.broker::<AmqpBroker>()
            .published::<Invoice>("invoices")
            .assert_called_once()
            .with(&Invoice {
                order: 1,
                total_cents: 1400,
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn an_aborted_order_publishes_nothing() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&Order {
                id: 2,
                lines: vec![line("book", 1250), line("gift", 0)],
            })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("billing.orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<AmqpBroker>()
            .published::<LedgerEntry>("ledger")
            .assert_not_called();
        tb.broker::<AmqpBroker>()
            .published::<Invoice>("invoices")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
