//! Transactional publishing on `AMQP` 1.0, behind the `transaction` feature: the invoice lines of
//! one order become visible together at the commit, and an abort discards every line posted so
//! far.
//!
//! ```text
//! cargo test --example amqp_transaction --features testing,transaction
//! cargo run --example amqp_transaction --features transaction -- run
//! ```

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Line {
    sku: String,
    quantity: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    lines: Vec<Line>,
}

// --8<-- [start:transaction]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct InvoiceLine {
    order_id: u64,
    sku: String,
    quantity: u32,
}

#[subscriber(AmqpAddress::queue("orders"))]
async fn bill(order: &Order, Out(invoices): Out<impl TransactionalPublisher>) -> HandlerOutcome {
    if invoices.begin_transaction().await.is_err() {
        return HandlerOutcome::retry();
    }
    for line in &order.lines {
        // An empty line voids the order, and the abort takes back the lines posted before it.
        if line.quantity == 0 {
            tracing::warn!(order.id, sku = %line.sku, "empty line, order voided");
            return match invoices.abort().await {
                Ok(()) => HandlerOutcome::drop(),
                Err(_) => HandlerOutcome::retry(),
            };
        }
        let invoice_line = InvoiceLine {
            order_id: order.id,
            sku: line.sku.clone(),
            quantity: line.quantity,
        };
        if invoices.message(&invoice_line).publish().await.is_err() {
            let _ = invoices.abort().await;
            return HandlerOutcome::retry();
        }
    }
    match invoices.commit().await {
        Ok(()) => {
            tracing::info!(order.id, lines = order.lines.len(), "order billed");
            HandlerOutcome::ack()
        }
        Err(_) => HandlerOutcome::retry(),
    }
}
// --8<-- [end:transaction]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            // The transactional policy is what gives the slot begin, commit and abort.
            b.include(bill)
                .out(DefaultSlot, TransactionalPublish)
                .build();
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{InvoiceLine, Line, Order, app};

    fn line(sku: &str, quantity: u32) -> Line {
        Line {
            sku: sku.to_owned(),
            quantity,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_committed_order_posts_every_line() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<AmqpBroker>()
            .message(&Order {
                id: 7,
                lines: vec![line("tea", 2), line("cups", 6)],
            })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        let posted = tb
            .broker::<AmqpBroker>()
            .published::<InvoiceLine>("invoices")
            .decoded();
        assert_eq!(
            posted,
            [
                InvoiceLine {
                    order_id: 7,
                    sku: "tea".to_owned(),
                    quantity: 2,
                },
                InvoiceLine {
                    order_id: 7,
                    sku: "cups".to_owned(),
                    quantity: 6,
                },
            ],
        );
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_aborted_order_posts_nothing() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<AmqpBroker>()
            .message(&Order {
                id: 8,
                lines: vec![line("tea", 2), line("cups", 0)],
            })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<AmqpBroker>()
            .published::<InvoiceLine>("invoices")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
