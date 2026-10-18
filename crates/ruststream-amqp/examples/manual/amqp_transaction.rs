//! The service of `examples/amqp_transaction.rs` written without `#[subscriber]`: the body
//! declares its transactional slot in the injections arena and posts the invoice lines there.
//!
//! ```text
//! cargo test --example manual_amqp_transaction --features testing,transaction
//! cargo run --example manual_amqp_transaction --features transaction -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Line {
    sku: String,
    quantity: u32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    lines: Vec<Line>,
}

// --8<-- [start:transaction]
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "invoices")]
struct InvoiceLine {
    order_id: u64,
    sku: String,
    quantity: u32,
}

struct Bill;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Bill
where
    Entry: OutEntry<DefaultSlot, Wire: TransactionalPublisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let invoices = outs.get(DefaultSlot);
        invoices
            .begin_transaction()
            .await
            .map_err(|_| HandlerOutcome::retry())?;
        for line in &order.lines {
            // An empty line voids the order, and the abort takes back the lines posted before it.
            if line.quantity == 0 {
                tracing::warn!(order.id, sku = %line.sku, "empty line, order voided");
                invoices
                    .abort()
                    .await
                    .map_err(|_| HandlerOutcome::retry())?;
                return Err(HandlerOutcome::drop());
            }
            let invoice_line = InvoiceLine {
                order_id: order.id,
                sku: line.sku.clone(),
                quantity: line.quantity,
            };
            if invoices.message(&invoice_line).publish().await.is_err() {
                let _ = invoices.abort().await;
                return Err(HandlerOutcome::retry());
            }
        }
        invoices
            .commit()
            .await
            .map_err(|_| HandlerOutcome::retry())?;
        tracing::info!(order.id, lines = order.lines.len(), "order billed");
        Ok(())
    }
}
// --8<-- [end:transaction]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            let bill = subscriber(AmqpAddress::queue("orders"), Bill);
            // The transactional policy is what gives the slot begin, commit and abort.
            b.include(bill.build())
                .out(DefaultSlot, TransactionalPublish)
                .build();
        },
    )
}
// --8<-- [end:app]

fn main() -> ExitCode {
    run_main(app)
}

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
