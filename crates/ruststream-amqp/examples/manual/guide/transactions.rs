//! The billing service of the Transactions guide, written without the macros.
//!
//! ```text
//! docker compose up -d
//! cargo run --example manual_guide_transactions --features testing,transaction -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:bill]
#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    lines: Vec<Line>,
}

impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "billing.orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

impl MessageInfo for Order {
    const NAME: &'static str = "Order";
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Line {
    sku: String,
    cents: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct LedgerEntry {
    order: u64,
    sku: String,
    cents: u64,
}

impl OutgoingDestination for LedgerEntry {
    type Form = FixedName;

    const DESTINATION: &'static str = "ledger";
}

impl MessageHeaders for LedgerEntry {
    type Contract = NoHeaders;
}

impl MessageInfo for LedgerEntry {
    const NAME: &'static str = "LedgerEntry";
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Invoice {
    order: u64,
    total_cents: u64,
}

impl OutgoingDestination for Invoice {
    type Form = FixedName;

    const DESTINATION: &'static str = "invoices";
}

impl MessageHeaders for Invoice {
    type Contract = NoHeaders;
}

impl MessageInfo for Invoice {
    const NAME: &'static str = "Invoice";
}

struct Billing;

impl OutSlot for Billing {
    const NAME: &'static str = "Billing";
    type Destination = Reads;
}

impl PublishedThrough<Billing> for LedgerEntry {}
impl PublishedThrough<Billing> for Invoice {}

struct Bill;

impl<Sent> Handle<Order, (), Outs<(Sent,)>> for Bill
where
    Sent: OutEntry<Billing, Wire: TransactionalPublisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Sent,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let billing = outs.get(Billing);
        if billing.begin_transaction().await.is_err() {
            return Err(HandlerOutcome::retry());
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
                return Err(HandlerOutcome::drop());
            }
            let entry = LedgerEntry {
                order: order.id,
                sku: line.sku.clone(),
                cents: line.cents,
            };
            if billing.message(&entry).publish().await.is_err() {
                let _ = billing.abort().await;
                return Err(HandlerOutcome::retry());
            }
        }
        let invoice = Invoice {
            order: order.id,
            total_cents: order.lines.iter().map(|line| line.cents).sum(),
        };
        if billing.message(&invoice).publish().await.is_err() {
            let _ = billing.abort().await;
            return Err(HandlerOutcome::retry());
        }
        if billing.commit().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        tracing::info!(order.id, invoice.total_cents, "order billed");
        Ok(())
    }
}
// --8<-- [end:bill]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(subscriber(AmqpAddress::queue("billing.orders"), Bill).build())
                .out(Billing, TransactionalPublish)
                .build();
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
