//! The service of `examples/amqp_subscriptions_credit.rs` written without the macros: the credit
//! rides the descriptor that joins the chain through `map_source`.
//!
//! ```text
//! cargo test --example manual_amqp_subscriptions_credit --features testing,asyncapi
//! cargo run --example manual_amqp_subscriptions_credit -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Invoice {
    id: u64,
}

// `#[outgoing(name = "invoices")]` by hand: the type fixes its destination.
impl OutgoingDestination for Invoice {
    type Form = FixedName;

    const DESTINATION: &'static str = "invoices";
}

impl MessageHeaders for Invoice {
    type Contract = NoHeaders;
}

// --8<-- [start:credit]
/// Rendering an invoice is slow, so the broker sends 16 ahead instead of the default 256 and
/// the rest stay on the queue for the other replicas.
struct Render;

impl Handle<Invoice> for Render {
    fn handle(
        &self,
        invoice: &Invoice,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(invoice.id, "invoice rendered");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber("invoices", Render)
                .build()
                .map_source(|_| AmqpAddress::queue("invoices").credit(nonzero!(16))),
        );
    })
}
// --8<-- [end:credit]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Invoice, app};

    #[tokio::test]
    async fn the_credit_leaves_the_handler_as_it_is() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for id in 1..=3 {
            tb.broker::<AmqpBroker>()
                .message(&Invoice { id })
                .publish()
                .await?;
        }

        let ids: Vec<u64> = tb
            .broker::<AmqpBroker>()
            .subscriber("invoices")
            .assert_called(3)
            .received::<Invoice>()
            .into_iter()
            .map(|invoice| invoice.id)
            .collect();
        assert_eq!(ids, [1, 2, 3]);
        Ok(())
    }
}
