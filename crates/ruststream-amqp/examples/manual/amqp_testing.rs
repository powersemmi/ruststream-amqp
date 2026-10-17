//! The service and tests of `examples/amqp_testing.rs` without the macros: the handlers are named
//! types, `subscriber` takes the address descriptors, and `main` is written by hand. The tests are
//! the same: a test hands the harness the production app either way.
//!
//! ```text
//! cargo test --example manual_amqp_testing --features testing,asyncapi
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_amqp::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    amount: u64,
}

// `#[outgoing(name = "payments")]` by hand: a payment names the address it is published to.
impl OutgoingDestination for Payment {
    type Form = FixedName;
    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

struct Accept;

impl Handle<Payment> for Accept {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if payment.amount == 0 {
            return ready(Err(HandlerOutcome::drop()));
        }
        ready(Ok(()))
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    order_id: u64,
}

// `#[outgoing(name = "receipts")]` by hand: the reply names the address it goes to.
impl OutgoingDestination for Receipt {
    type Form = FixedName;
    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

struct IssueReceipt;

impl Handle<Order, Receipt> for IssueReceipt {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { order_id: order.id }))
    }
}

/// The app `main` runs in production, and the one a test hands the harness unchanged.
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            let accept = subscriber(AmqpAddress::queue("payments"), Accept);
            b.include(accept.build());
            let receipts = subscriber(AmqpAddress::queue("orders"), IssueReceipt);
            b.include(receipts.reply().build()).out_reply(Publish);
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Order, Payment, Receipt, app};

    // --8<-- [start:testapp]
    #[tokio::test]
    async fn a_payment_is_accepted() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<AmqpBroker>()
            .message(&Payment { amount: 100 })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("payments")
            .assert_called_once()
            .with(&Payment { amount: 100 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
    // --8<-- [end:testapp]

    #[tokio::test]
    async fn a_payment_of_nothing_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<AmqpBroker>()
            .message(&Payment { amount: 0 })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        Ok(())
    }

    // --8<-- [start:reply]
    #[tokio::test]
    async fn an_order_is_answered_with_a_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<AmqpBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 7 });
        Ok(())
    }
    // --8<-- [end:reply]
}
