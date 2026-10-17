//! Testing a service in process: the production app, `AmqpBroker` included, run by `TestApp`
//! with no AMQP broker. A publish through the harness returns once every handler it woke has
//! settled, so the assertions read finished state.
//!
//! ```text
//! cargo run --example amqp_testing --features testing -- run
//! cargo test --example amqp_testing --features testing
//! ```

use serde::{Deserialize, Serialize};

use ruststream_amqp::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    amount: u64,
}

#[subscriber(AmqpAddress::queue("payments"))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

#[subscriber(AmqpAddress::queue("orders"), reply)]
async fn issue_receipt(order: &Order) -> Receipt {
    Receipt { order_id: order.id }
}

/// The app `main` runs in production, and the one a test hands the harness unchanged.
#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(accept);
            b.include(issue_receipt).out_reply(Publish);
        },
    )
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
