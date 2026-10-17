//! The plain publish policy on `AMQP` 1.0: a reply leaves once the broker has accepted it, and a
//! refusal fails the publish instead of passing as sent.
//!
//! ```text
//! cargo test --example amqp_publishing_publish --features testing
//! cargo run --example amqp_publishing_publish -- run
//! ```

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:publish]
/// The receipt names its own address, so the mount site adds nothing to it.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

#[subscriber(AmqpAddress::queue("orders"), reply)]
async fn issue_receipt(order: &Order) -> Receipt {
    tracing::info!(order.id, "receipt issued");
    Receipt { order_id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            // The reply waits for the broker's disposition; anything but `accepted` fails it.
            b.include(issue_receipt).out_reply(Publish);
        },
    )
}
// --8<-- [end:publish]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Order, Receipt, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_receipt_lands_on_the_address_its_type_names() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<AmqpBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<AmqpBroker>()
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 7 });
        tb.shutdown().await?;
        Ok(())
    }
}
