//! A verbatim address: `AmqpAddress::raw` sends the address as written, with no terminus
//! capability, for a deployment that has its own addressing convention.
//!
//! ```text
//! cargo test --example amqp_subscriptions_raw --features testing
//! cargo run --example amqp_subscriptions_raw -- run
//! ```

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

/// The type names no destination, so a publish names the address.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:raw]
/// `RabbitMQ` 4.x addresses a queue by its v2 path; the string reaches the broker as written.
#[subscriber(AmqpAddress::raw("/queues/orders"))]
async fn fulfil(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order fulfilled");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://guest:guest@localhost:5672");
    RustStream::new(AppInfo::new("fulfilment", "0.1.0")).with_broker(broker, |b| {
        b.include(fulfil);
    })
}
// --8<-- [end:raw]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn the_address_is_read_as_written() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&Order { id: 1 })
            .to("/queues/orders")
            .publish()
            .await?;
        // A neighbouring spelling is another node: nothing about the address is a pattern.
        tb.broker::<AmqpBroker>()
            .message(&Order { id: 2 })
            .to("orders")
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("/queues/orders")
            .assert_called_once()
            .with(&Order { id: 1 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
