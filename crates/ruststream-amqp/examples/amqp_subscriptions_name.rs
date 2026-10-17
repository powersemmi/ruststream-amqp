//! A subscription by name: `#[subscriber("orders")]` opens a receiver on the address `orders`,
//! sent to the broker as written.
//!
//! ```text
//! cargo test --example amqp_subscriptions_name --features testing
//! cargo run --example amqp_subscriptions_name -- run
//! ```

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:name]
/// Reads the address `orders`; the name reaches the broker as written.
#[subscriber("orders")]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(place);
    })
}
// --8<-- [end:name]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Order, app};

    /// The same handler on an empty name, which no broker can attach a receiver to.
    #[subscriber("")]
    async fn nameless(order: &Order) -> HandlerOutcome {
        let _ = order.id;
        HandlerOutcome::ack()
    }

    #[tokio::test]
    async fn the_address_named_is_the_one_read() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_name_stops_the_service_at_startup() {
        let unaddressed = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
            |b| {
                b.include(nameless);
            },
        );

        let refused = TestApp::start(unaddressed)
            .await
            .expect_err("an empty address cannot form a subscription");
        assert!(refused.to_string().contains("empty"), "{refused}");
    }
}
