//! A topic subscription: `AmqpAddress::topic` asks for the topic terminus, so every subscription
//! on the address gets its own copy of each message.
//!
//! ```text
//! cargo test --example amqp_subscriptions_topic --features testing
//! cargo run --example amqp_subscriptions_topic -- run
//! ```

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.placed")]
struct OrderPlaced {
    id: u64,
}

// --8<-- [start:topic]
/// Audit keeps a record of every placed order.
#[subscriber(AmqpAddress::topic("orders.placed"))]
async fn audit(event: &OrderPlaced) -> HandlerOutcome {
    tracing::info!(event.id, "order recorded");
    HandlerOutcome::ack()
}

/// Notifications tell the customer about the same orders, from a copy of their own.
#[subscriber(AmqpAddress::topic("orders.placed"))]
async fn notify(event: &OrderPlaced) -> HandlerOutcome {
    tracing::info!(event.id, "customer notified");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(audit);
        b.include(notify);
    })
}
// --8<-- [end:topic]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{OrderPlaced, app};

    #[tokio::test]
    async fn each_subscription_gets_its_own_copy() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&OrderPlaced { id: 1 })
            .publish()
            .await?;

        // One publish, two handler calls: audit's copy and notify's copy.
        let copies = tb
            .broker::<AmqpBroker>()
            .subscriber("orders.placed")
            .assert_called(2)
            .received::<OrderPlaced>();
        assert_eq!(copies, [OrderPlaced { id: 1 }, OrderPlaced { id: 1 }]);
        Ok(())
    }
}
