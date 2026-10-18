//! A queue subscription: `AmqpAddress::queue` asks for the queue terminus, so the subscriptions on
//! one address compete and each message reaches one of them.
//!
//! ```text
//! cargo test --example amqp_subscriptions_queue --features testing
//! cargo run --example amqp_subscriptions_queue -- run
//! ```

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:queue]
/// Every replica of the service competes for `orders`: each order is fulfilled once.
#[subscriber(AmqpAddress::queue("orders"))]
async fn fulfil(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order fulfilled");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("fulfillment", "0.1.0")).with_broker(broker, |b| {
        b.include(fulfil);
    })
}
// --8<-- [end:queue]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Order, app, fulfil};

    #[tokio::test]
    async fn an_order_is_fulfilled_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 1 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    /// Two subscriptions on one queue address, as two replicas of the service open them: the
    /// orders are shared out, none is handled twice and none is lost.
    #[tokio::test]
    async fn two_replicas_share_the_orders() -> Result<(), Box<dyn Error>> {
        let replicas = RustStream::new(AppInfo::new("fulfillment", "0.1.0")).with_broker(
            AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
            |b| {
                b.include(fulfil);
                b.include(fulfil);
            },
        );
        let tb = TestApp::start(replicas).await?;

        for id in 1..=4 {
            tb.broker::<AmqpBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }

        let mut ids: Vec<u64> = tb
            .broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called(4)
            .received::<Order>()
            .into_iter()
            .map(|order| order.id)
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, [1, 2, 3, 4]);
        Ok(())
    }
}
