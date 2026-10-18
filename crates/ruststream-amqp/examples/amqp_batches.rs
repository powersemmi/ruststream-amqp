//! A batch handler: one call per batch of orders instead of one per order. AMQP 1.0 transfers one
//! message at a time, so the batches are assembled on the client.
//!
//! ```text
//! cargo test --example amqp_batches --features testing
//! cargo run --example amqp_batches -- run
//! ```

use std::time::Duration;

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:handler]
/// Stores a batch of orders in one round-trip; one outcome settles every order of the batch.
/// `batch_wait` caps how long a partial batch waits for the deliveries that would fill it.
#[subscriber(AmqpAddress::queue("orders").batch_wait(Duration::from_millis(50)))]
async fn store(orders: &[Order]) -> HandlerOutcome {
    tracing::info!(count = orders.len(), "orders stored in one round-trip");
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        // At most 32 orders per call.
        b.include(store.batch(nonzero!(32)));
    })
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Order, app};

    #[tokio::test(start_paused = true)]
    async fn three_waiting_orders_arrive_in_one_call() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // Published at once, the three orders wait on the subscription together.
        let orders: Vec<Order> = (1..=3).map(|id| Order { id }).collect();
        let broker = tb.broker::<AmqpBroker>();
        for published in join_all(orders.iter().map(|order| broker.message(order).publish())).await
        {
            published?;
        }
        tb.settle().await?;

        tb.broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called_once()
            .assert_batch_sizes(&[3])
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
