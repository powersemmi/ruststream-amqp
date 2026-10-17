//! The service of `examples/amqp_batches.rs` written without the macros: a body over `[T]` is a
//! batch handler, and the batch size is a step of its chain.
//!
//! ```text
//! cargo test --example manual_amqp_batches --features testing,asyncapi
//! cargo run --example manual_amqp_batches -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:handler]
/// Stores a batch of orders in one round-trip; `Ok` settles every order of the batch.
struct Store;

impl Handle<[Order]> for Store {
    fn handle(
        &self,
        orders: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        tracing::info!(count = orders.len(), "orders stored in one round-trip");
        ready(Ok(()))
    }
}

/// `batch_wait` caps how long a partial batch waits for the deliveries that would fill it.
fn orders() -> AmqpAddress {
    AmqpAddress::queue("orders").batch_wait(Duration::from_millis(50))
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        // At most 32 orders per call.
        b.include(subscriber(orders(), Store).batch(nonzero!(32)).build());
    })
}
// --8<-- [end:app]

fn main() -> ExitCode {
    run_main(app)
}

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
