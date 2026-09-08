//! A batch handler: one call per batch of orders instead of one per order.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example amqp_batches -- run`

use std::time::Duration;

use ruststream_amqp::prelude::*;
use serde::Deserialize;

// --8<-- [start:handler]
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// A batch body takes a slice, and `batch_wait` caps how long a partial batch waits for the
/// deliveries that would fill it.
#[subscriber(AmqpAddress::queue("orders").batch_wait(Duration::from_millis(50)))]
async fn settle(orders: &[Order]) -> HandlerOutcome {
    println!("settling {} orders", orders.len());
    for order in orders {
        println!("  order {}", order.id);
    }
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| b.include(settle.batch(nonzero!(32))),
    )
}
// --8<-- [end:app]
