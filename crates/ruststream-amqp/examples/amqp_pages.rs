//! A page handler: one call per page of orders instead of one per order.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example amqp_pages -- run`

use std::time::Duration;

use ruststream_amqp::prelude::*;
use serde::Deserialize;

// --8<-- [start:handler]
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// A page body takes a slice, and `page_wait` caps how long a partial page waits for the
/// deliveries that would fill it.
#[subscriber(AmqpAddress::queue("orders").page_wait(Duration::from_millis(50)))]
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
