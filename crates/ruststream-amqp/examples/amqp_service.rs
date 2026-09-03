//! A minimal AMQP service: consume orders from a queue address.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example amqp_service -- run`

use ruststream_amqp::prelude::*;
use serde::Deserialize;

// --8<-- [start:handler]
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(AmqpAddress::queue("orders"))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}
// --8<-- [end:handler]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| b.include(handle),
    )
}
// --8<-- [end:app]
