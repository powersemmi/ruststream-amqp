//! A minimal AMQP service: consume orders from a queue address.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example amqp_service`

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_amqp::{AmqpAddress, AmqpBroker};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(AmqpAddress::queue("orders"))]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| b.include(handle),
    )
}
