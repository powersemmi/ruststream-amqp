//! Native request/reply: a dynamic reply link, `reply-to`, and `correlation-id`.
//!
//! Run a broker (`just brokers-up`) and a responder on the `greeter` address, then:
//! `cargo run --example amqp_request_reply`

use std::time::Duration;

use ruststream::{Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, RequestReply};
use ruststream_amqp::AmqpBroker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let connected = AmqpBroker::new("amqp://artemis:artemis@localhost:5672")
        .container_id("request-reply-example")
        .connect()
        .await?;

    let publisher = connected.publisher();
    let reply = publisher
        .request(
            OutgoingMessage::new("greeter", b"hello".as_slice()),
            Duration::from_secs(5),
        )
        .await?;
    println!("reply: {}", String::from_utf8_lossy(reply.payload()));

    connected.shutdown().await?;
    Ok(())
}
