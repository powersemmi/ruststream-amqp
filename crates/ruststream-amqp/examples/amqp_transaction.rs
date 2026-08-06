//! Transactional publishing, behind the `transaction` feature.
//!
//! A batch of invoices becomes visible on the broker atomically: nothing is readable until the
//! commit, and an abort discards the whole batch. The transactional policy is a distinct type, so
//! only a publisher paired from it carries the transactional surface.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example amqp_transaction --features transaction -- run`

use std::io;

use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::{OutgoingMessage, Publisher, TransactionalPublisher, subscriber};
use ruststream_amqp::{AmqpAddress, AmqpBroker, AmqpTransactionalPublish};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Invoice {
    id: u64,
}

#[subscriber(AmqpAddress::queue("invoices"))]
async fn handle(invoice: &Invoice) -> HandlerResult {
    println!("got invoice {}", invoice.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(handle);

            // --8<-- [start:transaction]
            b.after_startup(
                AmqpTransactionalPublish,
                async move |publisher| -> io::Result<()> {
                    publisher
                        .begin_transaction()
                        .await
                        .map_err(io::Error::other)?;

                    for id in 1..=3_u64 {
                        let payload = format!("{{\"id\":{id}}}");
                        let message = OutgoingMessage::new("invoices", payload.as_bytes());
                        if let Err(error) = publisher.publish(message).await {
                            publisher.abort().await.map_err(io::Error::other)?;
                            return Err(io::Error::other(error));
                        }
                    }

                    publisher.commit().await.map_err(io::Error::other)
                },
            );
            // --8<-- [end:transaction]
        },
    )
}
