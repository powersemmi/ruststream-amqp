//! Transactional publishing, behind the `transaction` feature.
//!
//! A batch of invoices becomes visible on the broker atomically: nothing is readable until the
//! commit, and an abort discards the whole batch. Only a publisher paired from the transactional
//! policy carries the transactional surface.
//!
//! Run a broker first (`just brokers-up`), then:
//! `cargo run --example amqp_transaction --features transaction -- run`

use std::io;

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
}

#[subscriber(AmqpAddress::queue("invoices"))]
async fn handle(invoice: &Invoice) -> HandlerOutcome {
    println!("got invoice {}", invoice.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(handle);

            // --8<-- [start:transaction]
            b.after_startup(
                TransactionalPublish,
                async move |publisher| -> io::Result<()> {
                    publisher
                        .begin_transaction()
                        .await
                        .map_err(io::Error::other)?;

                    for id in 1..=3_u64 {
                        // The destination rides the type's `#[outgoing(name = ..)]`, so the
                        // publish names only the value; the transaction is the publisher's.
                        if let Err(error) = publisher.message(&Invoice { id }).publish().await {
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
