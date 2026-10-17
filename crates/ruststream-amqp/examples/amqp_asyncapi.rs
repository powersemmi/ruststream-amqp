//! The `AsyncAPI` document of an `AMQP` 1.0 service: the server says it speaks `amqp1` and names
//! the container the service presents, each channel names its node and its link, each send
//! operation says how it posts, and a reply says where its address is read. The tests read the
//! document the app generates.
//!
//! ```text
//! cargo run --example amqp_asyncapi --features asyncapi,transaction -- asyncapi gen
//! cargo test --example amqp_asyncapi --features asyncapi,transaction
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;

// The derive and the value a transform edits share the name in different namespaces: the
// derive is the macro the prelude carries, the value is the type `ruststream::runtime::Outgoing`.
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize)]
struct Payment {
    order_id: u64,
}

// --8<-- [start:replies]
/// The receipt names no address of its own, so the transform may name one per delivery.
#[derive(Debug, Serialize, Outgoing)]
struct Receipt {
    order_id: u64,
}

/// Answers where the request asked to be answered: the `reply-to` property, which a handler reads
/// as the `reply-to` header.
#[derive(Debug, Clone, Copy)]
struct ToReplyTo;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ToReplyTo {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(address) = cx.headers().get_str("reply-to") {
            let address = address.to_owned();
            out.set_name(address);
        }
    }
}
// --8<-- [end:replies]

/// A ledger entry, posted under a transaction.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "ledger")]
struct LedgerEntry {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(LedgerEntry)]
struct Ledger;

// --8<-- [start:channels]
#[subscriber(AmqpAddress::queue("orders").credit(nonzero!(64)), reply("receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { order_id: order.id }
}

#[subscriber(AmqpAddress::topic("events").settle(Settle::AtMostOnce))]
async fn watch(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "event seen");
    HandlerOutcome::ack()
}

#[subscriber(AmqpAddress::raw("/queues/audit"))]
async fn audit(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "audited");
    HandlerOutcome::ack()
}
// --8<-- [end:channels]

#[subscriber(AmqpAddress::queue("payments"))]
async fn book(
    payment: &Payment,
    Out(ledger): Out<impl TransactionalPublisher, Ledger>,
) -> HandlerOutcome {
    let entry = LedgerEntry {
        order_id: payment.order_id,
    };
    if ledger.begin_transaction().await.is_err() {
        return HandlerOutcome::retry();
    }
    if ledger.message(&entry).publish().await.is_err() {
        let _ = ledger.abort().await;
        return HandlerOutcome::retry();
    }
    match ledger.commit().await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}

// --8<-- [start:server]
/// The production broker. The password rides the URL, and the document reports the host alone.
fn broker(password: &str) -> AmqpBroker {
    AmqpBroker::new(format!("amqp://billing:{password}@broker.example.com:5672"))
        .container_id("billing-svc")
}
// --8<-- [end:server]

// --8<-- [start:app]
// --8<-- [start:operations]
fn service(broker: AmqpBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("billing", "1.0.0")).with_broker_labeled("amqp", broker, |b| {
        b.include(confirm).out_reply(Publish).transform(ToReplyTo);
        b.include(watch)
            .max_attempts(nonzero!(3u32))
            .dead_letter("orders.dead");
        b.include(audit);
        b.include(book).out(Ledger, TransactionalPublish).build();
    })
}
// --8<-- [end:operations]

#[ruststream::app]
fn app() -> impl App<State = ()> {
    let password = env::var("AMQP_PASSWORD").unwrap_or_default();
    service(broker(&password))
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::{broker, service};

    const PASSWORD: &str = "hunter2";

    fn document() -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(
            &build_spec(&service(broker(PASSWORD))).to_json()?,
        )?)
    }

    /// The excerpt the AMQP bindings page shows is this service's own document.
    #[test]
    fn the_shown_excerpt_is_this_services_document() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let shown = json!({
            "servers": { "amqp": spec["servers"]["amqp"] },
            "channels": {
                "orders": { "bindings": spec["channels"]["orders"]["bindings"] },
            },
        });

        let excerpt: Value = serde_json::from_str(include_str!("../tests/asyncapi_excerpt.json"))?;
        assert_eq!(shown, excerpt);
        Ok(())
    }

    #[test]
    fn the_server_speaks_amqp_1_0_without_the_password() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"]["amqp"],
            json!({
                "host": "broker.example.com:5672",
                "protocol": "amqp1",
                "protocolVersion": "1.0",
                "bindings": { "x-ruststream-amqp1": { "containerId": "billing-svc" } },
            })
        );
        let text = spec.to_string();
        assert!(!text.contains(PASSWORD), "{text}");
        Ok(())
    }

    #[test]
    fn each_channel_names_its_node_and_its_link() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let channels = &spec["channels"];

        assert_eq!(
            channels["orders"]["bindings"]["x-ruststream-amqp1"],
            json!({
                "address": "orders",
                "capability": "queue",
                "credit": 64,
                "settleMode": "at-least-once",
            })
        );
        assert_eq!(
            channels["events"]["bindings"]["x-ruststream-amqp1"],
            json!({
                "address": "events",
                "capability": "topic",
                "credit": 256,
                "settleMode": "at-most-once",
            })
        );
        // A raw address asks for no terminus capability, so the document claims none.
        assert_eq!(
            channels["/queues/audit"]["bindings"]["x-ruststream-amqp1"],
            json!({
                "address": "/queues/audit",
                "credit": 256,
                "settleMode": "at-least-once",
            })
        );
        // A channel the service publishes to names the node its sender attaches to.
        for destination in ["receipts", "orders.dead", "ledger"] {
            assert_eq!(
                channels[destination]["bindings"]["x-ruststream-amqp1"],
                json!({ "address": destination }),
                "{destination}",
            );
        }
        Ok(())
    }

    #[test]
    fn each_send_says_how_it_posts() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let operations = &spec["operations"];

        assert_eq!(
            operations["send_events_orders_dead"]["bindings"]["x-ruststream-amqp1"],
            json!({ "posting": "confirmed" })
        );
        assert_eq!(
            operations["send_payments_ledger"]["bindings"]["x-ruststream-amqp1"],
            json!({ "posting": "transactional" })
        );
        Ok(())
    }

    #[test]
    fn the_reply_says_where_its_address_is_read() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["operations"]["receive_orders"]["reply"]["address"]["location"],
            "$message.header#/reply-to"
        );
        // Routed per delivery, the reply channel holds no fixed address of its own.
        assert!(spec["channels"]["receipts"]["address"].is_null());
        Ok(())
    }
}
