//! The service of `examples/amqp_asyncapi.rs` without the macros: the handlers are named types,
//! `subscriber` takes the descriptors, and `main` is written by hand. The tests read the same
//! document.
//!
//! ```text
//! cargo run --example manual_amqp_asyncapi --features asyncapi,transaction -- asyncapi gen
//! cargo test --example manual_amqp_asyncapi --features asyncapi,transaction
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
// The derive and the value a transform edits share the name in different namespaces: the
// derive is the macro the prelude carries, the value is the type `ruststream::runtime::Outgoing`.
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream::schemars::JsonSchema;
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    order_id: u64,
}

// --8<-- [start:replies]
/// The receipt names no address of its own, so the transform may name one per delivery.
#[derive(Debug, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
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
#[derive(Debug, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "ledger")]
struct LedgerEntry {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(LedgerEntry)]
struct Ledger;

struct Confirm;

impl Handle<Order, Receipt> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { order_id: order.id }))
    }
}

struct Watch;

impl Handle<Order> for Watch {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "event seen");
        ready(Ok(()))
    }
}

struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "audited");
        ready(Ok(()))
    }
}

struct Book;

impl<Entry> Handle<Payment, (), Outs<(Entry,)>> for Book
where
    Entry: OutEntry<Ledger, Wire: TransactionalPublisher>,
{
    async fn handle(
        &self,
        payment: &Payment,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let ledger = outs.get(Ledger);
        let entry = LedgerEntry {
            order_id: payment.order_id,
        };
        ledger
            .begin_transaction()
            .await
            .map_err(|_| HandlerOutcome::retry())?;
        if ledger.message(&entry).publish().await.is_err() {
            let _ = ledger.abort().await;
            return Err(HandlerOutcome::retry());
        }
        ledger.commit().await.map_err(|_| HandlerOutcome::retry())
    }
}

// --8<-- [start:channels]
/// The queue `orders`, with a credit of 64.
fn orders() -> AmqpAddress {
    AmqpAddress::queue("orders").credit(nonzero!(64))
}

/// The topic `events`, settled on receipt.
fn events() -> AmqpAddress {
    AmqpAddress::topic("events").settle(Settle::AtMostOnce)
}

/// The address `/queues/audit`, sent as written.
fn audit_queue() -> AmqpAddress {
    AmqpAddress::raw("/queues/audit")
}
// --8<-- [end:channels]

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
        let confirm = subscriber(orders(), Confirm);
        b.include(confirm.reply().to("receipts").build())
            .out_reply(Publish)
            .transform(ToReplyTo);
        b.include(subscriber(events(), Watch).build())
            .max_attempts(nonzero!(3u32))
            .dead_letter("orders.dead");
        b.include(subscriber(audit_queue(), Audit).build());
        let book = subscriber(AmqpAddress::queue("payments"), Book);
        b.include(book.build())
            .out(Ledger, TransactionalPublish)
            .build();
    })
}
// --8<-- [end:operations]

fn app() -> impl App<State = ()> {
    let password = env::var("AMQP_PASSWORD").unwrap_or_default();
    service(broker(&password))
}
// --8<-- [end:app]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::hash::{BuildHasher, RandomState};

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::{broker, service};

    /// A password no document could hold by coincidence, drawn afresh for every run.
    fn password() -> String {
        format!("{:016x}", RandomState::new().hash_one("password"))
    }

    fn document_with(password: &str) -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(
            &build_spec(&service(broker(password))).to_json()?,
        )?)
    }

    fn document() -> Result<Value, Box<dyn Error>> {
        document_with(&password())
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

        let excerpt: Value =
            serde_json::from_str(include_str!("../../tests/asyncapi_excerpt.json"))?;
        assert_eq!(shown, excerpt);
        Ok(())
    }

    #[test]
    fn the_server_speaks_amqp_1_0_without_the_password() -> Result<(), Box<dyn Error>> {
        let password = password();
        let spec = document_with(&password)?;

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
        assert!(!text.contains(&password), "{text}");
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
