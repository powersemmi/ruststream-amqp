//! What this crate puts into a generated `AsyncAPI` document.
//!
//! The specification reserves the `amqp1` binding: all four of its objects must stay empty. So the
//! server coordinate, the node a subscription reads and the way a publisher posts travel in the
//! `x-ruststream-amqp1` extension at the level the binding would have sat at, and these cases pin
//! what a reader of a published document actually gets.

#![cfg(feature = "asyncapi")]

use ruststream::asyncapi::build_spec;
use ruststream::runtime::{Outgoing, PublishContext};
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// The answer declares no destination of its own, so the mount site's name is the fallback and
/// the transform below may name one per delivery.
#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct Receipt {
    order_id: u64,
}

/// Answers where the request asked to be answered, which on this broker is the `reply-to`
/// property the requester set and a handler reads as the `reply-to` header.
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

#[subscriber(AmqpAddress::queue("orders").credit(nonzero!(64)), publish("receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { order_id: order.id }
}

#[subscriber(AmqpAddress::topic("events").settle(Settle::AtMostOnce))]
async fn watch(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(AmqpAddress::raw("/queues/audit"))]
async fn audit(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// The document a deployment of this crate publishes, from an app nothing has connected: every
/// binding is computed from the descriptor and the policy alone.
fn document() -> Value {
    let app = RustStream::new(AppInfo::new("billing", "1.0.0")).with_broker_labeled(
        "amqp",
        AmqpBroker::new("amqp://svc:hunter2@broker.example.com:5672").container_id("billing-svc"),
        |b| {
            b.include(confirm).out_reply(Publish).transform(ToReplyTo);
            b.include(watch)
                .max_attempts(nonzero!(3u32))
                .dead_letter("orders.dead");
            b.include(audit);
        },
    );
    let json = build_spec(&app)
        .to_json()
        .expect("the document must serialize");
    serde_json::from_str(&json).expect("valid JSON")
}

#[test]
fn the_server_names_amqp_1_0_and_the_container_it_presents() {
    let document = document();
    let server = &document["servers"]["amqp"];

    // `amqp` is AMQP 0.9.1's key in the specification, and the two protocols share the scheme and
    // the port, so the key plus the version is what tells a reader which one this is.
    assert_eq!(server["protocol"], "amqp1");
    assert_eq!(server["protocolVersion"], "1.0");
    assert_eq!(server["host"], "broker.example.com:5672");
    assert_eq!(
        server["bindings"]["x-ruststream-amqp1"]["containerId"],
        "billing-svc",
    );
}

#[test]
fn a_queue_subscription_describes_its_node_and_its_link() {
    let document = document();
    let channel = &document["channels"]["orders"]["bindings"]["x-ruststream-amqp1"];

    assert_eq!(channel["address"], "orders");
    assert_eq!(channel["capability"], "queue");
    assert_eq!(channel["credit"], 64);
    assert_eq!(channel["settleMode"], "at-least-once");
}

#[test]
fn a_topic_subscription_describes_the_terminus_and_the_guarantee_it_asked_for() {
    let document = document();
    let channel = &document["channels"]["events"]["bindings"]["x-ruststream-amqp1"];

    assert_eq!(channel["capability"], "topic");
    assert_eq!(channel["credit"], 256);
    assert_eq!(channel["settleMode"], "at-most-once");
}

/// A verbatim address asks for no terminus capability, and the document says so by leaving the
/// field out rather than naming one the deployment never declared.
#[test]
fn a_verbatim_address_claims_no_terminus() {
    let document = document();
    let channel = &document["channels"]["/queues/audit"]["bindings"]["x-ruststream-amqp1"];

    assert_eq!(channel["address"], "/queues/audit");
    assert!(channel["capability"].is_null());
}

/// The publisher a spent delivery leaves through has a send operation of its own, and it says how
/// that send reaches the node.
#[test]
fn the_dead_letter_destination_says_how_a_spent_delivery_is_posted() {
    let document = document();
    let operation = &document["operations"]["send_events_orders_dead"]["bindings"];

    assert_eq!(operation["x-ruststream-amqp1"]["posting"], "confirmed");
}

/// A channel a publish policy reaches names the node the sender attaches its target to. The reply
/// here is routed per delivery, so the channel reports no address of its own and the extension is
/// the only place the declared target is written down; the dead-letter destination is the same
/// policy on a name of its own.
#[test]
fn a_publish_destination_names_the_node_its_sender_attaches_to() {
    let document = document();
    let reply = &document["channels"]["receipts"]["bindings"]["x-ruststream-amqp1"];
    let dead_letter = &document["channels"]["orders.dead"]["bindings"]["x-ruststream-amqp1"];

    assert!(document["channels"]["receipts"]["address"].is_null());
    assert_eq!(reply["address"], "receipts");
    assert_eq!(dead_letter["address"], "orders.dead");
}

/// With a transform naming the reply per delivery, the channel carries no fixed address and the
/// document says where a client reads the one that applies.
#[test]
fn the_reply_names_where_its_address_is_read() {
    let document = document();
    let reply = &document["operations"]["receive_orders"]["reply"];

    assert_eq!(reply["address"]["location"], "$message.header#/reply-to");
}

/// The excerpt the documentation shows, held to what the document actually says: a page cannot
/// promise a shape the crate stopped emitting.
#[test]
fn the_documented_excerpt_is_what_a_deployment_gets() {
    let document = document();
    let shown = serde_json::json!({
        "servers": { "amqp": document["servers"]["amqp"] },
        "channels": {
            "orders": { "bindings": document["channels"]["orders"]["bindings"] },
        },
    });

    let documented: Value = serde_json::from_str(include_str!("asyncapi_excerpt.json"))
        .expect("the documented excerpt is JSON");
    assert_eq!(shown, documented);
}
