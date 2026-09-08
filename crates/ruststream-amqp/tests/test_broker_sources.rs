//! The declaration a service runs in production, mounted on the in-process broker.
//!
//! [`AmqpAddress`] is the crate's only subscription source, so these cases pin it against
//! `AmqpTestBroker`: every address kind opens a subscription under `TestApp` with the handler
//! untouched, and the options the stand-in reproduces keep the meaning they carry against a
//! server. What it drops instead (credit, the queue/topic terminus capability) is covered by the
//! live suite in `integration_amqp.rs`.

#![cfg(feature = "testing")]

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::testing::TestApp;
use ruststream::{AckError, Subscriber};
use ruststream_amqp::prelude::*;
use ruststream_amqp::testing::AmqpTestBroker;
use ruststream_amqp::{AmqpError, Settle};
use serde::{Deserialize, Serialize};

const WAIT: Duration = Duration::from_secs(1);

/// The derive names no destination, so the publishes below keep saying `to(..)`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// The three addresses a handler is declared on, paired with the id published to each.
const MOUNTS: [(&str, u64); 3] = [("orders", 1), ("events", 2), ("/queues/audit", 3)];

// `credit` is a protocol setting the stand-in has no analogue for; naming it must still leave the
// handler mountable, since a service does not rewrite its declaration to be tested.
#[subscriber(AmqpAddress::queue("orders").credit(64))]
async fn from_a_queue(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(AmqpAddress::topic("events"))]
async fn from_a_topic(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(AmqpAddress::raw("/queues/audit"))]
async fn from_a_raw_address(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_address_kind_mounts_on_the_test_broker() {
    let app =
        RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(AmqpTestBroker::new(), |b| {
            b.include(from_a_queue);
            b.include(from_a_topic);
            b.include(from_a_raw_address);
        });
    let app = TestApp::start(app).await.expect("startup failed");

    for (address, id) in MOUNTS {
        app.broker::<AmqpTestBroker>()
            .publish(address, &Order { id })
            .await
            .expect("publish failed");
    }

    for (address, id) in MOUNTS {
        app.broker::<AmqpTestBroker>()
            .subscriber(address)
            .assert_called_once()
            .with(&Order { id })
            .settled(HandlerOutcome::ack());
    }

    app.shutdown().await.expect("shutdown failed");
}

#[subscriber(AmqpAddress::queue("bulk").batch_wait(Duration::from_millis(50)))]
async fn in_batches(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

// The descriptor carries the batch deadline, and both brokers assemble batches with the same
// client-side buffer, so a batch mount takes the descriptor as readily as a bare name does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_mount_takes_the_descriptor_too() {
    // A producer handle publishes as an external client does, without driving the reaction to a
    // standstill on every message - which would close each batch after a single delivery.
    let broker = AmqpTestBroker::new();
    let producer = broker.publisher();

    let app = RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(in_batches.batch(nonzero!(2usize)));
    });
    let app = TestApp::start(app).await.expect("startup failed");

    for id in 0..4 {
        producer
            .message(&Order { id })
            .to("bulk")
            .publish()
            .await
            .expect("publish failed");
    }
    app.settle().await.expect("the run settles");

    let batches: Vec<Vec<Order>> = app.broker::<AmqpTestBroker>().subscriber("bulk").batches();
    assert_eq!(
        batches.concat(),
        (0..4).map(|id| Order { id }).collect::<Vec<_>>(),
        "every order must reach the batch handler, in order",
    );

    app.shutdown().await.expect("shutdown failed");
}

// The settle mode is the option the stand-in reproduces rather than drops: an at-most-once
// delivery arrives settled, so its settlement reports `Unsupported` here exactly as it does
// against a server (`integration_amqp.rs::at_most_once_reports_ack_unsupported`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_at_most_once_descriptor_settles_on_receipt() {
    let broker = AmqpTestBroker::new()
        .connect()
        .await
        .expect("connect failed");
    let mut subscriber = broker
        .subscribe_address(AmqpAddress::queue("fire").settle(Settle::AtMostOnce))
        .await
        .expect("subscription opens");
    broker
        .publisher()
        .message(&Order { id: 1 })
        .to("fire")
        .publish()
        .await
        .expect("publish failed");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    assert!(matches!(message.ack().await, Err(AckError::Unsupported)));
}

// The descriptor is validated on both brokers, so a mount that cannot form a subscription fails
// in a test as early as it does in production.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_invalid_descriptor_is_rejected_before_the_subscription_opens() {
    let broker = AmqpTestBroker::new()
        .connect()
        .await
        .expect("connect failed");
    let err = broker
        .subscribe_address(AmqpAddress::queue(""))
        .await
        .expect_err("an empty address cannot form a subscription");
    assert!(matches!(err, AmqpError::InvalidAddress(_)), "got {err}");
}
