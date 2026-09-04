//! A batch handler mounted on the in-process broker: what the mount site names is what the body
//! is handed.
//!
//! The conformance suite checks the capability itself; this pins the runtime path, where the size
//! travels from `batch(..)` at the mount site down to the subscriber that builds the batches.

#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream_amqp::prelude::*;
use ruststream_amqp::testing::AmqpTestBroker;
use serde::{Deserialize, Serialize};

/// The batch size the mount below names, spelled once so the assertion cannot drift from it.
const SIZE: usize = 2;

/// The derive names no destination, so the publishes below keep saying `to("orders")`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn settle(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_carries_at_most_the_size_the_mount_named() {
    // A producer handle taken off the broker publishes as an external client does, without
    // driving the reaction to a standstill on every message - which an injection through the
    // harness would do, closing each batch after a single delivery.
    let broker = AmqpTestBroker::new();
    let producer = broker.publisher();

    let app = RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(settle.batch(nonzero!(SIZE)));
    });
    let app = TestApp::start(app).await.expect("startup failed");

    for id in 0..5 {
        producer
            .message(&Order { id })
            .to("orders")
            .publish()
            .await
            .expect("publish failed");
    }
    app.settle().await.expect("the run settles");

    // How the five split across batches is the buffer's business (its deadline may close one
    // early); that none of them is longer than the mount named is the contract.
    let batches: Vec<Vec<Order>> = app
        .broker::<AmqpTestBroker>()
        .subscriber("orders")
        .batches();
    assert!(
        batches
            .iter()
            .all(|batch| (1..=SIZE).contains(&batch.len())),
        "batches must be non-empty and no longer than {SIZE}, got {:?}",
        batches.iter().map(Vec::len).collect::<Vec<_>>(),
    );
    assert_eq!(
        batches.concat(),
        (0..5).map(|id| Order { id }).collect::<Vec<_>>(),
        "every order must arrive, in order",
    );

    app.broker::<AmqpTestBroker>()
        .subscriber("orders")
        .settled(HandlerOutcome::ack());

    app.shutdown().await.expect("shutdown failed");
}
