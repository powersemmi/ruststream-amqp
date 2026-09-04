//! A page handler mounted on the in-process broker: what the mount site names is what the body
//! is handed.
//!
//! The conformance suite checks the capability itself; this pins the runtime path, where the size
//! travels from `batch(..)` at the mount site down to the subscriber that builds the pages.

#![cfg(feature = "testing")]

use std::sync::Mutex;

use ruststream::testing::TestApp;
use ruststream_amqp::prelude::*;
use ruststream_amqp::testing::AmqpTestBroker;
use serde::{Deserialize, Serialize};

/// The page size the mount below names, spelled once so the assertion cannot drift from it.
const PAGE: usize = 2;

static PAGES: Mutex<Vec<usize>> = Mutex::new(Vec::new());

/// The derive names no destination, so the publishes below keep saying `to("orders")`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn settle(orders: &[Order]) -> HandlerOutcome {
    PAGES
        .lock()
        .expect("no handler panics here")
        .push(orders.len());
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_page_carries_at_most_the_size_the_mount_named() {
    let app =
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(AmqpTestBroker::new(), |b| {
            b.include(settle.batch(nonzero!(PAGE)));
        });
    let app = TestApp::start(app).await.expect("startup failed");

    for id in 0..5 {
        app.message(&Order { id })
            .to("orders")
            .publish()
            .await
            .expect("publish failed");
    }

    // How the five split across pages is the buffer's business (its deadline may close a page
    // early); that none of them is longer than the mount named is the contract.
    let pages = PAGES.lock().expect("no handler panics here").clone();
    assert!(
        pages.iter().all(|len| (1..=PAGE).contains(len)),
        "pages must be non-empty and no longer than {PAGE}, got {pages:?}",
    );
    assert_eq!(pages.iter().sum::<usize>(), 5, "every order must arrive");

    let received: Vec<Order> = app
        .broker::<AmqpTestBroker>()
        .subscriber("orders")
        .received();
    assert_eq!(received, (0..5).map(|id| Order { id }).collect::<Vec<_>>());
}
