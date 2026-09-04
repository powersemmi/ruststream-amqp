//! Conformance: the routing and paging suites against the in-process transport, and the
//! lifecycle plus capability suites against a real broker (gated behind `AMQP_TEST_URL`).
//!
//! Start a broker for the gated half with `just brokers-up` (`ActiveMQ` Artemis), then:
//! `AMQP_TEST_URL=amqp://guest:guest@127.0.0.1:5672 cargo test --all-features`.

#![cfg(feature = "testing")]

use ruststream::Name;
use ruststream::conformance::{capabilities, harness};
use ruststream_amqp::testing::{AmqpTestBroker, ConnectedAmqpTestBroker};
use ruststream_amqp::{AmqpAddress, AmqpBroker};

fn test_url() -> Option<String> {
    match std::env::var("AMQP_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            eprintln!("AMQP_TEST_URL is not set; skipping the live-broker conformance check");
            None
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_test_broker_passes_conformance_suite() {
    harness::run_suite(AmqpTestBroker::new).await;
}

/// Both brokers page through the same client-side buffer, so the in-process one proves the
/// contract - a page never longer than the size it was opened with - without a server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_test_broker_passes_batches_suite() {
    capabilities::batches(
        AmqpTestBroker::new,
        |name| Name::new(name.to_owned()),
        ConnectedAmqpTestBroker::publisher,
    )
    .await;
}

// `make_source` / `make_publisher` must stay closures: their bounds are higher-ranked
// (`Fn(&str) -> _` / `Fn(&B) -> _`), so a bare method path - which binds one concrete lifetime -
// would not type-check.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_broker_passes_lifecycle() {
    let Some(url) = test_url() else { return };
    harness::lifecycle(
        || AmqpBroker::new(url.clone()),
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_broker_passes_batches_suite() {
    let Some(url) = test_url() else { return };
    capabilities::batches(
        || AmqpBroker::new(url.clone()),
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
    )
    .await;
}

#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_broker_passes_request_reply_suite() {
    let Some(url) = test_url() else { return };
    capabilities::request_reply(
        || AmqpBroker::new(url.clone()),
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
        |connected| connected.publisher(),
    )
    .await;
}

#[cfg(feature = "transaction")]
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_broker_passes_transactions_suite() {
    let Some(url) = test_url() else { return };
    capabilities::transactions(
        || AmqpBroker::new(url.clone()),
        |name| AmqpAddress::queue(name),
        |connected| connected.transactional_publisher(),
    )
    .await;
}
