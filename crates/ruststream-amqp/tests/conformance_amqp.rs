//! Conformance: the suites the in-process transport can answer for on its own, and every suite
//! again against a real broker (gated behind `AMQP_TEST_URL`).
//!
//! The stand-in now carries the production descriptor and the production publish policies, so the
//! framework's own contract suites run against it unchanged - which is what keeps its emulation
//! honest rather than merely compiling.
//!
//! Start a broker for the gated half with `just brokers-up` (`ActiveMQ` Artemis), then:
//! `AMQP_TEST_URL=amqp://guest:guest@127.0.0.1:5672 cargo test --all-features`.

#![cfg(feature = "testing")]

use ruststream::Name;
use ruststream::conformance::{capabilities, harness};
use ruststream_amqp::testing::{AmqpTestBroker, ConnectedAmqpTestBroker};
use ruststream_amqp::{AmqpAddress, AmqpBroker};

mod live;

/// The broker URL, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing one is a failure.
fn test_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_test_broker_passes_conformance_suite() {
    harness::run_suite(AmqpTestBroker::new).await;
}

/// Both brokers batch through the same client-side buffer, so the in-process one proves the
/// contract - a batch never longer than the size it was opened with - without a server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_test_broker_passes_batches_suite() {
    capabilities::batches(
        AmqpTestBroker::new,
        |name| Name::new(name.to_owned()),
        ConnectedAmqpTestBroker::publisher,
    )
    .await;
}

/// The whole ladder in process, through the production descriptor: sync `new`, `connect`,
/// subscribe, publish, receive, ack, `shutdown`, and a publisher that aliased the transport
/// reporting an error afterwards rather than routing into a dead router.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_test_broker_passes_lifecycle() {
    harness::lifecycle(
        AmqpTestBroker::new,
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// The in-process request/reply: correlation, reply routing, and the leg nobody answers, which
/// must fail once its timeout elapses instead of hanging or resolving with someone else's reply.
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_test_broker_passes_request_reply_suite() {
    capabilities::request_reply(
        AmqpTestBroker::new,
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
        |connected| connected.publisher(),
    )
    .await;
}

/// The in-process transaction: nothing visible before the commit, the buffer visible in publish
/// order after it, an abort discarding it, and misuse (double begin, commit or abort with nothing
/// open) reported rather than silently accepted.
#[cfg(feature = "transaction")]
#[allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_test_broker_passes_transactions_suite() {
    capabilities::transactions(
        AmqpTestBroker::new,
        |name| AmqpAddress::queue(name),
        |connected| connected.transactional_publisher(),
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
