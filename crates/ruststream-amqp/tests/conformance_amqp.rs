//! Conformance: the suites the in-process transport can answer for on its own, and every suite
//! again against a real broker (gated behind `AMQP_TEST_URL`).
//!
//! The in-process half runs the production broker through its in-process mode, so the descriptor
//! and the publish policies under test are the ones a service ships: the framework's own contract
//! suites keep the in-process transport honest rather than merely compiling. The live half holds
//! the in-process transport to the server as well: its backlog and its refusals must answer as the
//! server does.
//!
//! Start a broker for the gated half with `just brokers-up` (`ActiveMQ` Artemis), then:
//! `AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 cargo test --all-features`.

// `make_source` / `make_publisher` stay closures: their bounds are higher-ranked (`Fn(&str) -> _`
// / `Fn(&B) -> _`), so a bare method path, which binds one concrete lifetime, would not type-check.
#![cfg(feature = "testing")]
#![allow(clippy::redundant_closure, clippy::redundant_closure_for_method_calls)]

use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::helpers::unique_subject;
use ruststream::conformance::in_process::{self as in_process_checks, Refusal};
use ruststream::conformance::{capabilities, harness, lifecycle, message_shape, retry};
use ruststream::testing::Backlog;
use ruststream::{Bytes, HeaderMap, Name};
use ruststream_amqp::{AmqpAddress, AmqpBroker, PARTITION_KEY_HEADER, Sasl};

mod live;

/// The broker's address in process; the in-process mode dials nothing.
const URL: &str = "amqp://broker.example.com:5672";

/// The production broker, connected in process by the suites that call `connect`.
fn in_process() -> InProcessBroker<AmqpBroker> {
    InProcessBroker::new(AmqpBroker::new(URL))
}

/// The broker URL, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing one is a failure.
fn test_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

/// A key carried the way this broker carries one: the header its publish maps onto `group-id`.
fn key_header(key: &[u8], headers: &mut HeaderMap) -> Option<()> {
    headers.insert(PARTITION_KEY_HEADER, Bytes::copy_from_slice(key));
    None
}

/// Both transports batch through the same client-side buffer, so the in-process one proves the
/// contract - a batch never longer than the size it was opened with - without a server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_batches_suite() {
    capabilities::batches(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// The whole ladder in process, through the production descriptor: sync `new`, `connect`,
/// subscribe, publish, receive, ack, `shutdown`, and a publisher that aliased the transport
/// reporting an error afterwards rather than routing into a closed transport.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_lifecycle() {
    harness::lifecycle(
        in_process,
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// The promise an addressing descriptor makes: publish to the address it reports and the
/// subscription that reported it gets the message. The runtime publishes a deferred `retry_after`
/// copy exactly like that, so an address reaching nothing would lose every delayed message. A bare
/// name makes the same promise for `#[subscriber("orders")]`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_reports_a_redelivery_address_that_arrives() {
    retry::redelivery_address(
        in_process,
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
    )
    .await;
    retry::redelivery_address(
        in_process,
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// A key rides `group-id` and comes back on the delivery; one key keeps its order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_keeps_keyed_order() {
    message_shape::keyed_order(
        in_process,
        &unique_subject("conformance.keyed"),
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
        key_header,
    )
    .await;
}

/// The broker describes itself twice - as a server coordinate and as the bindings of its
/// descriptor - and both halves are published, so neither may carry the password the deployment
/// configured.
#[cfg(feature = "asyncapi")]
#[test]
fn amqp_broker_describes_itself_without_credentials() {
    harness::describes_without_credentials(
        &AmqpBroker::new("amqp://svc:hunter2@broker.example.com:5672")
            .sasl(Sasl::plain("svc", "hunter2")),
        &AmqpAddress::queue("orders"),
        "hunter2",
    );
    // The broker connects to one address: it takes the first of the list.
    message_shape::describes_addresses_without_credentials(
        |addrs| AmqpBroker::new(addrs[0]),
        "amqp",
    );
}

/// The publish policies describe their positions without the password.
#[cfg(feature = "asyncapi")]
#[test]
fn amqp_publish_policies_describe_themselves_without_credentials() {
    message_shape::publishes_without_credentials::<ruststream_amqp::ConnectedAmqpBroker, _>(
        &ruststream_amqp::AmqpPublish,
        "hunter2",
    );
    #[cfg(feature = "transaction")]
    message_shape::publishes_without_credentials::<ruststream_amqp::ConnectedAmqpBroker, _>(
        &ruststream_amqp::AmqpTransactionalPublish,
        "hunter2",
    );
}

/// The in-process request/reply: correlation, reply routing, and the leg nobody answers, which
/// must fail once its timeout elapses instead of hanging or resolving with someone else's reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_request_reply_suite() {
    Box::pin(capabilities::request_reply(
        in_process,
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
        |connected| connected.publisher(),
    ))
    .await;
}

/// The in-process transaction: nothing visible before the commit, the buffer visible in publish
/// order after it, an abort discarding it, and misuse (double begin, commit or abort with nothing
/// open) reported rather than silently accepted.
#[cfg(feature = "transaction")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_process_passes_transactions_suite() {
    Box::pin(capabilities::transactions(
        in_process,
        |name| AmqpAddress::queue(name),
        |connected| connected.transactional_publisher(),
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_broker_passes_lifecycle() {
    let Some(url) = test_url() else { return };
    harness::lifecycle(
        move || AmqpBroker::new(url.clone()),
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
    )
    .await;
}

/// The same promise against a server, where the node the address names is the broker's and not
/// this process's to arrange.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_broker_reports_a_redelivery_address_that_arrives() {
    let Some(url) = test_url() else { return };
    retry::redelivery_address(
        || AmqpBroker::new(url.clone()),
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
    )
    .await;
    retry::redelivery_address(
        || AmqpBroker::new(url.clone()),
        |name| Name::new(name.to_owned()),
        |connected| connected.publisher(),
    )
    .await;
}

/// A shutdown finishes the acknowledgement and the publish handed to it just before. A queue keeps
/// what reaches it for the next connection, so the acknowledgement must have landed and the publish
/// must be there. The check needs two connections to one server, which in process are two worlds,
/// so it runs live.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_broker_shutdown_flushes() {
    let Some(url) = test_url() else { return };
    lifecycle::shutdown_flushes(
        || AmqpBroker::new(url.clone()),
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
        Backlog::Delivered,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_broker_keeps_keyed_order() {
    let Some(url) = test_url() else { return };
    message_shape::keyed_order(
        || AmqpBroker::new(url.clone()),
        &unique_subject("conformance.keyed"),
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
        key_header,
    )
    .await;
}

/// What a subscription opened by name receives of the messages published before it: the
/// in-process declaration against the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_broker_backlog_matches_in_process() {
    let Some(url) = test_url() else { return };
    in_process_checks::backlog_matches_server(
        || AmqpBroker::new(url.clone()),
        |connected| connected.publisher(),
    )
    .await;
}

/// What the server refuses, the in-process transport refuses too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_broker_refusals_match_in_process() {
    let Some(url) = test_url() else { return };
    in_process_checks::refuses_like_the_server(
        move || AmqpBroker::new(url.clone()),
        |connected| connected.publisher(),
        [
            Refusal::Publish {
                name: String::new(),
            },
            Refusal::Subscription {
                source: AmqpAddress::queue(""),
            },
        ],
    )
    .await;
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_broker_passes_request_reply_suite() {
    let Some(url) = test_url() else { return };
    Box::pin(capabilities::request_reply(
        || AmqpBroker::new(url.clone()),
        |name| AmqpAddress::queue(name),
        |connected| connected.publisher(),
        |connected| connected.publisher(),
    ))
    .await;
}

#[cfg(feature = "transaction")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn amqp_broker_passes_transactions_suite() {
    let Some(url) = test_url() else { return };
    Box::pin(capabilities::transactions(
        || AmqpBroker::new(url.clone()),
        |name| AmqpAddress::queue(name),
        |connected| connected.transactional_publisher(),
    ))
    .await;
}
