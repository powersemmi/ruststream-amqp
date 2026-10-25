//! The TLS settings: SASL EXTERNAL logging in by the client certificate on a mutual-TLS listener,
//! and the refusals around it.
//!
//! The live cases are gated behind `AMQP_TEST_TLS_URL`, `AMQP_TEST_TLS_PLAIN_URL` and
//! `AMQP_TEST_TLS_DIR`: the stand's mutual-TLS listener, its TLS listener with the password login,
//! and the directory its certificates were generated into. `just brokers-up` generates them and
//! starts the stand, then:
//! `AMQP_TEST_TLS_URL=amqps://localhost:5671
//! AMQP_TEST_TLS_PLAIN_URL=amqps://artemis:artemis@localhost:5673
//! AMQP_TEST_TLS_DIR=$PWD/.stand-tls cargo test --all-features --test suite tls::`.

#![cfg(any(feature = "rustls", feature = "native-tls"))]

use std::error::Error;
use std::path::{Path, PathBuf};
use std::{fs, process};

use ruststream::testing::TestApp;
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

use crate::live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// The queue orders arrive on. Live runs share one broker, so each process reads its own.
fn orders() -> String {
    format!("it.tls.orders.{}", process::id())
}

#[subscriber(AmqpAddress::queue(orders()))]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

fn service(broker: AmqpBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(place);
    })
}

/// The `AmqpError` behind a failed start, or a panic naming what failed instead.
fn amqp_error<'a>(failed: Option<&'a (dyn Error + 'static)>) -> &'a AmqpError {
    let mut cause = failed;
    while let Some(err) = cause {
        if let Some(amqp) = err.downcast_ref::<AmqpError>() {
            return amqp;
        }
        cause = err.source();
    }
    panic!("startup did not fail with an AmqpError: {failed:?}");
}

#[tokio::test]
async fn tls_settings_on_an_amqp_url_fail_startup() {
    let broker = AmqpBroker::new("amqp://broker.internal:5672")
        .tls(Tls::new())
        .sasl(Sasl::external());

    let failed = TestApp::start(service(broker)).await.err();

    let refused = amqp_error(failed.as_ref().map(|err| err as _));
    assert!(
        matches!(refused, AmqpError::Connect(reason) if reason.to_string().contains("amqps://")),
        "{refused}"
    );
}

/// A test runs the service's logic, so it starts without the deployment's certificates.
#[tokio::test]
async fn a_test_starts_without_reading_the_certificates() -> Result<(), Box<dyn Error>> {
    let tls = Tls::new()
        .root_certificates("the deployment's authority")
        .client_identity("the deployment's certificate", "the deployment's key");
    let broker = AmqpBroker::new("amqps://broker.internal:5671")
        .tls(tls)
        .sasl(Sasl::external());
    places_an_order(TestApp::start(service(broker)).await?).await
}

/// The body every mode runs: an order reaches the handler on the connection.
async fn places_an_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
    tb.broker::<AmqpBroker>()
        .message(&Order { id: 7 })
        .to(orders().as_str())
        .publish()
        .await?;

    tb.broker::<AmqpBroker>()
        .subscriber(&orders())
        .assert_called_once()
        .with(&Order { id: 7 })
        .settled(HandlerOutcome::ack());
    tb.shutdown().await?;
    Ok(())
}

/// The stand's mutual-TLS listener and its certificate directory, or `None` to skip.
fn stand() -> Option<(String, PathBuf)> {
    let url = live::url("AMQP_TEST_TLS_URL")?;
    let dir = live::url("AMQP_TEST_TLS_DIR")?;
    Some((url, PathBuf::from(dir)))
}

fn pem(dir: &Path, name: &str) -> Vec<u8> {
    let path = dir.join(name);
    fs::read(&path).unwrap_or_else(|err| panic!("{}: {err}", path.display()))
}

/// The broker on the stand's listener, logging in with SASL EXTERNAL through `tls`.
fn external(url: &str, tls: Tls) -> AmqpBroker {
    AmqpBroker::new(url)
        .container_id(format!("tls-{}", process::id()))
        .tls(tls)
        .sasl(Sasl::external())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_logs_in_with_the_client_certificate() -> Result<(), Box<dyn Error>> {
    let Some((url, dir)) = stand() else {
        return Ok(());
    };
    let tls = Tls::new()
        .root_certificates(pem(&dir, "ca.crt"))
        .client_identity(pem(&dir, "client.crt"), pem(&dir, "client.key"));

    places_an_order(TestApp::start_live(service(external(&url, tls))).await?).await
}

/// Asserts that a start against the stand with `tls` fails with a connection error.
async fn refused(url: &str, tls: Tls) {
    let failed = TestApp::start_live(service(external(url, tls))).await.err();
    let refused = amqp_error(failed.as_ref().map(|err| err as _));
    assert!(matches!(refused, AmqpError::Connect(_)), "{refused}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_without_a_client_certificate_is_refused() {
    let Some((url, dir)) = stand() else {
        return;
    };
    refused(&url, Tls::new().root_certificates(pem(&dir, "ca.crt"))).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_certificate_the_broker_does_not_trust_is_refused() {
    let Some((url, dir)) = stand() else {
        return;
    };
    let tls = Tls::new()
        .root_certificates(pem(&dir, "ca.crt"))
        .client_identity(pem(&dir, "stranger.crt"), pem(&dir, "stranger.key"));
    refused(&url, tls).await;
}

/// The stand's certificate is signed by its own authority, which no default store holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_outside_the_trusted_authorities_is_refused() {
    let Some((url, dir)) = stand() else {
        return;
    };
    let tls = Tls::new().client_identity(pem(&dir, "client.crt"), pem(&dir, "client.key"));
    refused(&url, tls).await;
}

/// The other profiles stay the client's, run over the TLS this crate opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_logs_in_over_tls_with_the_named_authority() -> Result<(), Box<dyn Error>> {
    let Some((_, dir)) = stand() else {
        return Ok(());
    };
    let Some(url) = live::url("AMQP_TEST_TLS_PLAIN_URL") else {
        return Ok(());
    };
    let broker = AmqpBroker::new(url)
        .container_id(format!("tls-plain-{}", process::id()))
        .tls(Tls::new().root_certificates(pem(&dir, "ca.crt")));

    places_an_order(TestApp::start_live(service(broker)).await?).await
}
