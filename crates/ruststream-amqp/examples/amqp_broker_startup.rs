//! Startup: the runtime connects the broker once, before any handler runs, and a broker that
//! refuses the connection stops the service with `AmqpError::Connect`.
//!
//! ```text
//! cargo test --example amqp_broker_startup --features testing
//! just brokers-up
//! AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example amqp_broker_startup --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;

use serde::{Deserialize, Serialize};

use ruststream_amqp::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[subscriber(AmqpAddress::queue("orders"))]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

// --8<-- [start:app]
/// The service on the broker at `url`.
fn service(url: &str) -> impl App<State = ()> + use<> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(AmqpBroker::new(url), |b| {
        b.include(place);
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    // The deployment names the broker, credentials included.
    let url = env::var("AMQP_URL").unwrap_or_else(|_| "amqp://localhost:5672".to_owned());
    service(&url)
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;
    use url::Url;

    use super::{Order, live, service};

    /// The `AmqpError` a failed startup carries, found along the chain of sources.
    fn amqp_error<'a>(failed: Option<&'a (dyn Error + 'static)>) -> Option<&'a AmqpError> {
        let mut cause = failed;
        while let Some(err) = cause {
            if let Some(amqp) = err.downcast_ref::<AmqpError>() {
                return Some(amqp);
            }
            cause = err.source();
        }
        None
    }

    #[tokio::test]
    async fn the_started_service_handles_an_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service("amqp://broker.internal:5672")).await?;
        tb.broker::<AmqpBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn a_url_the_client_cannot_open_fails_startup() {
        let failed = TestApp::start(service("http://broker.internal:5672"))
            .await
            .err();

        let amqp = amqp_error(failed.as_ref().map(|err| err as _));
        assert!(
            matches!(amqp, Some(AmqpError::Connect(_))),
            "startup did not fail with AmqpError::Connect: {failed:?}"
        );
    }

    /// The stand's URL from `AMQP_TEST_URL`, or `None` to skip.
    fn stand() -> Option<Url> {
        Url::parse(&live::url("AMQP_TEST_URL")?).ok()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_wrong_password_fails_startup() {
        let Some(mut url) = stand() else { return };
        let wrong = format!("{}-wrong", url.password().unwrap_or_default());
        url.set_password(Some(&wrong))
            .expect("the stand's URL takes a password");

        let failed = TestApp::start_live(service(url.as_str())).await.err();

        let amqp = amqp_error(failed.as_ref().map(|err| err as _));
        assert!(
            matches!(amqp, Some(AmqpError::Connect(_))),
            "startup did not fail with AmqpError::Connect: {failed:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_port_nobody_listens_on_fails_startup() {
        let Some(mut url) = stand() else { return };
        // Port 1 is reserved and has no listener, so the socket is refused at once.
        url.set_port(Some(1)).expect("the stand's URL takes a port");

        let failed = TestApp::start_live(service(url.as_str())).await.err();

        let amqp = amqp_error(failed.as_ref().map(|err| err as _));
        assert!(
            matches!(amqp, Some(AmqpError::Connect(_))),
            "startup did not fail with AmqpError::Connect: {failed:?}"
        );
    }
}
