//! The service of `examples/amqp_broker_startup.rs` without the macros: the handler is a named
//! type, `subscriber` takes the address descriptor, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_amqp_broker_startup --features testing,asyncapi
//! just brokers-up
//! AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example manual_amqp_broker_startup --features testing,asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_amqp::prelude::*;

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

struct Place;

impl Handle<Order> for Place {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order placed");
        ready(Ok(()))
    }
}

// --8<-- [start:app]
/// The service on the broker at `url`.
fn service(url: &str) -> impl App<State = ()> + use<> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(AmqpBroker::new(url), |b| {
        let place = subscriber(AmqpAddress::queue("orders"), Place);
        b.include(place.build());
    })
}

fn app() -> impl App<State = ()> {
    // The deployment names the broker, credentials included.
    let url = env::var("AMQP_URL").unwrap_or_else(|_| "amqp://localhost:5672".to_owned());
    service(&url)
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: an order names the address it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
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
