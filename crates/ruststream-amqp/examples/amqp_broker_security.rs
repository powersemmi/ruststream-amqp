//! A service on a broker that authenticates its clients: SASL PLAIN with the service's own
//! credentials over TLS. An `amqps://` URL needs the `rustls` or the `native-tls` feature.
//!
//! ```text
//! cargo test --example amqp_broker_security --features testing,rustls
//! just brokers-up
//! AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example amqp_broker_security --features testing,rustls
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::{env, process};

use serde::{Deserialize, Serialize};

use ruststream_amqp::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

/// The address orders arrive on. Live runs share one broker, so each process reads its own.
fn orders() -> String {
    format!("broker-security.orders.{}", process::id())
}

#[subscriber(AmqpAddress::queue(orders()))]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

// --8<-- [start:broker]
/// The broker at `url`, which carries no credentials: the service logs in with SASL PLAIN.
fn broker(url: &str, user: &str, password: &str) -> AmqpBroker {
    AmqpBroker::new(url).sasl(Sasl::plain(user, password))
}

fn service(broker: AmqpBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(place);
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    let password = env::var("AMQP_PASSWORD").unwrap_or_default();
    service(broker(
        "amqps://broker.internal:5671",
        "orders-svc",
        &password,
    ))
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;
    use url::Url;

    use super::{Order, broker, live, orders, service};

    /// The body both modes run: an order reaches the handler on an authenticated connection.
    async fn places_an_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<AmqpBroker>()
            .message(&Order { id: 7 })
            .to(orders().as_str())
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber(&orders())
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn the_secured_service_handles_an_order() -> Result<(), Box<dyn Error>> {
        let broker = broker("amqps://broker.internal:5671", "orders-svc", "secret");
        places_an_order(TestApp::start(service(broker)).await?).await
    }

    /// The stand's address without its userinfo, and the user and password the userinfo held.
    fn stand() -> Option<(String, String, String)> {
        let mut url = Url::parse(&live::url("AMQP_TEST_URL")?).ok()?;
        let user = url.username().to_owned();
        let password = url.password()?.to_owned();
        url.set_username("").ok()?;
        url.set_password(None).ok()?;
        Some((url.to_string(), user, password))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_service_logs_in_with_its_own_credentials() -> Result<(), Box<dyn Error>> {
        let Some((url, user, password)) = stand() else {
            return Ok(());
        };
        let tb = TestApp::start_live(service(broker(&url, &user, &password))).await?;
        places_an_order(tb).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_wrong_password_fails_startup() {
        let Some((url, user, password)) = stand() else {
            return;
        };
        let wrong = format!("{password}-wrong");

        let failed = TestApp::start_live(service(broker(&url, &user, &wrong)))
            .await
            .err();
        let mut cause: Option<&(dyn Error + 'static)> = failed.as_ref().map(|err| err as _);
        while let Some(err) = cause {
            if let Some(amqp) = err.downcast_ref::<AmqpError>() {
                assert!(matches!(amqp, AmqpError::Connect(_)), "{amqp}");
                return;
            }
            cause = err.source();
        }
        panic!("startup did not fail with an AmqpError: {failed:?}");
    }
}
