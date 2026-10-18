//! The service of the production guide, written without the macros.
//!
//! ```text
//! AMQP_URL=amqps://orders.example.com AMQP_USERNAME=orders-svc AMQP_PASSWORD=... \
//!     cargo run --example manual_guide_production --features testing,rustls -- run
//! ```

use std::env;
use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

/// Where the broker is and who the service is, as the deployment provides them.
struct Endpoint {
    url: String,
    username: String,
    password: String,
}

impl Endpoint {
    fn from_env() -> Self {
        let read = |name: &str| env::var(name).unwrap_or_default();
        Self {
            url: read("AMQP_URL"),
            username: read("AMQP_USERNAME"),
            password: read("AMQP_PASSWORD"),
        }
    }
}

fn broker(endpoint: Endpoint) -> AmqpBroker {
    AmqpBroker::new(endpoint.url)
        .sasl(Sasl::plain(endpoint.username, endpoint.password))
        .container_id("orders-eu-1")
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

impl MessageInfo for Order {
    const NAME: &'static str = "Order";
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Confirmation {
    id: u64,
}

impl OutgoingDestination for Confirmation {
    type Form = FixedName;

    const DESTINATION: &'static str = "confirmations";
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

impl MessageInfo for Confirmation {
    const NAME: &'static str = "Confirmation";
}

// --8<-- [start:service]
struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        tracing::info!(order.id, "order confirmed");
        ready(Ok(Confirmation { id: order.id }))
    }
}

fn service(broker: AmqpBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(AmqpAddress::queue("orders"), Confirm)
                .reply()
                .build(),
        )
        .out_reply(Publish);
    })
}

fn app() -> impl App {
    service(broker(Endpoint::from_env()))
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::hash::{BuildHasher, RandomState};

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Confirmation, Endpoint, Order, broker, service};

    /// A password drawn afresh for every run, so no credential lives in the source.
    fn password() -> String {
        format!("{:016x}", RandomState::new().hash_one("password"))
    }

    fn endpoint(url: &str) -> Endpoint {
        Endpoint {
            url: url.to_owned(),
            username: "orders-svc".to_owned(),
            password: password(),
        }
    }

    #[tokio::test]
    async fn the_production_configuration_runs_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker(endpoint("amqps://orders.example.com")))).await?;

        tb.broker::<AmqpBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { id: 1 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_url_the_client_cannot_open_fails_the_start() {
        let refused = broker(endpoint("https://orders.example.com"));

        let error = TestApp::start(service(refused))
            .await
            .err()
            .map(|error| error.to_string());

        assert!(
            error
                .as_deref()
                .is_some_and(|error| error.contains("not an AMQP scheme")),
            "{error:?}",
        );
    }
}
