//! The service of the production guide: an orders service on a broker that takes SASL PLAIN over
//! TLS, with its address and credentials read from the environment.
//!
//! ```text
//! AMQP_URL=amqps://orders.example.com AMQP_USERNAME=orders-svc AMQP_PASSWORD=... \
//!     cargo run --example guide_production --features testing,rustls -- run
//! ```

use std::env;

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:broker]
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
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

// --8<-- [start:service]
#[subscriber(AmqpAddress::queue("orders"), reply)]
async fn confirm(order: &Order) -> Confirmation {
    tracing::info!(order.id, "order confirmed");
    Confirmation { id: order.id }
}

fn service(broker: AmqpBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(confirm).out_reply(Publish);
    })
}

#[ruststream::app]
fn app() -> impl App {
    service(broker(Endpoint::from_env()))
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Confirmation, Endpoint, Order, broker, service};

    fn endpoint(url: &str) -> Endpoint {
        Endpoint {
            url: url.to_owned(),
            username: "orders-svc".to_owned(),
            password: "s3cret/pa55?".to_owned(),
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
