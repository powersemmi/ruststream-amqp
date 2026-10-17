//! The service of `examples/amqp_broker_container.rs` without the macros: the handler is a named
//! type, the address descriptor is mounted with `map_source`, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_amqp_broker_container --features testing,asyncapi
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_amqp::prelude::*;

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

// --8<-- [start:broker]
/// The broker, which knows this service as `orders-svc`.
fn broker() -> AmqpBroker {
    AmqpBroker::new("amqp://artemis:artemis@broker.internal:5672").container_id("orders-svc")
}

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker(), |b| {
        let place = subscriber("orders", Place).map_source(|_| AmqpAddress::queue("orders"));
        b.include(place.build());
    })
}
// --8<-- [end:broker]

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

    use ruststream::asyncapi::build_spec;
    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;
    use serde_json::Value;

    use super::{Order, Place, app, broker};

    #[tokio::test]
    async fn the_named_service_handles_an_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
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

    /// The server the generated document reports for `broker`, mounted under the label `amqp`.
    fn server(broker: AmqpBroker) -> Result<Value, Box<dyn Error>> {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_labeled(
            "amqp",
            broker,
            |b| {
                let place =
                    subscriber("orders", Place).map_source(|_| AmqpAddress::queue("orders"));
                b.include(place.build());
            },
        );
        let document: Value = serde_json::from_str(&build_spec(&app).to_json()?)?;
        Ok(document["servers"]["amqp"].clone())
    }

    #[test]
    fn the_document_names_the_container_and_no_credentials() -> Result<(), Box<dyn Error>> {
        let server = server(broker())?;

        assert_eq!(
            server["bindings"]["x-ruststream-amqp1"]["containerId"],
            "orders-svc"
        );
        assert_eq!(server["host"], "broker.internal:5672");
        Ok(())
    }

    #[test]
    fn a_service_that_names_no_container_presents_ruststream() -> Result<(), Box<dyn Error>> {
        let server = server(AmqpBroker::new("amqp://broker.internal:5672"))?;

        assert_eq!(
            server["bindings"]["x-ruststream-amqp1"]["containerId"],
            "ruststream"
        );
        Ok(())
    }
}
