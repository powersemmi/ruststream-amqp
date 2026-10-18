//! `container_id`: the name a service presents to the broker, which an operator finds its
//! connection and links by in the broker's console.
//!
//! ```text
//! cargo test --example amqp_broker_container --features testing,asyncapi
//! ```

use serde::{Deserialize, Serialize};

use ruststream_amqp::prelude::*;

#[subscriber(AmqpAddress::queue("orders"))]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

// --8<-- [start:broker]
/// The broker, which knows this service as `orders-svc`.
fn broker() -> AmqpBroker {
    AmqpBroker::new("amqp://artemis:artemis@broker.internal:5672").container_id("orders-svc")
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker(), |b| {
        b.include(place);
    })
}
// --8<-- [end:broker]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;
    use serde_json::Value;

    use super::{Order, app, broker, place};

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
                b.include(place);
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
