//! The service of `examples/amqp_subscriptions_name.rs` written without the macros: a name in
//! `subscriber(..)` opens a receiver on that address, sent to the broker as written.
//!
//! ```text
//! cargo test --example manual_amqp_subscriptions_name --features testing,asyncapi
//! cargo run --example manual_amqp_subscriptions_name -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: the type fixes its destination.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:name]
/// Reads the address `orders`; the name reaches the broker as written.
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

fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber("orders", Place).build());
    })
}
// --8<-- [end:name]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Order, Place, app};

    #[tokio::test]
    async fn the_address_named_is_the_one_read() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 7 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_name_stops_the_service_at_startup() {
        let unaddressed = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
            |b| {
                b.include(subscriber("", Place).build());
            },
        );

        let refused = TestApp::start(unaddressed)
            .await
            .expect_err("an empty address cannot form a subscription");
        assert!(refused.to_string().contains("empty"), "{refused}");
    }
}
