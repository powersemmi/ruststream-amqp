//! The service of `examples/amqp_subscriptions_raw.rs` written without the macros: a verbatim
//! address goes straight to `subscriber` and reaches the broker as written.
//!
//! ```text
//! cargo test --example manual_amqp_subscriptions_raw --features testing,asyncapi
//! cargo run --example manual_amqp_subscriptions_raw -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

/// The type names no destination, so a publish names the address.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Order {
    id: u64,
}

// `#[derive(Outgoing)]` with no name, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:raw]
/// `RabbitMQ` 4.x addresses a queue by its v2 path; the string reaches the broker as written.
struct Fulfil;

impl Handle<Order> for Fulfil {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order fulfilled");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://guest:guest@localhost:5672");
    RustStream::new(AppInfo::new("fulfillment", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(AmqpAddress::raw("/queues/orders"), Fulfil).build());
    })
}
// --8<-- [end:raw]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Order, app};

    #[tokio::test]
    async fn the_address_is_read_as_written() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&Order { id: 1 })
            .to("/queues/orders")
            .publish()
            .await?;
        // A neighbouring spelling is another node: nothing about the address is a pattern.
        tb.broker::<AmqpBroker>()
            .message(&Order { id: 2 })
            .to("orders")
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("/queues/orders")
            .assert_called_once()
            .with(&Order { id: 1 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
