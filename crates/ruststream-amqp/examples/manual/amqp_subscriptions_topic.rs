//! The service of `examples/amqp_subscriptions_topic.rs` written without the macros: two topic
//! subscriptions on one address, each with a copy of its own.
//!
//! ```text
//! cargo test --example manual_amqp_subscriptions_topic --features testing,asyncapi
//! cargo run --example manual_amqp_subscriptions_topic -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct OrderPlaced {
    id: u64,
}

// `#[outgoing(name = "orders.placed")]` by hand: the type fixes its destination.
impl OutgoingDestination for OrderPlaced {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders.placed";
}

impl MessageHeaders for OrderPlaced {
    type Contract = NoHeaders;
}

// --8<-- [start:topic]
/// Audit keeps a record of every placed order.
struct Audit;

impl Handle<OrderPlaced> for Audit {
    fn handle(
        &self,
        event: &OrderPlaced,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(event.id, "order recorded");
        ready(Ok(()))
    }
}

/// Notifications tell the customer about the same orders, from a copy of their own.
struct Notify;

impl Handle<OrderPlaced> for Notify {
    fn handle(
        &self,
        event: &OrderPlaced,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(event.id, "customer notified");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber("orders.placed", Audit)
                .build()
                .map_source(|_| AmqpAddress::topic("orders.placed")),
        );
        b.include(
            subscriber("orders.placed", Notify)
                .build()
                .map_source(|_| AmqpAddress::topic("orders.placed")),
        );
    })
}
// --8<-- [end:topic]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{OrderPlaced, app};

    #[tokio::test]
    async fn each_subscription_gets_its_own_copy() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&OrderPlaced { id: 1 })
            .publish()
            .await?;

        // One publish, two handler calls: audit's copy and notify's copy.
        let copies = tb
            .broker::<AmqpBroker>()
            .subscriber("orders.placed")
            .assert_called(2)
            .received::<OrderPlaced>();
        assert_eq!(copies, [OrderPlaced { id: 1 }, OrderPlaced { id: 1 }]);
        Ok(())
    }
}
