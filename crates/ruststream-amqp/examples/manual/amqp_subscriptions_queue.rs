//! The service of `examples/amqp_subscriptions_queue.rs` written without the macros: the queue
//! descriptor goes straight to `subscriber`, and the subscriptions on the address compete.
//!
//! ```text
//! cargo test --example manual_amqp_subscriptions_queue --features testing,asyncapi
//! cargo run --example manual_amqp_subscriptions_queue -- run
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

// --8<-- [start:queue]
/// Every replica of the service competes for `orders`: each order is fulfilled once.
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

/// The queue terminus: the subscriptions on `orders` compete for its messages.
fn orders() -> AmqpAddress {
    AmqpAddress::queue("orders")
}

fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("fulfillment", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(orders(), Fulfil).build());
    })
}
// --8<-- [end:queue]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Fulfil, Order, app, orders};

    #[tokio::test]
    async fn an_order_is_fulfilled_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 1 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    /// Two subscriptions on one queue address, as two replicas of the service open them: the
    /// orders are shared out, none is handled twice and none is lost.
    #[tokio::test]
    async fn two_replicas_share_the_orders() -> Result<(), Box<dyn Error>> {
        let replicas = RustStream::new(AppInfo::new("fulfillment", "0.1.0")).with_broker(
            AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
            |b| {
                for _ in 0..2 {
                    b.include(subscriber(orders(), Fulfil).build());
                }
            },
        );
        let tb = TestApp::start(replicas).await?;

        for id in 1..=4 {
            tb.broker::<AmqpBroker>()
                .message(&Order { id })
                .publish()
                .await?;
        }

        let mut ids: Vec<u64> = tb
            .broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called(4)
            .received::<Order>()
            .into_iter()
            .map(|order| order.id)
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, [1, 2, 3, 4]);
        Ok(())
    }
}
