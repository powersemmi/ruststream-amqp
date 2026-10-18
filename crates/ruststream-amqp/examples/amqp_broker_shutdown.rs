//! Shutdown: handlers still running get a bounded time to finish, then the broker closes its
//! subscriptions, its publisher links and the connection, from the inside out. The in-process test
//! runs everywhere; the live one runs against a running broker and skips without one.
//!
//! ```text
//! cargo test --example amqp_broker_shutdown --features testing
//! just brokers-up
//! AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example amqp_broker_shutdown --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::process;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use ruststream_amqp::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

/// The address orders arrive on. Live runs share one broker, so each process reads its own.
fn orders() -> String {
    format!("broker-shutdown.orders.{}", process::id())
}

#[subscriber(AmqpAddress::queue(orders()))]
async fn place(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order placed");
    HandlerOutcome::ack()
}

// --8<-- [start:app]
/// The service on `broker`. Once shutdown begins, the handlers still running get ten seconds to
/// finish before the broker closes.
fn service(broker: AmqpBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(broker, |b| {
            b.include(place);
        })
        .shutdown_timeout(Duration::from_secs(10))
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(AmqpBroker::new("amqp://artemis:artemis@localhost:5672"))
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Order, live, orders, service};

    /// The body both modes run: an order is placed, and the shutdown that follows, with the
    /// subscription still open, reports no error.
    async fn places_then_shuts_down(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
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
    async fn in_process() -> Result<(), Box<dyn Error>> {
        let broker = AmqpBroker::new("amqp://broker.internal:5672");
        places_then_shuts_down(TestApp::start(service(broker)).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("AMQP_TEST_URL") else {
            return Ok(());
        };
        places_then_shuts_down(TestApp::start_live(service(AmqpBroker::new(url))).await?).await
    }
}
