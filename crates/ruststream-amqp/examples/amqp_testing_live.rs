//! One test body, two modes: in process under `TestApp::start`, and against a running AMQP 1.0
//! broker under `TestApp::start_live`. The live test skips without `AMQP_TEST_URL`, and
//! `RUSTSTREAM_REQUIRE_LIVE=1` turns that skip into a failure.
//!
//! ```text
//! cargo test --example amqp_testing_live --features testing
//! just brokers-up
//! AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 RUSTSTREAM_REQUIRE_LIVE=1 \
//!     cargo test --example amqp_testing_live --features testing
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::process;

use serde::{Deserialize, Serialize};

use ruststream_amqp::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

/// The address payments arrive on. Live runs share one broker, so each process reads its own.
fn payments() -> String {
    format!("testing-live.payments.{}", process::id())
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Payment {
    amount: u64,
}

#[subscriber(AmqpAddress::queue(payments()))]
async fn accept(payment: &Payment) -> HandlerOutcome {
    if payment.amount == 0 {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

/// The service on the broker at `url`.
fn app(url: &str) -> impl App<State = ()> + use<> {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(AmqpBroker::new(url), |b| {
        b.include(accept);
    })
}

#[ruststream::app]
fn service() -> impl App<State = ()> {
    app("amqp://artemis:artemis@localhost:5672")
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Payment, app, live, payments};

    // --8<-- [start:both]
    /// The body both modes run; only the start call differs.
    async fn accepts_a_payment(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<AmqpBroker>()
            .message(&Payment { amount: 100 })
            .to(payments().as_str())
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber(&payments())
            .assert_called_once()
            .with(&Payment { amount: 100 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        accepts_a_payment(TestApp::start(app("amqp://broker.internal:5672")).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn against_the_stand() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("AMQP_TEST_URL") else {
            return Ok(());
        };
        accepts_a_payment(TestApp::start_live(app(&url)).await?).await
    }
    // --8<-- [end:both]
}
