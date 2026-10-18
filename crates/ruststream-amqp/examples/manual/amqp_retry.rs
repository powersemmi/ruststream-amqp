//! The service of `examples/amqp_retry.rs` written without the macros: each handler implements
//! `Handle`, and the cap, the dead-letter address and the retry publisher are the same steps of
//! the mount chain.
//!
//! ```text
//! cargo test --example manual_amqp_retry --features testing,asyncapi
//! cargo run --example manual_amqp_retry -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::runtime::{Outgoing, PublishContext, RETRY_COUNT_HEADER};
use ruststream::schemars::{self, JsonSchema};
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
struct Payment {
    id: u64,
    amount_cents: i64,
}

// `#[outgoing(name = "payments")]` by hand: the type fixes its destination.
impl OutgoingDestination for Payment {
    type Form = FixedName;

    const DESTINATION: &'static str = "payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

/// Why a charge failed.
#[derive(Debug)]
enum ChargeError {
    /// The gateway asked to be left alone for a while.
    RateLimited,
    /// The connection to the gateway dropped mid-call.
    Disconnected,
}

/// A stand-in for a payment gateway client: it rate-limits amounts ending in 7 and drops the
/// connection on amounts ending in 3, every time.
async fn charge_card(payment: &Payment) -> Result<(), ChargeError> {
    tokio::task::yield_now().await;
    match payment.amount_cents % 10 {
        7 => Err(ChargeError::RateLimited),
        3 => Err(ChargeError::Disconnected),
        _ => Ok(()),
    }
}

/// How long a rate-limited payment waits before it is charged again.
const RECHECK: Duration = Duration::from_secs(30);

// --8<-- [start:retry_after]
/// A rate-limited charge comes back after the delay; a dropped connection is retried at once.
struct Charge;

impl Handle<Payment> for Charge {
    async fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        match charge_card(payment).await {
            Ok(()) => Ok(()),
            Err(ChargeError::RateLimited) => Err(HandlerOutcome::retry_after(RECHECK)),
            Err(ChargeError::Disconnected) => Err(HandlerOutcome::retry()),
        }
    }
}
// --8<-- [end:retry_after]

// --8<-- [start:dead_letter]
/// The dead-letter address is read by an ordinary subscription. The framework's retry-count
/// header says how many times the payment was delivered before it was carried here.
struct OnDeadLetter;

impl Handle<Payment> for OnDeadLetter {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
        tracing::error!(payment.id, deliveries, "payment dead-lettered");
        ready(Ok(()))
    }
}
// --8<-- [end:dead_letter]

// --8<-- [start:transform]
/// Marks every copy with the subscription the delivery came from, so a redelivery is
/// recognisable downstream. A transform on the retry position reads the delivery being retried.
struct Retried;

impl<C, Options> PublishTransform<ForReply<C>, Options> for Retried {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut()
            .insert("x-retried-from", cx.name().to_owned());
    }
}
// --8<-- [end:transform]

// --8<-- [start:declaration]
fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        // Five deliveries of one payment, the first included; the fifth failure goes to
        // "payments.dead" instead. Every copy leaves through `Publish`, stamped by `Retried`.
        b.include(subscriber(AmqpAddress::queue("payments"), Charge).build())
            .max_attempts(nonzero!(5u32))
            .dead_letter("payments.dead")
            .out_retry(Publish)
            .transform(Retried);
        b.include(subscriber(AmqpAddress::queue("payments.dead"), OnDeadLetter).build());
    })
}
// --8<-- [end:declaration]

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_amqp::prelude::*;

    use super::{Payment, RECHECK, app};

    #[tokio::test(start_paused = true)]
    async fn a_rate_limited_payment_comes_back_after_the_delay() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let limited = Payment {
            id: 7,
            amount_cents: 1_007,
        };
        tb.broker::<AmqpBroker>()
            .message(&limited)
            .publish()
            .await?;
        tb.broker::<AmqpBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(RECHECK));

        tb.advance(RECHECK).await?;

        tb.broker::<AmqpBroker>()
            .subscriber("payments")
            .assert_called(2)
            .with(&limited);
        // The test's own publish, then the runtime's copy, counted and stamped.
        tb.broker::<AmqpBroker>()
            .published::<Payment>("payments")
            .assert_called(2)
            .with_header(RETRY_COUNT_HEADER, "1")
            .with_header("x-retried-from", "payments");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn the_fifth_failure_is_dead_lettered_and_read_there() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let disconnected = Payment {
            id: 3,
            amount_cents: 1_003,
        };
        tb.broker::<AmqpBroker>()
            .message(&disconnected)
            .publish()
            .await?;

        // An immediate retry needs no delay: the five deliveries happen at once.
        tb.broker::<AmqpBroker>()
            .subscriber("payments")
            .assert_called(5);
        tb.broker::<AmqpBroker>()
            .published::<Payment>("payments.dead")
            .assert_called_once()
            .with(&disconnected)
            .with_header(RETRY_COUNT_HEADER, "5")
            .with_header("x-retried-from", "payments");
        tb.broker::<AmqpBroker>()
            .subscriber("payments.dead")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_charged_payment_is_acked_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let paid = Payment {
            id: 1,
            amount_cents: 1_000,
        };
        tb.broker::<AmqpBroker>().message(&paid).publish().await?;
        tb.advance(RECHECK).await?;

        assert_eq!(
            tb.broker::<AmqpBroker>().subscriber("payments").outcomes(),
            [Outcome::Ack],
        );
        Ok(())
    }
}
