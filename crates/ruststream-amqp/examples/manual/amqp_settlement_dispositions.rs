//! The service of `examples/amqp_settlement_dispositions.rs` written without the macros: `Ok`
//! accepts the delivery, and the error side carries the outcome that settles it otherwise.
//!
//! ```text
//! cargo test --example manual_amqp_settlement_dispositions --features testing,asyncapi
//! cargo run --example manual_amqp_settlement_dispositions -- run
//! ```

use std::collections::HashSet;
use std::convert::Infallible;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use ruststream::runtime::cli::run_main;
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

/// A stand-in for a payment gateway client: it times out on the first charge of every payment
/// and succeeds on the next one.
#[derive(Clone, Default)]
struct PaymentGateway {
    attempted: Arc<Mutex<HashSet<u64>>>,
}

impl PaymentGateway {
    async fn charge(&self, payment: &Payment) -> Result<(), &'static str> {
        tokio::task::yield_now().await;
        let first = self
            .attempted
            .lock()
            .expect("the gateway's bookkeeping is never poisoned")
            .insert(payment.id);
        if first {
            Err("gateway timed out")
        } else {
            Ok(())
        }
    }
}

/// What the handlers share, built once at startup and read through `Context::state`.
struct AppState {
    gateway: PaymentGateway,
}

// --8<-- [start:dispositions]
/// A charged payment is accepted, a timed-out one goes back to the broker for another try, and a
/// malformed one is rejected.
struct Charge;

impl Handle<Payment, (), (), (), AppState> for Charge {
    async fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> Result<(), HandlerOutcome> {
        if payment.amount_cents <= 0 {
            // No retry can fix it: the broker's dead-letter policy decides where it goes.
            return Err(HandlerOutcome::drop());
        }
        if let Err(err) = ctx.state().gateway.charge(payment).await {
            tracing::warn!(payment.id, err, "charge failed, retrying");
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:dispositions]

fn app() -> impl App<State = AppState> {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("payments", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                gateway: PaymentGateway::default(),
            })
        })
        .with_broker(broker, |b| {
            b.include(subscriber(AmqpAddress::queue("payments"), Charge).build());
        })
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_amqp::prelude::*;

    use super::{Payment, app};

    #[tokio::test]
    async fn a_timed_out_charge_is_delivered_again_and_accepted() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let payment = Payment {
            id: 1,
            amount_cents: 1_000,
        };
        tb.broker::<AmqpBroker>()
            .message(&payment)
            .publish()
            .await?;

        assert_eq!(
            tb.broker::<AmqpBroker>()
                .subscriber("payments")
                .assert_called(2)
                .with(&payment)
                .outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_malformed_payment_is_rejected_and_not_delivered_again() -> Result<(), Box<dyn Error>>
    {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&Payment {
                id: 2,
                amount_cents: 0,
            })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        Ok(())
    }
}
