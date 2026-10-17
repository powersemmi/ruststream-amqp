//! The service the Testing guide tests, written without the macros.
//!
//! ```text
//! cargo test --example manual_guide_testing --features testing
//! AMQP_TEST_URL=amqp://artemis:artemis@localhost:5672 \
//!     cargo test --example manual_guide_testing --features testing -- --test-threads=1
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;
use std::time::Duration;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../../../tests/live/mod.rs"]
mod live;

// --8<-- [start:service]
/// The largest payment the bank settles at once; a larger one is held.
const INSTANT_LIMIT_CENTS: u64 = 100_000;

/// How long a held payment waits before the service asks the bank again.
const HOLD: Duration = Duration::from_secs(60);

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Payment {
    id: u64,
    card: String,
    amount_cents: u64,
}

impl OutgoingDestination for Payment {
    type Form = FixedName;

    const DESTINATION: &'static str = "card-payments";
}

impl MessageHeaders for Payment {
    type Contract = NoHeaders;
}

impl MessageInfo for Payment {
    const NAME: &'static str = "Payment";
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Receipt {
    payment: u64,
    amount_cents: u64,
}

impl OutgoingDestination for Receipt {
    type Form = FixedName;

    const DESTINATION: &'static str = "card-receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

impl MessageInfo for Receipt {
    const NAME: &'static str = "Receipt";
}

struct Charge;

impl Handle<Payment, Receipt> for Charge {
    fn handle(
        &self,
        payment: &Payment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        if payment.card.is_empty() {
            tracing::warn!(payment.id, "payment without a card dropped");
            return ready(Err(HandlerOutcome::drop()));
        }
        if payment.amount_cents > INSTANT_LIMIT_CENTS {
            tracing::info!(payment.id, "payment held by the bank");
            return ready(Err(HandlerOutcome::retry_after(HOLD)));
        }
        tracing::info!(payment.id, "payment charged");
        ready(Ok(Receipt {
            payment: payment.id,
            amount_cents: payment.amount_cents,
        }))
    }
}

fn service(broker: AmqpBroker) -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(
            subscriber(AmqpAddress::queue("card-payments"), Charge)
                .reply()
                .build(),
        )
        .max_attempts(nonzero!(3u32))
        .dead_letter("card-payments.review")
        .out_reply(Publish);
    })
}

fn app() -> impl App {
    service(AmqpBroker::new("amqp://artemis:artemis@localhost:5672"))
}

fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:service]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::process;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_amqp::prelude::*;

    use super::{HOLD, Payment, Receipt, live, service};

    const URL: &str = "amqp://artemis:artemis@localhost:5672";

    fn payment(id: u64, amount_cents: u64) -> Payment {
        Payment {
            id,
            card: "4111".to_owned(),
            amount_cents,
        }
    }

    // --8<-- [start:in_process]
    #[tokio::test]
    async fn a_payment_gets_a_receipt() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(AmqpBroker::new(URL))).await?;
        a_charged_payment_gets_its_receipt(tb, 1).await
    }

    async fn a_charged_payment_gets_its_receipt(
        tb: TestApp<()>,
        id: u64,
    ) -> Result<(), Box<dyn Error>> {
        tb.broker::<AmqpBroker>()
            .message(&payment(id, 2_500))
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<AmqpBroker>()
            .published::<Receipt>("card-receipts")
            .assert_called_once()
            .with(&Receipt {
                payment: id,
                amount_cents: 2_500,
            });
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:in_process]

    // --8<-- [start:cardless]
    #[tokio::test]
    async fn a_payment_without_a_card_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(AmqpBroker::new(URL))).await?;

        tb.broker::<AmqpBroker>()
            .message(&Payment {
                id: 2,
                card: String::new(),
                amount_cents: 2_500,
            })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<AmqpBroker>()
            .published::<Receipt>("card-receipts")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:cardless]

    // --8<-- [start:held]
    #[tokio::test(start_paused = true)]
    async fn a_payment_the_bank_keeps_holding_goes_to_review() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(AmqpBroker::new(URL))).await?;
        let held = payment(3, 250_000);

        tb.broker::<AmqpBroker>().message(&held).publish().await?;
        tb.broker::<AmqpBroker>()
            .subscriber("card-payments")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(HOLD));

        // The delay is a copy of the payment, published back to its queue after `HOLD`.
        tb.advance(HOLD).await?;
        tb.broker::<AmqpBroker>()
            .published::<Payment>("card-payments")
            .assert_called(2)
            .with(&held)
            .with_header(RETRY_COUNT_HEADER, "1");

        // The third delivery is the last one: it goes to the review address as it arrived.
        tb.advance(HOLD).await?;
        assert_eq!(
            tb.broker::<AmqpBroker>()
                .subscriber("card-payments")
                .outcomes(),
            [Outcome::Nack, Outcome::Nack, Outcome::Nack],
        );
        tb.broker::<AmqpBroker>()
            .published::<Payment>("card-payments.review")
            .assert_called_once()
            .with(&held);
        tb.shutdown().await?;
        Ok(())
    }
    // --8<-- [end:held]

    // --8<-- [start:live]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_payment_gets_a_receipt_from_a_broker() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("AMQP_TEST_URL") else {
            return Ok(());
        };
        let tb = TestApp::start_live(service(AmqpBroker::new(url))).await?;
        a_charged_payment_gets_its_receipt(tb, u64::from(process::id())).await
    }
    // --8<-- [end:live]
}
