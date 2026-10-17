//! The refunds service of the Retries and dead letters guide: a busy card processor is asked again
//! later, a silent one at once, five deliveries at most, then the dead-letter address.
//!
//! ```text
//! docker compose up -d
//! cargo run --example guide_retries --features testing -- run
//! ```

use std::time::Duration;

use ruststream::runtime::{Outgoing, PublishContext, RETRY_COUNT_HEADER};
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

/// Why the card processor refused a refund.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    /// The processor takes large refunds a few at a time.
    Busy,
    /// The bank behind the card did not answer.
    Unavailable,
    /// The card account is closed; another try gets the same answer.
    Declined,
}

/// The largest refund the processor takes at any moment; a larger one waits for its turn.
const INSTANT_LIMIT_CENTS: u64 = 100_000;

/// Stands in for the card processor's client.
fn send_refund(refund: &Refund) -> Result<(), Refusal> {
    if refund.card.is_empty() {
        return Err(Refusal::Declined);
    }
    if refund.amount_cents > INSTANT_LIMIT_CENTS {
        return Err(Refusal::Busy);
    }
    // Cards of one bank in this example: the bank is down.
    if refund.card.starts_with('9') {
        return Err(Refusal::Unavailable);
    }
    Ok(())
}

// --8<-- [start:refund]
/// How long a busy processor asks its callers to wait.
const BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Refund {
    id: u64,
    card: String,
    amount_cents: u64,
}

#[subscriber(AmqpAddress::queue("refunds"))]
async fn process_refund(refund: &Refund) -> HandlerOutcome {
    match send_refund(refund) {
        Ok(()) => HandlerOutcome::ack(),
        // Past the rate limit: a delivery right now would be refused again.
        Err(Refusal::Busy) => HandlerOutcome::retry_after(BACKOFF),
        // No answer: try again at once, up to the attempt cap.
        Err(Refusal::Unavailable) => HandlerOutcome::retry(),
        Err(Refusal::Declined) => {
            tracing::warn!(refund.id, "card declined, refund dropped");
            HandlerOutcome::drop()
        }
    }
}
// --8<-- [end:refund]

// --8<-- [start:dead_letters]
#[subscriber(AmqpAddress::queue("refunds.dead"))]
async fn review(refund: &Refund, ctx: &mut Context<'_>) -> HandlerOutcome {
    let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
    tracing::error!(refund.id, deliveries, "refund needs a person to look at it");
    HandlerOutcome::ack()
}
// --8<-- [end:dead_letters]

// --8<-- [start:stamp]
/// Marks every copy with the address its delivery came from.
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
// --8<-- [end:stamp]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("refunds", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            // Five deliveries of one refund, the first included; then the dead-letter address.
            b.include(process_refund)
                .max_attempts(nonzero!(5u32))
                .dead_letter("refunds.dead")
                .out_retry(Publish)
                .transform(Retried);
            b.include(review);
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream_amqp::prelude::*;

    use super::{BACKOFF, Refund, app};

    fn refund(id: u64, card: &str, amount_cents: u64) -> Refund {
        Refund {
            id,
            card: card.to_owned(),
            amount_cents,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_processor_is_asked_again_after_the_backoff() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let large = refund(1, "4111", 250_000);

        tb.broker::<AmqpBroker>()
            .message(&large)
            .to("refunds")
            .publish()
            .await?;
        tb.broker::<AmqpBroker>()
            .subscriber("refunds")
            .assert_called_once()
            .settled(HandlerOutcome::retry_after(BACKOFF));

        // The copy waits out the backoff, then comes back stamped.
        tb.advance(BACKOFF).await?;
        tb.broker::<AmqpBroker>()
            .published::<Refund>("refunds")
            .assert_called(2)
            .with(&large)
            .with_header(RETRY_COUNT_HEADER, "1")
            .with_header("x-retried-from", "refunds");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn the_fifth_delivery_goes_to_the_dead_letter_address() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let large = refund(2, "4111", 250_000);

        tb.broker::<AmqpBroker>()
            .message(&large)
            .to("refunds")
            .publish()
            .await?;
        for _ in 0..4 {
            tb.advance(BACKOFF).await?;
        }

        assert_eq!(
            tb.broker::<AmqpBroker>().subscriber("refunds").outcomes(),
            [Outcome::Nack; 5],
        );
        tb.broker::<AmqpBroker>()
            .published::<Refund>("refunds.dead")
            .assert_called_once()
            .with(&large)
            .with_header(RETRY_COUNT_HEADER, "5");
        tb.broker::<AmqpBroker>()
            .subscriber("refunds.dead")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_silent_bank_is_asked_again_at_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let stalled = refund(3, "9012", 1_250);

        tb.broker::<AmqpBroker>()
            .message(&stalled)
            .to("refunds")
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("refunds")
            .assert_called(5);
        tb.broker::<AmqpBroker>()
            .published::<Refund>("refunds.dead")
            .assert_called_once()
            .with(&stalled);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn a_declined_card_is_dropped_and_not_dead_lettered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&refund(4, "", 1_250))
            .to("refunds")
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("refunds")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        tb.broker::<AmqpBroker>()
            .published::<Refund>("refunds.dead")
            .assert_not_called();
        tb.shutdown().await?;
        Ok(())
    }
}
