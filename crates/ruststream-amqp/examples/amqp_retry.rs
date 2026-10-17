//! Retries on AMQP 1.0: the mount site caps the deliveries of one message and names the address
//! a spent delivery goes to, and a deferred retry comes back as a copy the runtime publishes.
//!
//! ```text
//! cargo test --example amqp_retry --features testing
//! cargo run --example amqp_retry -- run
//! ```

use std::time::Duration;

use ruststream::runtime::{Outgoing, PublishContext, RETRY_COUNT_HEADER};
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    amount_cents: i64,
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
#[subscriber(AmqpAddress::queue("payments"))]
async fn charge(payment: &Payment) -> HandlerOutcome {
    match charge_card(payment).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(ChargeError::RateLimited) => HandlerOutcome::retry_after(RECHECK),
        Err(ChargeError::Disconnected) => HandlerOutcome::retry(),
    }
}
// --8<-- [end:retry_after]

// --8<-- [start:dead_letter]
/// The dead-letter address is read by an ordinary subscription. The framework's retry-count
/// header says how many times the payment was delivered before it was carried here.
#[subscriber(AmqpAddress::queue("payments.dead"))]
async fn on_dead_letter(payment: &Payment, ctx: &mut Context<'_>) -> HandlerOutcome {
    let deliveries = ctx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("1");
    tracing::error!(payment.id, deliveries, "payment dead-lettered");
    HandlerOutcome::ack()
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
#[ruststream::app]
fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        // Five deliveries of one payment, the first included; the fifth failure goes to
        // "payments.dead" instead. Every copy leaves through `Publish`, stamped by `Retried`.
        b.include(charge)
            .max_attempts(nonzero!(5u32))
            .dead_letter("payments.dead")
            .out_retry(Publish)
            .transform(Retried);
        b.include(on_dead_letter);
    })
}
// --8<-- [end:declaration]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::pin::pin;
    use std::time::Duration;

    use futures::StreamExt;
    use ruststream::runtime::RETRY_COUNT_HEADER;
    use ruststream::testing::{Outcome, TestApp};
    use ruststream::{Broker, ConnectedBroker, IncomingMessage, Subscriber};
    use ruststream_amqp::prelude::*;
    use serde::{Deserialize, Serialize};

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

    /// The broker URL of a live run, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing
    /// one is a failure, so a job that started a broker cannot pass without reaching it.
    fn live_url() -> Option<String> {
        match std::env::var("AMQP_TEST_URL") {
            Ok(url) if !url.is_empty() => Some(url),
            _ => {
                assert!(
                    std::env::var("RUSTSTREAM_REQUIRE_LIVE").map_or(true, |v| v.is_empty()),
                    "RUSTSTREAM_REQUIRE_LIVE is set, but AMQP_TEST_URL is not",
                );
                None
            }
        }
    }

    /// The dead-letter address of `ActiveMQ` Artemis in its default configuration.
    const ARTEMIS_DEAD_LETTERS: &str = "DLQ";

    /// A message of this run alone, so the search through the shared dead-letter queue cannot
    /// match another run's leftovers.
    #[derive(Debug, Deserialize, Serialize, Outgoing)]
    struct Marked {
        run: u32,
    }

    fn deferred() -> String {
        format!("payments.deferred.{}", std::process::id())
    }

    /// Defers the first delivery and acks the copy that comes back.
    #[subscriber(AmqpAddress::queue(deferred()))]
    async fn defer_once(marked: &Marked, ctx: &mut Context<'_>) -> HandlerOutcome {
        let _ = marked.run;
        if ctx.headers().get_str(RETRY_COUNT_HEADER).is_none() {
            HandlerOutcome::retry_after(Duration::from_millis(200))
        } else {
            HandlerOutcome::ack()
        }
    }

    /// A deferred retry rejects the original delivery before the copy goes out, and a broker
    /// that dead-letters rejected deliveries keeps that original: on Artemis it lands in `DLQ`
    /// while the copy comes back to the handler. Only a broker has the policy.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_original_of_a_deferred_retry_reaches_the_brokers_dead_letters() {
        let Some(url) = live_url() else { return };
        // Opened before the retry, settling on receipt: the queue is shared with every other run
        // that rejects, and a reject from here would carry the message straight back to it.
        let watcher = AmqpBroker::new(&url)
            .connect()
            .await
            .expect("the watcher connects");
        let mut dead_letters = watcher
            .subscribe_address(AmqpAddress::queue(ARTEMIS_DEAD_LETTERS).settle(Settle::AtMostOnce))
            .await
            .expect("the dead-letter subscription opens");
        let service = RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(
            AmqpBroker::new(&url),
            |b| {
                b.include(defer_once);
            },
        );
        let tb = TestApp::start_live(service)
            .await
            .expect("the service starts");

        let run = std::process::id();
        tb.broker::<AmqpBroker>()
            .message(&Marked { run })
            .to(deferred().as_str())
            .publish()
            .await
            .expect("the publish is accepted");
        tb.advance(Duration::from_millis(200))
            .await
            .expect("the run settles");
        assert_eq!(
            tb.broker::<AmqpBroker>().subscriber(&deferred()).outcomes(),
            [Outcome::Nack, Outcome::Ack],
            "the copy comes back to the handler",
        );

        let needle = serde_json::to_vec(&Marked { run }).expect("the marker encodes");
        let mut stream = pin!(dead_letters.stream());
        let found = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(delivery) = stream.next().await {
                if delivery.is_ok_and(|message| message.payload() == needle.as_slice()) {
                    return true;
                }
            }
            false
        })
        .await;
        assert_eq!(
            found,
            Ok(true),
            "the rejected original must reach {ARTEMIS_DEAD_LETTERS}"
        );

        tb.shutdown().await.expect("the service shuts down");
        watcher.shutdown().await.expect("the watcher shuts down");
    }
}
