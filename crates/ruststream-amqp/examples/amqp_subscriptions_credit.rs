//! Link credit: how many deliveries the broker may send a subscription ahead of its settlements.
//!
//! ```text
//! cargo test --example amqp_subscriptions_credit --features testing
//! AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 \
//!     cargo test --example amqp_subscriptions_credit --features testing
//! cargo run --example amqp_subscriptions_credit -- run
//! ```

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
}

// --8<-- [start:credit]
/// Rendering an invoice is slow, so the broker sends 16 ahead instead of the default 256 and
/// the rest stay on the queue for the other replicas.
#[subscriber(AmqpAddress::queue("invoices").credit(nonzero!(16)))]
async fn render(invoice: &Invoice) -> HandlerOutcome {
    tracing::info!(invoice.id, "invoice rendered");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(render);
    })
}
// --8<-- [end:credit]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::pin::pin;
    use std::time::Duration;

    use futures::{Stream, StreamExt};
    use ruststream::testing::TestApp;
    use ruststream::{
        Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
    };
    use ruststream_amqp::AmqpMessage;
    use ruststream_amqp::prelude::*;

    use super::{Invoice, app};

    #[tokio::test]
    async fn the_credit_leaves_the_handler_as_it_is() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        for id in 1..=3 {
            tb.broker::<AmqpBroker>()
                .message(&Invoice { id })
                .publish()
                .await?;
        }

        let ids: Vec<u64> = tb
            .broker::<AmqpBroker>()
            .subscriber("invoices")
            .assert_called(3)
            .received::<Invoice>()
            .into_iter()
            .map(|invoice| invoice.id)
            .collect();
        assert_eq!(ids, [1, 2, 3]);
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

    /// Long enough for whatever the broker was willing to send to arrive over loopback.
    const QUIET: Duration = Duration::from_millis(500);

    /// The next delivery, or a failure naming what was waited for.
    async fn next<S>(stream: &mut S, what: &str) -> AmqpMessage
    where
        S: Stream<Item = Result<AmqpMessage, AmqpError>> + Unpin,
    {
        tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .unwrap_or_else(|_| panic!("{what} must arrive"))
            .unwrap_or_else(|| panic!("{what}: the stream closed"))
            .unwrap_or_else(|err| panic!("{what}: {err}"))
    }

    /// Asserts that the broker sends nothing more within [`QUIET`].
    async fn quiet<S>(stream: &mut S, what: &str)
    where
        S: Stream<Item = Result<AmqpMessage, AmqpError>> + Unpin,
    {
        if let Ok(extra) = tokio::time::timeout(QUIET, stream.next()).await {
            let payload = extra.map(|item| item.map(|message| message.payload().to_vec()));
            panic!("{what}, got {payload:?}");
        }
    }

    /// The credit is a send window the client tops up: the broker sends `credit` deliveries
    /// ahead, and once half of them is settled the client grants the full credit again, counted
    /// from the deliveries already sent. So the deliveries held unsettled can outnumber the
    /// credit. The subject is the link itself, which only a broker has.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_credit_is_granted_again_once_half_of_it_is_settled() {
        let Some(url) = live_url() else { return };
        let connected = AmqpBroker::new(&url)
            .container_id(format!("credit-example-{}", std::process::id()))
            .connect()
            .await
            .expect("the broker connects");
        let address = format!("credit.window.{}", std::process::id());
        let mut subscriber = connected
            .subscribe_address(AmqpAddress::queue(&address).credit(nonzero!(4)))
            .await
            .expect("the subscription opens");

        let publisher = connected.publisher();
        for id in 0..12_u8 {
            publisher
                .publish(
                    OutgoingMessage::new(&address, format!("{id}").as_bytes()),
                    None,
                )
                .await
                .expect("the publish is accepted");
        }

        let mut stream = pin!(subscriber.stream());
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(next(&mut stream, "a delivery within the credit").await);
        }
        quiet(
            &mut stream,
            "four unsettled deliveries spend a credit of four",
        )
        .await;

        // One settlement is under half of the credit: the window stays where it is.
        held.remove(0).ack().await.expect("the ack is accepted");
        quiet(&mut stream, "one settlement does not top the credit up").await;

        // The second one reaches half, and the client grants four more beyond the fourth
        // delivery: six deliveries are now held unsettled on a credit of four.
        held.remove(0).ack().await.expect("the ack is accepted");
        for _ in 0..4 {
            held.push(next(&mut stream, "a delivery the new grant allows").await);
        }
        assert_eq!(held.len(), 6);
        quiet(&mut stream, "the new grant is four deliveries wide").await;

        for message in held {
            message.ack().await.expect("the ack is accepted");
        }
        connected.shutdown().await.expect("the shutdown is clean");
    }
}
