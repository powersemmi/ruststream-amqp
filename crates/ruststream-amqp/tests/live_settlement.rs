//! Settling deliveries against a real `AMQP` 1.0 broker while other settlements of the same
//! subscription are in flight, gated behind `AMQP_TEST_URL`.
//!
//! A subscription answers every settlement through answer slots it reuses, so these cases settle
//! many deliveries at once, from many tasks, and leave some unsettled: every disposition has to
//! reach its own delivery, and a delivery dropped unsettled has to stay with the broker.
//!
//! Start a broker with `just brokers-up` (`ActiveMQ` Artemis), then:
//! `AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 cargo test --all-features -- --test-threads=1`.

use std::collections::BTreeSet;
use std::error::Error;
use std::pin::pin;
use std::process;
use std::str;
use std::time::Duration;

use futures::future::{join_all, try_join_all};
use futures::{Stream, StreamExt};
use ruststream::testing::TestApp;
use ruststream::{Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Subscriber};
use ruststream_amqp::prelude::*;
use ruststream_amqp::{AmqpMessage, ConnectedAmqpBroker};
use serde::{Deserialize, Serialize};

use crate::live;

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a case waits before calling a further delivery absent.
const QUIET: Duration = Duration::from_millis(800);

/// How many deliveries a case settles at once.
const AT_ONCE: u64 = 64;

fn address(name: &str) -> String {
    format!("it.settlement.{name}.{}", process::id())
}

async fn connect(url: &str, name: &str) -> ConnectedAmqpBroker {
    AmqpBroker::new(url)
        .container_id(format!("settlement-{name}-{}", process::id()))
        .connect()
        .await
        .expect("broker connects")
}

async fn next(
    stream: &mut (impl Stream<Item = Result<AmqpMessage, AmqpError>> + Unpin),
) -> AmqpMessage {
    tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("a delivery arrives")
        .expect("the subscription is open")
        .expect("the delivery is ok")
}

fn number(delivery: &AmqpMessage) -> u64 {
    str::from_utf8(delivery.payload())
        .expect("the payload is text")
        .parse()
        .expect("the payload is a number")
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Job {
    id: u64,
}

fn jobs() -> String {
    address("workers")
}

#[subscriber(AmqpAddress::queue(jobs()), workers(8))]
async fn run(job: &Job) -> HandlerOutcome {
    let _ = job.id;
    HandlerOutcome::ack()
}

fn app(url: &str) -> impl App<State = ()> + use<> {
    RustStream::new(AppInfo::new("settlement", "0.1.0")).with_broker(AmqpBroker::new(url), |b| {
        b.include(run);
    })
}

/// A service with eight workers on one subscription: deliveries published at once are handled
/// and accepted side by side, and each one is settled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_settle_their_deliveries_side_by_side() -> Result<(), Box<dyn Error>> {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return Ok(());
    };
    let tb = TestApp::start_live(app(&url)).await?;
    let broker = tb.broker::<AmqpBroker>();
    let address = jobs();

    let batch: Vec<Job> = (0..AT_ONCE).map(|id| Job { id }).collect();
    try_join_all(
        batch
            .iter()
            .map(|job| broker.message(job).to(address.as_str()).publish()),
    )
    .await?;

    broker
        .subscriber(&address)
        .assert_called(usize::try_from(AT_ONCE)?)
        .settled(HandlerOutcome::ack());
    tb.shutdown().await?;
    Ok(())
}

/// Deliveries settled from as many tasks at once, each with a disposition of its own, while a
/// quarter of them are dropped unsettled. Exactly the requeued ones come back on the link, and
/// exactly the dropped ones come back once the link is gone; nothing accepted or rejected returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_settlements_reach_their_own_deliveries() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    let connected = connect(&url, "concurrent").await;
    let address = address("concurrent");
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(&address).credit(nonzero!(256)))
        .await
        .expect("the subscription opens");
    let publisher = connected.publisher();
    for n in 0..AT_ONCE {
        publisher
            .publish(
                OutgoingMessage::new(&address, n.to_string().as_bytes()),
                None,
            )
            .await
            .expect("publish succeeds");
    }

    let requeued: BTreeSet<u64> = (0..AT_ONCE).filter(|n| n % 4 == 1).collect();
    let dropped: BTreeSet<u64> = (0..AT_ONCE).filter(|n| n % 4 == 3).collect();
    {
        let mut stream = pin!(subscriber.stream());
        let mut kept = Vec::new();
        let mut settlements = Vec::new();
        for _ in 0..AT_ONCE {
            let delivery = next(&mut stream).await;
            let n = number(&delivery);
            match n % 4 {
                3 => kept.push(delivery),
                1 => settlements.push(tokio::spawn(delivery.nack(true))),
                2 => settlements.push(tokio::spawn(delivery.nack(false))),
                _ => settlements.push(tokio::spawn(delivery.ack())),
            }
        }
        for settled in join_all(settlements).await {
            settled
                .expect("the settling task ran")
                .expect("the broker took the disposition");
        }
        drop(kept);

        let mut redelivered = BTreeSet::new();
        for _ in 0..requeued.len() {
            let delivery = next(&mut stream).await;
            redelivered.insert(number(&delivery));
            delivery.ack().await.expect("the redelivery is accepted");
        }
        assert_eq!(
            redelivered, requeued,
            "exactly the requeued deliveries came back"
        );
        assert!(
            tokio::time::timeout(QUIET, stream.next()).await.is_err(),
            "nothing else returns while the dropped deliveries are still held by the link"
        );
    }
    drop(subscriber);

    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("the second subscription opens");
    {
        let mut stream = pin!(subscriber.stream());
        let mut released = BTreeSet::new();
        for _ in 0..dropped.len() {
            let delivery = next(&mut stream).await;
            released.insert(number(&delivery));
            delivery
                .ack()
                .await
                .expect("the released delivery is accepted");
        }
        assert_eq!(
            released, dropped,
            "exactly the deliveries dropped unsettled came back"
        );
        assert!(
            tokio::time::timeout(QUIET, stream.next()).await.is_err(),
            "nothing accepted or rejected came back"
        );
    }
    drop(subscriber);

    connected.shutdown().await.expect("shutdown succeeds");
}
