//! Owned transactions under a pool of workers, in process and against a real `AMQP` 1.0 broker.
//!
//! The workers of one subscription share its publisher, so each worker opens a transaction of its
//! own on it. The handler holds its transaction open until the other worker holds one too, which
//! is the case a single transaction per publisher cannot serve.
//!
//! The live case runs against a broker started with `just brokers-up` (`ActiveMQ` Artemis):
//! `AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 cargo test --all-features -- --test-threads=1`.

#![cfg(all(feature = "testing", feature = "transaction"))]

use std::convert::Infallible;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use futures::{Stream, StreamExt};
use ruststream::testing::TestApp;
use ruststream::{Broker, IncomingMessage, Subscriber};
use ruststream_amqp::AmqpMessage;
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::Barrier;

use crate::live;

/// How many workers the subscription runs, and so how many transactions are open at once.
const WORKERS: usize = 2;

/// The longest the batches may take together. A worker that could not open its transaction would
/// leave the other one waiting at the barrier, and this deadline turns that into a failure.
const DEADLINE: Duration = Duration::from_secs(10);

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the live case waits before calling a line from the dropped transaction absent.
const QUIET: Duration = Duration::from_millis(500);

/// The addresses are per process, so a stand kept running between runs hands this one nothing
/// left over from another.
fn batches() -> String {
    format!("owned.batches.{}", std::process::id())
}

fn abandoned() -> String {
    format!("owned.abandoned.{}", std::process::id())
}

fn ledger() -> String {
    format!("owned.ledger.{}", std::process::id())
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Outgoing)]
struct Batch {
    id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Outgoing)]
struct Entry {
    batch: u64,
    line: u64,
}

/// Posts two ledger lines per batch in a transaction of its own, and commits only once every
/// worker holds an open transaction.
#[subscriber(AmqpAddress::queue(batches()), workers(2))]
async fn post_batch(
    batch: &Batch,
    ctx: &mut Context<'_, (), Arc<Barrier>>,
    Out(out): Out<impl OwnedTransactions>,
) -> HandlerOutcome {
    let Ok(mut txn) = out.owned_transaction().await else {
        return HandlerOutcome::drop();
    };
    for line in 0..2 {
        let entry = Entry {
            batch: batch.id,
            line,
        };
        if txn.publish(&ledger(), &entry).await.is_err() {
            let _ = txn.abort().await;
            return HandlerOutcome::drop();
        }
    }
    ctx.state().wait().await;
    if txn.commit().await.is_err() {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

/// Posts a ledger line in a transaction and lets the transaction go without settling it, which
/// rolls it back.
#[subscriber(AmqpAddress::queue(abandoned()))]
async fn abandon(batch: &Batch, Out(out): Out<impl OwnedTransactions>) -> HandlerOutcome {
    let Ok(mut txn) = out.owned_transaction().await else {
        return HandlerOutcome::drop();
    };
    let entry = Entry {
        batch: batch.id,
        line: 0,
    };
    if txn.publish(&ledger(), &entry).await.is_err() {
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

fn app(url: &str) -> impl App<State = Arc<Barrier>> {
    RustStream::new(AppInfo::new("ledger", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Barrier::new(WORKERS))))
        .with_broker(AmqpBroker::new(url), |b| {
            b.include(post_batch)
                .out(DefaultSlot, TransactionalPublish)
                .build();
            b.include(abandon)
                .out(DefaultSlot, TransactionalPublish)
                .build();
        })
}

/// Injects one batch per worker at once and waits for both to settle.
async fn post_batches_together(tb: &TestApp<Arc<Barrier>>) {
    let posted: Vec<Batch> = (1..=WORKERS as u64).map(|id| Batch { id }).collect();
    let published = tokio::time::timeout(
        DEADLINE,
        join_all(posted.iter().map(|batch| {
            tb.broker::<AmqpBroker>()
                .message(batch)
                .to(batches())
                .publish()
        })),
    )
    .await
    .expect("each worker must commit a transaction of its own while the other one is open");
    for result in published {
        result.expect("publish failed");
    }
    tb.broker::<AmqpBroker>()
        .subscriber(&batches())
        .assert_called(WORKERS)
        .settled(HandlerOutcome::ack());
}

/// Injects a batch whose transaction the handler drops unsettled.
async fn abandon_a_batch(tb: &TestApp<Arc<Barrier>>) {
    tb.broker::<AmqpBroker>()
        .message(&Batch { id: 0 })
        .to(abandoned())
        .publish()
        .await
        .expect("publish failed");
    tb.broker::<AmqpBroker>()
        .subscriber(&abandoned())
        .assert_called_once()
        .settled(HandlerOutcome::ack());
}

/// What the two commits must have published: every line of both batches.
fn expected_ledger() -> Vec<Entry> {
    (1..=WORKERS as u64)
        .flat_map(|batch| (0..2).map(move |line| Entry { batch, line }))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_worker_commits_a_transaction_of_its_own() {
    let tb = TestApp::start(app("amqp://broker.example.com:5672"))
        .await
        .expect("startup failed");

    abandon_a_batch(&tb).await;
    post_batches_together(&tb).await;

    let mut ledger: Vec<Entry> = tb
        .broker::<AmqpBroker>()
        .published::<Entry>(&ledger())
        .decoded();
    ledger.sort();
    assert_eq!(
        ledger,
        expected_ledger(),
        "both workers' transactions must commit whole, and the dropped one must publish nothing",
    );

    tb.shutdown().await.expect("shutdown failed");
}

/// The next ledger line on the watching subscription, decoded and accepted.
async fn next_entry<S>(stream: &mut S) -> Entry
where
    S: Stream<Item = Result<AmqpMessage, AmqpError>> + Unpin,
{
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("a committed ledger line must arrive")
        .expect("the ledger stream closed")
        .expect("the ledger stream failed");
    let entry = serde_json::from_slice(message.payload()).expect("a ledger line decodes");
    message.ack().await.expect("ack succeeds");
    entry
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_worker_commits_a_transaction_of_its_own_live() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    // The ledger is read from a connection of its own, opened before the service starts so the
    // node the commits are posted to is the queue it reads.
    let watcher = AmqpBroker::new(url.clone())
        .container_id(format!("owned-watch-{}", std::process::id()))
        .connect()
        .await
        .expect("broker connects");
    let mut lines = watcher
        .subscribe_address(AmqpAddress::queue(ledger()))
        .await
        .expect("the ledger subscription opens");

    let tb = TestApp::start_live(app(&url))
        .await
        .expect("startup failed");
    abandon_a_batch(&tb).await;
    post_batches_together(&tb).await;
    tb.shutdown().await.expect("shutdown failed");

    let mut stream = pin!(lines.stream());
    let mut ledger = Vec::new();
    for _ in 0..expected_ledger().len() {
        ledger.push(next_entry(&mut stream).await);
    }
    ledger.sort();
    assert_eq!(
        ledger,
        expected_ledger(),
        "both workers' transactions must commit whole on the server",
    );
    if let Ok(extra) = tokio::time::timeout(QUIET, stream.next()).await {
        let payload = extra.map(|item| item.map(|message| message.payload().to_vec()));
        panic!("the dropped transaction must publish nothing, got {payload:?}");
    }
}
