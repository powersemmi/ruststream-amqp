//! Shared parts of the crate's code-cost benchmarks: what is measured, and how the measurement is
//! kept to one region.
//!
//! # What a scenario looks like
//!
//! One scenario per file, and this module carries what they have in common: the payload, the
//! service setup, the fill, the latch a handler counts deliveries down on, and the measurement
//! configuration. The method is the core's, described in its `benches/common` and on the
//! [RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).
//!
//! A scenario runs the service a user writes: the app, built on [`AmqpBroker::new`] with the
//! stand's URL and started through [`RustStream::start`], on a single-threaded runtime. What comes
//! out is what a message costs on the service's thread: the framework's dispatch, this crate's
//! subscription, message and publisher, and the `fe2o3-amqp` client's framing and decoding, which
//! run on that thread as the tasks the connection spawns. The broker is a separate process and
//! nothing it does is in the number.
//!
//! # Steady state and cold start
//!
//! Every scenario is measured over one delivery, over [`MESSAGES`] deliveries and over twice as
//! many. The slope between the last two is the steady-state cost of a message: everything that
//! happens once is in both totals and cancels in the subtraction. The one-delivery run is the
//! cold start, reported on its own: connecting, attaching the subscription, and taking the first
//! delivery.
//!
//! What a body measures is the start and the drain, in two regions. The queue is filled between
//! them, from a thread of its own with its own runtime and its own connection, and every publish
//! waits for the broker to accept it. The service's runtime only runs inside a measured region,
//! so it takes nothing while the queue fills: the deliveries wait on the broker, up to the link
//! credit in the service's socket, and the drain region takes them all.
//!
//! # What is counted
//!
//! Collection starts switched off and is switched on for [`measure`], which every body wraps its
//! work in. Callgrind switches collection per thread, so what is counted is the service's thread
//! inside the region: the dispatcher, the codec, this crate's code, the client's work on that
//! thread, and tokio's share of driving them. The filling thread runs outside both regions.
//! [`measure`] is the only frame that carries its name, because a toggle on a name that also
//! appears inside closure types switches collection off again one frame deeper. DHAT is pointed
//! at the same frame; the number read is `Total blocks`, allocations per run.
//!
//! The socket is real, so a count is not exact to the digit: how many transfers one read brings
//! in depends on what the broker had written by then.

// Each benchmark target compiles this module on its own and uses the part it needs; what another
// target uses looks unused here.
#![allow(dead_code)]

// The same refusal `paired.rs` makes: a benchmark measures what ships, and the framework's harness
// feature changes the dispatch path.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench-code`"
);

use std::convert::Infallible;
use std::env;
use std::hint::black_box;
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use gungraun::{Callgrind, Dhat, DhatMetric, EntryPoint, EventKind, LibraryBenchmarkConfig};
use ruststream::runtime::{AppInfo, BrokerScope, Identity, RunningApp, RustStream};
use ruststream::{Broker, ConnectedBroker, OutgoingMessage, Publisher};
use ruststream_amqp::AmqpBroker;
use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;

/// The variable naming the broker every scenario runs against; `just bench-code` sets it.
const URL_VARIABLE: &str = "AMQP_TEST_URL";

/// The values every body carries. Fixed, so that every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// How long a drain may take before the run is called stuck. Valgrind slows the service down
/// about fifty times, so this is far above what a run takes.
const DRAIN_LIMIT: Duration = Duration::from_secs(600);

/// How far, in percent, the instructions of a run may rise over the run compared against before
/// it fails.
const INSTRUCTION_LIMIT: f64 = 2.0;

/// The payload every scenario decodes: two integer fields, so a decode allocates nothing and the
/// number is about the crate and the framework rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

/// Deliveries per measured run: large enough that entering and leaving the region is lost in the
/// per-message number, small enough that a scenario stays within seconds of valgrind time.
/// `scripts/bench_results.py` divides by the same count.
pub const MESSAGES: usize = 1_000;

/// The broker every scenario runs against.
///
/// # Panics
///
/// When the variable is not set: the scenarios need the stand `just bench-code` starts.
pub fn url() -> String {
    env::var(URL_VARIABLE).unwrap_or_else(|_| {
        panic!("{URL_VARIABLE} names the broker to measure against; `just bench-code` sets it")
    })
}

/// The address one benchmark process delivers on, fresh for every process.
///
/// Every run is a process of its own under valgrind, so no run sees what the one before it left
/// in a queue. A handler names it in its descriptor, `AmqpAddress::queue(common::input())`, which
/// is evaluated where the handler is mounted.
pub fn input() -> String {
    static ADDRESS: OnceLock<String> = OnceLock::new();
    ADDRESS
        .get_or_init(|| format!("rs.bench.code.{}.{}", process::id(), stamp()))
        .clone()
}

fn stamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default()
}

/// The measurement configuration every gated scenario shares.
///
/// `blocks` is the hard limit on the allocations of the longest run of the scenario (twice
/// [`MESSAGES`] deliveries), and every run of it is held to that limit, so a run fails when the
/// path allocates more than it does today. A scenario states it through [`floor`]. The limit is
/// lowered in the same change that lowers the count. The instruction limit is relative:
/// `just bench-code --save-baseline=main` records a baseline and `just bench-code --baseline=main`
/// compares against it.
pub fn config(blocks: u64) -> LibraryBenchmarkConfig {
    let mut config = LibraryBenchmarkConfig::default();
    config
        .env(URL_VARIABLE, url())
        .tool(callgrind().soft_limits([(EventKind::Ir, INSTRUCTION_LIMIT)]))
        .tool(dhat().hard_limits([(DhatMetric::TotalBlocks, blocks)]));
    config
}

/// The allocation limit for the highest count the longest run was seen at: that count plus a
/// tenth of a percent, rounded up.
///
/// The socket moves the count by a few blocks between runs of an unchanged tree, because how
/// much one read brings in decides how often a buffer grows. The margin keeps that from failing
/// a run. One allocation more per message moves the count by twice [`MESSAGES`], far past it.
pub const fn floor(highest: u64) -> u64 {
    highest + highest.div_ceil(1_000)
}

/// Callgrind collecting inside the measured region alone.
fn callgrind() -> Callgrind {
    let mut callgrind = Callgrind::with_args([
        "--collect-atstart=no",
        &format!("--toggle-collect={REGION}"),
    ]);
    callgrind.entry_point(EntryPoint::None);
    callgrind
}

/// The measured region: everything this runs is counted, nothing around it is.
#[inline(never)]
pub fn measure<T>(body: impl FnOnce() -> T) -> T {
    body()
}

/// DHAT with a stack window deep enough to reach the measured frame from a publish inside a
/// dispatched handler.
fn dhat() -> Dhat {
    let mut dhat = Dhat::with_args(["--num-callers=128"]);
    dhat.entry_point(EntryPoint::Custom(REGION.to_owned()));
    dhat
}

/// The frame both tools are pointed at.
const REGION: &str = "*common::measure*";

/// A single-threaded runtime: the service, the client's connection tasks and the dispatch all run
/// on the one thread the measurement follows.
pub fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

/// Counts deliveries down and wakes the benchmark body when the last one has been handled.
///
/// Handlers reach it as the application state. What a delivery pays for it is one relaxed
/// decrement and the branch that reads it.
#[derive(Clone, Debug)]
pub struct Latch(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    remaining: AtomicUsize,
    drained: Notify,
}

impl Default for Latch {
    fn default() -> Self {
        Self(Arc::new(Inner {
            remaining: AtomicUsize::new(0),
            drained: Notify::new(),
        }))
    }
}

impl Latch {
    /// Arms the latch for `count` deliveries.
    pub fn expect(&self, count: usize) {
        self.0.remaining.store(count, Ordering::Release);
    }

    /// Records one handled delivery, waking the waiter on the last one.
    pub fn arrived(&self) {
        if self.0.remaining.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.drained.notify_one();
        }
    }

    /// How many deliveries the latch is still waiting for.
    pub fn remaining(&self) -> usize {
        self.0.remaining.load(Ordering::Acquire)
    }

    /// Resolves once every expected delivery has been handled.
    pub async fn drained(&self) {
        while self.0.remaining.load(Ordering::Acquire) > 0 {
            self.0.drained.notified().await;
        }
    }
}

/// The JSON body every delivery carries: the two fields a handler reads.
pub fn json_body() -> Vec<u8> {
    format!("{{\"id\":{ID},\"quantity\":{QUANTITY}}}").into_bytes()
}

/// A service that is built but not started, and how many deliveries its queue will hold.
///
/// The start is part of the measurement rather than of the setup, because the cold number is
/// what starting costs. It is held as a boxed call so that every scenario hands over the same
/// type; the one indirect call it adds lands in the cold number and nowhere else.
pub struct Pending {
    runtime: Runtime,
    latch: Latch,
    start: Box<dyn FnOnce(&Runtime) -> RunningApp>,
    messages: usize,
}

/// The mount a scenario passes in: what `with_broker` does with the scope.
pub type Mount<'a> = &'a mut BrokerScope<AmqpBroker, Identity, (), Latch>;

/// Builds a one-handler service on the production broker, ready to be started by the body.
///
/// The constructor records the URL and opens nothing, so building the service is setup and
/// connecting it is the start the body measures.
pub fn pending(messages: usize, mount: impl FnOnce(Mount<'_>)) -> Pending {
    let latch = Latch::default();
    let state = latch.clone();
    let app = RustStream::new(AppInfo::new("bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(AmqpBroker::new(url()), mount);
    Pending {
        runtime: runtime(),
        latch,
        start: Box::new(move |runtime| runtime.block_on(app.start()).expect("the service starts")),
        messages,
    }
}

/// Publishes `count` bodies on [`input`] through this crate's publisher, from a thread and a
/// connection of its own, and returns once the broker has accepted every one of them.
///
/// Part of every setup, never of a measured region: the deliveries are on the broker before the
/// drain starts, so what the drain pays for is delivery, not production.
fn fill(count: usize) {
    thread::scope(|scope| {
        scope
            .spawn(|| {
                let address = input();
                let body = json_body();
                runtime().block_on(async {
                    let connected = AmqpBroker::new(url())
                        .container_id(format!("amqp-bench-code-fill-{}", stamp()))
                        .connect()
                        .await
                        .expect("the filling connection opens");
                    let publisher = connected.publisher();
                    for _ in 0..count {
                        publisher
                            .publish(OutgoingMessage::new(&address, &body), None)
                            .await
                            .expect("the broker accepts the publish");
                    }
                    if let Err(err) = connected.shutdown().await {
                        eprintln!("the filling connection reported a teardown fault: {err}");
                    }
                });
            })
            .join()
            .expect("the filling thread finishes");
    });
}

/// Fails the process if a drain stops moving, rather than leaving a benchmark hanging.
///
/// A thread of its own that sleeps in the kernel until the drain ends, so the measured thread
/// pays nothing for it.
struct Watchdog {
    done: mpsc::Sender<()>,
    thread: thread::JoinHandle<()>,
}

impl Watchdog {
    fn arm(latch: Latch) -> Self {
        let (done, wait) = mpsc::channel();
        let thread = thread::spawn(move || {
            if wait.recv_timeout(DRAIN_LIMIT) == Err(RecvTimeoutError::Timeout) {
                eprintln!(
                    "the drain stopped: {} deliveries still expected after {DRAIN_LIMIT:?}",
                    latch.remaining()
                );
                process::exit(2);
            }
        });
        Self { done, thread }
    }

    fn disarm(self) {
        let _ = self.done.send(());
        self.thread.join().expect("the watchdog thread finishes");
    }
}

/// Starts the service, fills its queue, and drains it: the shape of every scenario here.
///
/// Two measured regions, and the fill between them is in neither. The first is the cold start,
/// the second the deliveries.
pub fn start_and_drain(pending: Pending) {
    let Pending {
        runtime,
        latch,
        start,
        messages,
    } = pending;
    let running = measure(|| start(&runtime));
    latch.expect(messages);
    fill(messages);
    assert_eq!(
        latch.remaining(),
        messages,
        "the queue was consumed while it was being filled, so the measured region would be short"
    );
    let watchdog = Watchdog::arm(latch.clone());
    measure(|| runtime.block_on(latch.drained()));
    watchdog.disarm();
    // The service settles the last delivery after its handler returns, outside the region; the
    // shutdown lets it finish before the connection closes.
    if let Err(err) = runtime.block_on(running.shutdown()) {
        eprintln!("the service reported a teardown fault: {err}");
    }
    black_box(&latch);
}
