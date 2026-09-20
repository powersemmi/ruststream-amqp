// The benchmark is a binary of its own, not library surface: the framework's macros generate the
// handler scaffolding, and a measured loop panics on a broker fault rather than threading a
// `Result` through a scenario nobody recovers from.
#![allow(
    missing_docs,
    unreachable_pub,
    unused_qualifications,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! What this crate costs over the `fe2o3-amqp` client it wraps, and what the runtime costs on top
//! of it.
//!
//! One scenario - a queue consumed with an explicit accept per delivery - run three times over, as
//! three loops that differ in one thing each: what carries the messages.
//!
//! - **raw** drives `fe2o3-amqp` directly.
//! - **adapter** drives this crate's own consumer - the broker type, the subscription source, the
//!   [`Subscriber`] stream it yields, the message handle and its `ack` - from a loop here. No
//!   handler, no app, no dispatch.
//! - **framework** is the whole service a user writes: `#[subscriber]`, the app, the runtime.
//!
//! Adapter against raw is what this crate's own consumer costs over the client it wraps. Framework
//! against adapter is what the runtime costs on top, over this broker in particular.
//!
//! Everything else is the same across the three - the connection, the session layout, the link
//! credit, the delivery guarantee, the position of the settlement, the decode into the same type,
//! the payload bytes, the tokio runtime and the binary. The procedure the numbers follow is the
//! framework's own, published at <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
//!
//! # Why the publish path is not measured here
//!
//! A second scenario answering every delivery on the address the request named was written and
//! thrown away: it measured a TCP timer rather than any code. `fe2o3-amqp` never sets
//! `TCP_NODELAY`, and this crate's publisher waits for the peer's disposition, so a reply written
//! on a connection that is also writing dispositions sits in Nagle's buffer until the peer's
//! delayed acknowledgement releases it. The same exchange takes 50 microseconds on an idle
//! connection (the round-trip probe below) and twenty to forty milliseconds inside that loop, in
//! all three loops alike. A row built on it would publish the delay of a timer as the cost of a
//! crate.
//!
//! # What a run is
//!
//! The consumer is attached first, a client on a second connection then feeds it, and the window
//! runs from the first delivery to the end of the last one's work. Connecting, beginning the
//! sessions and attaching the links are startup cost and sit outside it. Every run owns a fresh
//! address, so a run never sees what the one before it left behind.
//!
//! The message count is not a constant: a probe run measures the raw loop's rate and the count is
//! set from it, so a measured run lasts at least [`SECONDS`] on whatever machine it is taken on.
//!
//! The three loops are interleaved - raw, adapter, framework, raw, adapter, framework - and each
//! reports its best round: noise only ever slows a run down, so the fastest round is the closest
//! to the undisturbed cost. Blocking one loop and then the next would charge every drift of the
//! machine to whichever ran last.
//!
//! # What the numbers do not say
//!
//! The window ends where a delivery's work ends rather than after its settlement, in all three
//! loops alike: the framework accepts a delivery once the handler is done, which is a point the
//! handler itself cannot observe. One disposition out of hundreds of thousands is far below the
//! run-to-run spread.
//!
//! The load is published pre-settled, so the number is about what a delivery costs on the
//! consuming side rather than about how fast a producer can be confirmed.

use std::convert::Infallible;
use std::env;
use std::fmt::Write as _;
use std::hint::black_box;
use std::iter::repeat_n;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fe2o3_amqp::connection::{Connection, ConnectionHandle};
use fe2o3_amqp::link::delivery::Sendable;
use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::session::{Session, SessionHandle};
use fe2o3_amqp::{Receiver, Sender};
use fe2o3_amqp_types::messaging::{Body, Data, Message, Outcome, Source};
use fe2o3_amqp_types::primitives::{Array, Binary, Symbol, Value};
use futures::StreamExt;
use ruststream::{ConnectedBroker, Subscriber, SubscriptionSource};
use ruststream_amqp::prelude::*;
use ruststream_amqp::{AmqpSubscriber, ConnectedAmqpBroker};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};
use url::Url;

// A benchmark measures what ships. With the framework's harness feature compiled in, every
// delivery records what the handler saw and every handler call runs inside a task-local scope, so
// a number taken with it on is not the production path. The benchmark lives in a package of its
// own for the same reason: `ruststream-amqp`'s dev-dependencies enable that feature through the
// conformance harness, and a benchmark inside that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);

/// What the published row is called.
const SCENARIO: &str = "queue, 512 B JSON, accept each";
/// Deliveries the probe run takes to measure the raw loop's rate. It is the warm-up as well.
const PROBE_MESSAGES: usize = 20_000;
/// Round trips one delivery costs the consuming side, for the `broker_bound` arithmetic.
///
/// None: `AMQP` 1.0 pushes transfers against credit the receiver replenishes in the background,
/// and a disposition is written without an answer being waited for. Nothing in this loop asks the
/// broker a question and waits for it.
const ROUND_TRIPS_PER_DELIVERY: f64 = 0.0;

/// How long a measured run lasts, at least.
const SECONDS: f64 = 5.0;
/// How much the calibrated count is raised above the probe's estimate.
///
/// The probe is short and cold, so it reads the machine low; without the margin a run lands just
/// under the floor.
const MARGIN: f64 = 1.25;
/// The ceiling on a calibrated count, so a machine an order faster does not turn a run into an
/// afternoon.
const MAX_MESSAGES: usize = 5_000_000;
/// Rounds run. The best of them is reported.
const ROUNDS: usize = 3;
/// Worker threads every loop is driven on.
const WORKERS: usize = 4;
/// Sends the round-trip probe takes.
const ROUND_TRIPS: usize = 20_000;

/// Link credit granted to every receiver here, which is what the descriptor grants by default.
///
/// All three loops ask the broker for the same flow-control window; a receiver that let the broker
/// run further ahead would be measuring the window rather than the code under it.
const CREDIT: u32 = 256;
/// How far the producer may run ahead of the consumer, in messages.
///
/// Deliveries are published pre-settled, so the protocol's own back-pressure is the link credit
/// the broker grants the producer, and that window is far larger than a queue this benchmark
/// wants to build up. 8192 bodies is 4 MiB outstanding: enough that the consumer never waits for
/// a message, small enough that the broker is never paging.
const IN_FLIGHT: usize = 8_192;
/// How often the producer checks that ceiling.
const CHECK_EVERY: usize = 512;
/// How long a run may go without a delivery before it is called stuck.
const STALL: Duration = Duration::from_secs(30);

/// The body size every loop publishes and decodes, to the byte: the scenario is published under
/// this number, so the bytes on the wire have to be it.
const BODY_BYTES: usize = 512;
/// How wide one padding value is before the next field starts.
const PAD_WIDTH: usize = 16;
/// The values every body carries. Fixed, so every delivery of a run costs the same.
const ID: u64 = 1_000_000;
const QUANTITY: u32 = 37;

/// What every loop decodes a delivery into.
///
/// Two integer fields the loop reads, and a padding the type ignores: a decode that allocates
/// nothing, so the number is about this crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// A JSON body carrying the two fields, padded with fields [`Order`] ignores until it is exactly
/// `size` bytes.
///
/// The padding is a run of equally wide fields and one last field cut to whatever is left, so a
/// scenario published as a 512 byte body is one. Building it is startup work, and the assertion
/// below holds the promise the published name makes.
fn json_body(size: usize) -> Vec<u8> {
    let mut body = format!("{{\"id\":{ID},\"quantity\":{QUANTITY}");
    let mut field = 0u32;
    loop {
        let key = format!(",\"f{field}\":\"\"");
        // One byte stays reserved for the closing brace.
        let Some(room) = size.checked_sub(body.len() + key.len() + 1) else {
            break;
        };
        // A full-width field only when what it leaves behind can still hold the next one, whose
        // key is at most one digit longer. Otherwise this is the last field and it takes the
        // rest, because a remainder too small to start a field would come out as a short body.
        let width = if room > PAD_WIDTH + key.len() {
            PAD_WIDTH
        } else {
            room
        };
        body.push_str(&key[..key.len() - 1]);
        body.extend(repeat_n('x', width));
        body.push('"');
        field += 1;
    }
    body.push('}');
    assert_eq!(
        body.len(),
        size,
        "a body has to be the size the scenario publishes"
    );
    body.into_bytes()
}

/// The address one run owns: nothing is shared with the run before it.
fn fresh_address() -> String {
    format!("rs.bench.{}", stamp())
}

/// The address the service being built subscribes to.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, which is
/// inside the builder of the run that is starting. A run installs its own address here first, so
/// the subscription the framework opens is the one this run publishes to.
static ADDRESS: Mutex<Option<String>> = Mutex::new(None);

fn install(address: &str) {
    *ADDRESS
        .lock()
        .expect("the address cell is never held across a panic") = Some(address.to_owned());
}

fn installed() -> String {
    ADDRESS
        .lock()
        .expect("the address cell is never held across a panic")
        .clone()
        .expect("a run installs its address before it builds the service")
}

/// Counts deliveries and marks the ends of the measured window.
///
/// All three loops call the same methods, so all three pay for the signal. A delivery pays one
/// relaxed increment and two comparisons; the waiter is a single future for the whole run, woken
/// once.
#[derive(Clone, Debug)]
struct Run(Arc<RunInner>);

#[derive(Debug)]
struct RunInner {
    total: usize,
    seen: AtomicUsize,
    first: OnceLock<Instant>,
    last: OnceLock<Instant>,
    drained: Notify,
}

impl Run {
    fn new(total: usize) -> Self {
        Self(Arc::new(RunInner {
            total,
            seen: AtomicUsize::new(0),
            first: OnceLock::new(),
            last: OnceLock::new(),
            drained: Notify::new(),
        }))
    }

    /// Records one handled delivery, and answers whether the run is over.
    fn arrived(&self) -> bool {
        let seen = self.0.seen.fetch_add(1, Ordering::Relaxed) + 1;
        if seen == 1 {
            let _ = self.0.first.set(Instant::now());
        }
        if seen == self.0.total {
            let _ = self.0.last.set(Instant::now());
            self.0.drained.notify_one();
        }
        seen >= self.0.total
    }

    fn handled(&self) -> usize {
        self.0.seen.load(Ordering::Acquire).min(self.0.total)
    }

    /// Resolves once every expected delivery has been handled.
    async fn drained(&self) {
        while self.0.seen.load(Ordering::Acquire) < self.0.total {
            self.0.drained.notified().await;
        }
    }

    /// The measured window: the first delivery to the end of the last one's work.
    fn window(&self) -> Duration {
        let first = *self.0.first.get().expect("the run took a delivery");
        let last = *self.0.last.get().expect("the run took its last delivery");
        last - first
    }
}

/// Waits for the run to finish, and fails with what it was waiting for if it stops moving.
async fn drain(run: &Run, loop_name: &str) {
    let mut seen = 0;
    loop {
        if timeout(STALL, run.drained()).await.is_ok() {
            return;
        }
        let handled = run.handled();
        assert!(
            handled > seen,
            "{loop_name}: {handled} of {} deliveries handled and nothing moved for {STALL:?}",
            run.0.total
        );
        seen = handled;
    }
}

/// What one loop of one round produced.
#[derive(Clone, Copy, Debug)]
struct Sample {
    window: Duration,
    /// How long the producer's own loop took. Printed next to the window, because a run whose two
    /// figures agree is a run the producer paced.
    published: Duration,
    /// How often the producer had to wait for the consumer.
    throttled: usize,
}

impl Sample {
    fn rate(self, messages: usize) -> f64 {
        messages as f64 / self.window.as_secs_f64()
    }

    fn published_rate(self, messages: usize) -> f64 {
        messages as f64 / self.published.as_secs_f64()
    }
}

// ---------------------------------------------------------------------------------------------
// The client, spelled out the way the crate configures it
// ---------------------------------------------------------------------------------------------

/// One raw connection with the session its senders share, which is the layout the crate opens:
/// links that send ride one session, and every subscription gets a session of its own so a slow
/// consumer's flow window is nobody else's.
struct Client {
    connection: ConnectionHandle<()>,
    session: SessionHandle<()>,
    /// One per receiver attached here. A session handle ends its session when it is dropped, so
    /// the client holds them for as long as the links on them are in use.
    subscriptions: Vec<SessionHandle<()>>,
    container: String,
    links: usize,
}

impl Client {
    /// Opens a connection the way this crate opens one, down to the socket.
    ///
    /// The client's own `open` leaves Nagle's algorithm on, and a consumer that settles every
    /// delivery writes exactly what that algorithm holds back. The crate sets `TCP_NODELAY`, so a
    /// raw loop that did not would be a different transport rather than the same one without the
    /// crate, and the row would report a socket option as this crate's cost.
    async fn open(url: &str, role: &str) -> Self {
        let container = format!("amqp-bench-{role}-{}", stamp());
        let parsed = Url::parse(url).expect("the benchmark URL parses");
        let addresses = parsed
            .socket_addrs(|| Some(5672))
            .expect("the URL names a reachable address");
        let stream = TcpStream::connect(&*addresses)
            .await
            .expect("the AMQP peer accepts a socket");
        stream
            .set_nodelay(true)
            .expect("the socket takes TCP_NODELAY");
        let mut builder = Connection::builder()
            .container_id(container.clone())
            .scheme(parsed.scheme());
        if let Some(hostname) = parsed.host_str() {
            builder = builder.hostname(hostname).sasl_hostname(hostname);
        }
        if let Some(domain) = parsed.domain() {
            builder = builder.domain(domain);
        }
        if let Ok(profile) = SaslProfile::try_from(&parsed) {
            builder = builder.sasl_profile(profile);
        }
        let mut connection = builder
            .open_with_stream(stream)
            .await
            .expect("the AMQP peer accepts a connection");
        let session = Session::begin(&mut connection)
            .await
            .expect("the peer accepts a session");
        Self {
            connection,
            session,
            subscriptions: Vec::new(),
            container,
            links: 0,
        }
    }

    /// A process-unique link name, as the crate mints one.
    fn link_name(&mut self, role: &str) -> String {
        self.links += 1;
        format!("{}-{role}-{}", self.container, self.links)
    }

    /// A receiver on its own session, with the credit and the settlement the descriptor asks for.
    async fn receiver(&mut self, address: &str) -> Receiver {
        let name = self.link_name("receiver");
        let mut session = Session::begin(&mut self.connection)
            .await
            .expect("the peer accepts a session");
        let receiver = Receiver::builder()
            .name(name)
            .source(queue_source(address))
            .auto_accept(false)
            .credit_mode(CreditMode::Auto(CREDIT))
            .attach(&mut session)
            .await
            .expect("the peer accepts the receiver link");
        self.subscriptions.push(session);
        receiver
    }

    async fn sender(&mut self, address: &str) -> Sender {
        let name = self.link_name("sender");
        Sender::attach(&mut self.session, name, address)
            .await
            .expect("the peer accepts the sender link")
    }

    async fn close(mut self) {
        for mut session in self.subscriptions {
            let _ = session.end().await;
        }
        let _ = self.session.end().await;
        let _ = self.connection.close().await;
    }
}

fn stamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default()
}

/// The source the crate builds for a queue address: the node, and the terminus capability that
/// says consumers on it compete.
fn queue_source(address: &str) -> Source {
    let mut source = Source::builder().address(address).build();
    source.capabilities = Some(Array::from(vec![Symbol::from("queue")]));
    source
}

/// The bytes of a delivered body.
///
/// Every message this benchmark publishes carries one data section, which is what the crate's own
/// conversion produces; anything else is a body it never sent.
fn payload(body: Body<Value>) -> Vec<u8> {
    match body {
        Body::Data(batch) => {
            let mut chunks = batch.into_iter();
            match (chunks.next(), chunks.next()) {
                (Some(Data(first)), None) => first.into_vec(),
                _ => panic!("a delivery carried a body this benchmark never published"),
            }
        }
        _ => panic!("a delivery carried a body this benchmark never published"),
    }
}

/// Publishes the run's bodies, never letting the consumer fall further behind than [`IN_FLIGHT`].
async fn publish_all(sender: &mut Sender, messages: usize, run: &Run) -> (Duration, usize) {
    let body = json_body(BODY_BYTES);
    let message = Sendable {
        message: Message::builder().data(Binary::from(body)).build(),
        message_format: 0,
        // Pre-settled: the producer is the same in all three loops, and waiting for a disposition
        // per message would measure how fast this broker confirms a send instead of how fast the
        // consumer under test drains what it is handed.
        settled: Some(true),
    };
    let started = Instant::now();
    let mut throttled = 0;
    for sent in 0..messages {
        if sent % CHECK_EVERY == 0 {
            let waiting_since = Instant::now();
            while sent.saturating_sub(run.handled()) > IN_FLIGHT {
                assert!(
                    waiting_since.elapsed() < STALL,
                    "the consumer stopped taking deliveries: {} of {messages} handled",
                    run.handled()
                );
                throttled += 1;
                sleep(Duration::from_micros(200)).await;
            }
        }
        sender
            .send_batchable_ref(&message)
            .await
            .expect("the peer accepts the transfer");
    }
    (started.elapsed(), throttled)
}

/// Feeds a run from a client of its own, which is the same for every loop.
async fn feed(url: &str, address: &str, messages: usize, run: &Run) -> (Duration, usize) {
    let mut producer = Client::open(url, "producer").await;
    let mut sender = producer.sender(address).await;
    let published = publish_all(&mut sender, messages, run).await;
    let _ = sender.close().await;
    producer.close().await;
    published
}

/// The transport's round trip, measured outside every loop: a send whose answer the client waits
/// for, over one connection, and the median of what it took.
///
/// This is the figure the `broker_bound` flag is decided with. An unsettled send is the cheapest
/// request an `AMQP` 1.0 peer answers.
async fn round_trip(url: &str, samples: usize) -> Duration {
    let mut client = Client::open(url, "probe").await;
    let address = format!("rs.bench.probe.{}", stamp());
    let mut sender = client.sender(&address).await;
    let message = Sendable {
        message: Message::builder()
            .data(Binary::from(b"ping".to_vec()))
            .build(),
        message_format: 0,
        settled: Some(false),
    };

    let mut times = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        let outcome = sender
            .send_ref(&message)
            .await
            .expect("the peer takes the probe");
        times.push(started.elapsed());
        assert!(
            matches!(outcome, Outcome::Accepted(_)),
            "the probe was not accepted: {outcome:?}"
        );
    }

    let _ = sender.close().await;
    client.close().await;
    times.sort_unstable();
    times[times.len() / 2]
}

// ---------------------------------------------------------------------------------------------
// The three loops
// ---------------------------------------------------------------------------------------------

/// Raw: receive, decode, read a field, accept.
async fn consume_loop_raw(mut receiver: Receiver, run: Run) -> Receiver {
    loop {
        let delivery = receiver
            .recv::<Body<Value>>()
            .await
            .expect("the peer delivers");
        let (info, message) = delivery.into_parts();
        let order: Order =
            serde_json::from_slice(&payload(message.body)).expect("the body decodes");
        black_box((order.id, order.quantity));
        // The framework settles a delivery once the handler is done, so the window closes before
        // the disposition in every loop.
        let done = run.arrived();
        receiver
            .accept(info)
            .await
            .expect("the disposition reaches the peer");
        if done {
            return receiver;
        }
    }
}

/// Opens the crate's connected form and its subscription, the way a service's startup does.
async fn subscribe(url: &str, address: &str) -> (ConnectedAmqpBroker, AmqpSubscriber) {
    let connected = AmqpBroker::new(url)
        .container_id(format!("amqp-bench-adapter-{}", stamp()))
        .connect()
        .await
        .expect("the broker connects");
    let subscriber = AmqpAddress::queue(address)
        .subscribe(&connected)
        .await
        .expect("the subscription opens");
    (connected, subscriber)
}

/// Adapter: pull from this crate's subscription, decode, read a field, settle.
async fn consume_loop_adapter(subscriber: &mut AmqpSubscriber, run: &Run) {
    let mut stream = pin!(subscriber.stream());
    loop {
        let message = stream
            .next()
            .await
            .expect("the subscription stays open")
            .expect("the delivery arrives");
        let order: Order = serde_json::from_slice(message.payload()).expect("the body decodes");
        black_box((order.id, order.quantity));
        let done = run.arrived();
        message.ack().await.expect("the accept reaches the peer");
        if done {
            return;
        }
    }
}

/// Framework: the handler a user writes.
#[subscriber(AmqpAddress::queue(installed()))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

async fn start_consume(url: &str, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("amqp-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(AmqpBroker::new(url), |b| {
            b.include(consume);
        })
        .start()
        .await
        .expect("the service starts")
}

/// Stops the service, printing a teardown fault instead of failing the run.
///
/// The measured window closed with the last delivery's work, so nothing that happens here can
/// reach a number. What can happen is that the connection's teardown races the subscription's own:
/// the crate's pump task detaches its receiver and ends its session after the connected form has
/// already closed the connection, and the broker answers a frame arriving after a close by
/// dropping the socket.
async fn report_app_shutdown(app: RunningApp) {
    if let Err(err) = app.shutdown().await {
        eprintln!("the service reported a teardown fault: {err}");
    }
}

/// The same, for the connected form the adapter loop owns.
async fn report_broker_shutdown(connected: ConnectedAmqpBroker) {
    if let Err(err) = connected.shutdown().await {
        eprintln!("the connected broker reported a teardown fault: {err}");
    }
}

// ---------------------------------------------------------------------------------------------
// The three runs
// ---------------------------------------------------------------------------------------------

async fn raw_run(url: &str, address: &str, messages: usize) -> Sample {
    let mut consumer = Client::open(url, "raw-consumer").await;
    let receiver = consumer.receiver(address).await;

    let run = Run::new(messages);
    let consuming = tokio::spawn(consume_loop_raw(receiver, run.clone()));

    let (published, throttled) = feed(url, address, messages, &run).await;
    drain(&run, "raw").await;

    let receiver = consuming.await.expect("the consuming task ends");
    let _ = receiver.detach().await;
    consumer.close().await;
    Sample {
        window: run.window(),
        published,
        throttled,
    }
}

async fn adapter_run(url: &str, address: &str, messages: usize) -> Sample {
    let (connected, mut subscriber) = subscribe(url, address).await;

    let run = Run::new(messages);
    let consuming = {
        let run = run.clone();
        tokio::spawn(async move {
            consume_loop_adapter(&mut subscriber, &run).await;
            subscriber
        })
    };

    let (published, throttled) = feed(url, address, messages, &run).await;
    drain(&run, "adapter").await;

    drop(consuming.await.expect("the consuming task ends"));
    report_broker_shutdown(connected).await;
    Sample {
        window: run.window(),
        published,
        throttled,
    }
}

async fn framework_run(url: &str, address: &str, messages: usize) -> Sample {
    let run = Run::new(messages);
    install(address);
    let app = start_consume(url, run.clone()).await;

    let (published, throttled) = feed(url, address, messages, &run).await;
    drain(&run, "framework").await;

    report_app_shutdown(app).await;
    Sample {
        window: run.window(),
        published,
        throttled,
    }
}

/// Which of the three loops a sample came from.
#[derive(Clone, Copy, Debug)]
enum Carrier {
    Raw,
    Adapter,
    Framework,
}

impl Carrier {
    async fn run(self, url: &str, address: &str, messages: usize) -> Sample {
        match self {
            Self::Raw => raw_run(url, address, messages).await,
            Self::Adapter => adapter_run(url, address, messages).await,
            Self::Framework => framework_run(url, address, messages).await,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------

/// Best and worst of the rounds.
///
/// Noise on the machine only ever slows a run down, so the fastest round is the closest to the
/// undisturbed cost, and the slowest says how far from quiet the machine was.
#[derive(Clone, Copy, Debug)]
struct Stats {
    best: f64,
    worst: f64,
}

impl Stats {
    fn of(rates: &[f64]) -> Self {
        assert!(!rates.is_empty(), "no round was run");
        Self {
            best: rates.iter().copied().fold(f64::MIN, f64::max),
            worst: rates.iter().copied().fold(f64::MAX, f64::min),
        }
    }

    fn spread(self) -> f64 {
        self.best - self.worst
    }
}

#[derive(Debug)]
struct Measured {
    messages: usize,
    rounds: usize,
    raw: Stats,
    adapter: Stats,
    framework: Stats,
    overhead_percent: f64,
    adapter_overhead_percent: f64,
    verdict: &'static str,
    broker_bound: bool,
}

async fn measure(url: &str, rounds: usize, seconds: f64, round_trip: Duration) -> Measured {
    // The probe is the warm-up as well: its result is thrown away, and the rate it measured sets
    // a count that makes every run below last at least `seconds`.
    let probe = Carrier::Raw
        .run(url, &fresh_address(), PROBE_MESSAGES)
        .await;
    let messages = ((probe.rate(PROBE_MESSAGES) * seconds * MARGIN) as usize)
        .clamp(PROBE_MESSAGES, MAX_MESSAGES);
    println!(
        "{SCENARIO}: {messages} messages per run ({:.0} msg/s probed, producer {:.0} msg/s, {} \
         waits)",
        probe.rate(PROBE_MESSAGES),
        probe.published_rate(PROBE_MESSAGES),
        probe.throttled
    );

    let mut raws = Vec::with_capacity(rounds);
    let mut adapters = Vec::with_capacity(rounds);
    let mut frameworks = Vec::with_capacity(rounds);
    for round in 1..=rounds {
        let raw = Carrier::Raw.run(url, &fresh_address(), messages).await;
        let adapter = Carrier::Adapter.run(url, &fresh_address(), messages).await;
        let framework = Carrier::Framework
            .run(url, &fresh_address(), messages)
            .await;
        println!(
            "  round {round:>2}: raw {:>9.0}, adapter {:>9.0}, framework {:>9.0} msg/s \
             (producer {:.0}/{:.0}/{:.0}, waits {}/{}/{})",
            raw.rate(messages),
            adapter.rate(messages),
            framework.rate(messages),
            raw.published_rate(messages),
            adapter.published_rate(messages),
            framework.published_rate(messages),
            raw.throttled,
            adapter.throttled,
            framework.throttled
        );
        raws.push(raw.rate(messages));
        adapters.push(adapter.rate(messages));
        frameworks.push(framework.rate(messages));
    }

    // Whether every round came out the same way round. The rounds are interleaved for exactly
    // this reason: a difference that holds in each of them is a result, however wide the spread
    // of either half is on its own.
    let consistent = frameworks.iter().zip(&raws).all(|(fast, slow)| fast > slow)
        || frameworks.iter().zip(&raws).all(|(fast, slow)| fast < slow);

    let raw = Stats::of(&raws);
    let adapter = Stats::of(&adapters);
    let framework = Stats::of(&frameworks);
    let difference = (raw.best - framework.best).abs();
    // The flag is arithmetic on measurements, not an inference: what one delivery spends waiting
    // for the transport to answer, against what one delivery costs the raw client altogether.
    let per_message = 1.0 / raw.best;
    let waiting = round_trip.as_secs_f64() * ROUND_TRIPS_PER_DELIVERY;
    Measured {
        messages,
        rounds,
        raw,
        adapter,
        framework,
        overhead_percent: (raw.best - framework.best) / raw.best * 100.0,
        adapter_overhead_percent: (raw.best - adapter.best) / raw.best * 100.0,
        // The methodology's honesty rule: a difference below the run-to-run noise is a verdict,
        // never a percentage. The sign check is what keeps the rule from denying a result it was
        // never meant to deny - a framework that is faster in every single round is noisier than
        // the raw loop by construction, and its own spread would otherwise swallow a difference
        // every round agrees on.
        verdict: if difference < raw.spread().max(framework.spread()) && !consistent {
            "indistinguishable"
        } else {
            "measured"
        },
        broker_bound: waiting >= per_message / 2.0,
    }
}

fn document(row: &Measured, round_trip: Duration, round_trips: usize) -> String {
    let mut out = format!(
        "{{\n  \"round_trip_micros\": {},\n  \"round_trip_samples\": {round_trips},\n  \
         \"scenarios\": [\n",
        round_trip.as_micros()
    );
    write!(
        out,
        concat!(
            "    {{\n",
            "      \"name\": \"{name}\",\n",
            "      \"unit\": \"msg/s\",\n",
            "      \"messages\": {messages},\n",
            "      \"pairs\": {rounds},\n",
            "      \"raw\": {{ \"best\": {raw_best:.0}, \"worst\": {raw_worst:.0} }},\n",
            "      \"adapter\": {{ \"best\": {ad_best:.0}, \"worst\": {ad_worst:.0} }},\n",
            "      \"framework\": {{ \"best\": {fw_best:.0}, \"worst\": {fw_worst:.0} }},\n",
            "      \"overhead_percent\": {overhead:.1},\n",
            "      \"adapter_overhead_percent\": {adapter_overhead:.1},\n",
            "      \"verdict\": \"{verdict}\",\n",
            "      \"broker_bound\": {broker_bound}\n",
            "    }}\n",
        ),
        name = SCENARIO,
        messages = row.messages,
        rounds = row.rounds,
        raw_best = row.raw.best,
        raw_worst = row.raw.worst,
        ad_best = row.adapter.best,
        ad_worst = row.adapter.worst,
        fw_best = row.framework.best,
        fw_worst = row.framework.worst,
        overhead = row.overhead_percent,
        adapter_overhead = row.adapter_overhead_percent,
        verdict = row.verdict,
        broker_bound = row.broker_bound,
    )
    .expect("writing to a String");
    out.push_str("  ]\n}\n");
    out
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("the tokio runtime builds")
}

/// A positive count from the environment, or the default.
///
/// The parse target rejects zero, so a round count of zero is refused here rather than after the
/// probe run, where it would panic in the statistics with no round to report.
fn number(name: &str, fallback: usize) -> usize {
    env::var(name).ok().map_or(fallback, |value| {
        value
            .parse::<NonZeroUsize>()
            .unwrap_or_else(|_| panic!("{name} must be a positive number"))
            .get()
    })
}

fn main() {
    let url = env::var("AMQP_TEST_URL")
        .expect("AMQP_TEST_URL names the broker to measure against; `just bench` sets it");
    let rounds = number("RUSTSTREAM_BENCH_ROUNDS", ROUNDS);
    let seconds = number("RUSTSTREAM_BENCH_SECONDS", SECONDS as usize) as f64;
    let round_trips = number("RUSTSTREAM_BENCH_ROUND_TRIPS", ROUND_TRIPS);
    let out = env::var("RUSTSTREAM_BENCH_OUT").unwrap_or_else(|_| "bench-paired.json".to_owned());

    let runtime = runtime();
    let round_trip = runtime.block_on(round_trip(&url, round_trips));
    println!(
        "round trip: {:.0} us (median of {round_trips} sends waiting for their disposition)",
        round_trip.as_secs_f64() * 1e6
    );

    let row = runtime.block_on(measure(&url, rounds, seconds, round_trip));
    println!(
        "\n{SCENARIO}: raw {:.0}, adapter {:.0} ({:+.1}%), framework {:.0} ({:+.1}%) msg/s ({}{})",
        row.raw.best,
        row.adapter.best,
        -row.adapter_overhead_percent,
        row.framework.best,
        -row.overhead_percent,
        row.verdict,
        if row.broker_bound {
            ", broker-bound"
        } else {
            ""
        }
    );

    std::fs::write(&out, document(&row, round_trip, round_trips)).expect("the summary is written");
    println!("\nwrote {out}");
}
