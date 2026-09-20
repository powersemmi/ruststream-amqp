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
//! Every scenario is run three times over, as three loops that differ in one thing each: what
//! carries the messages.
//!
//! - **raw** drives `fe2o3-amqp` directly.
//! - **adapter** drives this crate's own consumer and publisher - the broker type, the
//!   subscription source, the [`Subscriber`] stream it yields, the message handle and its `ack`,
//!   the publisher - from a loop here. No handler, no app, no dispatch.
//! - **framework** is the whole service a user writes: `#[subscriber]`, the app, the runtime.
//!
//! Adapter against raw is what this crate's consumer and publisher cost over the client they wrap.
//! Framework against adapter is what the runtime costs on top, over this broker in particular.
//!
//! Everything else is the same across the three - the connection, the session layout, the link
//! credit, the delivery guarantee, the position of the settlement, the decode into the same type,
//! the payload bytes, the tokio runtime and the binary. The procedure the numbers follow is the
//! framework's own, published at <https://powersemmi.github.io/ruststream/latest/benchmarks/>.
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
//! The three loops are interleaved - raw, adapter, framework, raw, adapter, framework - and the
//! first round is discarded. Blocking one loop and then the next would charge every drift of the
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

use std::collections::HashMap;
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
use fe2o3_amqp::session::{Session, SessionHandle};
use fe2o3_amqp::{Receiver, Sender};
use fe2o3_amqp_types::messaging::{Body, Data, Message, MessageId, Outcome, Properties, Source};
use fe2o3_amqp_types::primitives::{Array, Binary, Symbol, Value};
use futures::StreamExt;
use ruststream::{ConnectedBroker, OutgoingMessage, Subscriber, SubscriptionSource};
use ruststream_amqp::prelude::*;
use ruststream_amqp::{AmqpPublisher, AmqpSubscriber, ConnectedAmqpBroker};
use serde::{Deserialize, Serialize};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use tokio::time::{sleep, timeout};

// A benchmark measures what ships. With the framework's harness feature compiled in, every
// delivery records what the handler saw and every handler call runs inside a task-local scope, so
// a number taken with it on is not the production path. The benchmark lives in a package of its
// own for the same reason: `ruststream-amqp`'s dev-dependencies enable that feature through the
// conformance harness, and a benchmark inside that package would link it.
#[cfg(feature = "testing")]
compile_error!(
    "benchmarks must be built without the `testing` feature; run them through `just bench`"
);

/// How long a measured run lasts, at least.
const SECONDS: f64 = 5.0;
/// How much the calibrated count is raised above the probe's estimate.
///
/// The probe is short and cold, so it reads the machine low; without the margin the faster
/// scenario lands just under the floor.
const MARGIN: f64 = 1.25;
/// The ceiling on a calibrated count, so a machine an order faster does not turn a run into an
/// afternoon.
const MAX_MESSAGES: usize = 5_000_000;
/// Rounds kept. One more is run and discarded.
const ROUNDS: usize = 11;
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
/// The correlation identifier every request carries. Fixed for the same reason as the body.
const CORRELATION_ID: &str = "rs-bench-correlation";

/// What every loop decodes a delivery into.
///
/// Two integer fields the loop reads, and a padding the type ignores: a decode that allocates
/// nothing, so the number is about this crate rather than about `serde_json`'s string handling.
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

/// What a responder sends back.
///
/// The framework loop encodes it with the default codec and the other two with `serde_json`,
/// which is the same encoder reached two ways, so the reply is the same bytes in all three.
#[derive(Debug, Serialize, Outgoing)]
struct Receipt {
    id: u64,
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

/// The addresses one run owns: nothing is shared with the run before it.
#[derive(Clone, Debug)]
struct Addresses {
    /// Where the load is published and the consumer subscribes.
    queue: String,
    /// Where a responder sends its replies, named in every request's `reply-to`.
    reply: String,
}

impl Addresses {
    fn fresh() -> Self {
        let stamp = stamp();
        Self {
            queue: format!("rs.bench.{stamp}"),
            reply: format!("rs.bench.reply.{stamp}"),
        }
    }
}

/// The addresses the service being built subscribes to.
///
/// `#[subscriber(..)]` takes an expression and evaluates it where the handler is mounted, which is
/// inside the builder of the run that is starting. A run installs its own addresses here first, so
/// the subscription the framework opens is the one this run publishes to.
static ADDRESSES: Mutex<Option<Addresses>> = Mutex::new(None);

fn install(addresses: &Addresses) {
    *ADDRESSES
        .lock()
        .expect("the address cell is never held across a panic") = Some(addresses.clone());
}

fn installed() -> Addresses {
    ADDRESSES
        .lock()
        .expect("the address cell is never held across a panic")
        .clone()
        .expect("a run installs its addresses before it builds the service")
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
    async fn open(url: &str, role: &str) -> Self {
        let container = format!("amqp-bench-{role}-{}", stamp());
        let mut connection = Connection::builder()
            .container_id(container.clone())
            .open(url)
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

/// The load one run publishes: the same bytes, pre-settled, with the request properties the
/// scenario asks for.
fn sendable(body: &[u8], reply_to: Option<&str>) -> Sendable<Data> {
    let mut builder = Message::builder();
    if let Some(reply_to) = reply_to {
        builder = builder.properties(Properties {
            reply_to: Some(reply_to.to_owned()),
            correlation_id: Some(MessageId::String(CORRELATION_ID.to_owned())),
            ..Properties::default()
        });
    }
    Sendable {
        message: builder.data(Binary::from(body.to_vec())).build(),
        message_format: 0,
        // Pre-settled: the producer is the same in all three loops, and waiting for a disposition
        // per message would measure how fast this broker confirms a send instead of how fast the
        // consumer under test drains what it is handed.
        settled: Some(true),
    }
}

/// Publishes the run's bodies, never letting the consumer fall further behind than [`IN_FLIGHT`].
async fn publish_all(
    sender: &mut Sender,
    messages: usize,
    reply_to: Option<&str>,
    run: &Run,
) -> (Duration, usize) {
    let body = json_body(BODY_BYTES);
    let message = sendable(&body, reply_to);
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

/// The transport's round trip, measured outside every loop: a send whose answer the client waits
/// for, over one connection, and the median of what it took.
///
/// This is the figure the `broker_bound` flag is decided with. An unsettled send is the cheapest
/// request an `AMQP` 1.0 peer answers, and it is the same exchange a reply publish pays for, so
/// the arithmetic under the request/reply row is about the round trip that row actually makes.
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
// The raw loop: the client, driven directly
// ---------------------------------------------------------------------------------------------

/// Receive, decode, read a field, accept.
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

/// Receive, decode, answer the address the request named, accept.
///
/// The links are keyed by address and looked up per delivery, which is what this crate's publisher
/// does with the sender links the connection owns: a responder answers wherever a request points
/// it, and one attached link per address is the shape every loop ends up with.
async fn respond_loop_raw(
    mut receiver: Receiver,
    mut senders: HashMap<String, Sender>,
    run: Run,
) -> (Receiver, HashMap<String, Sender>) {
    loop {
        let delivery = receiver
            .recv::<Body<Value>>()
            .await
            .expect("the peer delivers");
        let (info, message) = delivery.into_parts();
        let properties = message
            .properties
            .expect("every request carries its properties");
        let order: Order =
            serde_json::from_slice(&payload(message.body)).expect("the body decodes");
        black_box((order.id, order.quantity));

        let reply_to = properties
            .reply_to
            .expect("every request names a reply address");
        let receipt = serde_json::to_vec(&Receipt { id: order.id }).expect("the reply encodes");
        let reply = Message::builder()
            .properties(Properties {
                correlation_id: properties.correlation_id,
                ..Properties::default()
            })
            .data(Binary::from(receipt))
            .build();
        let sender = senders
            .get_mut(&reply_to)
            .expect("the reply address is one this run attached a link to");
        // This crate's publisher waits for the peer's disposition and reports anything but
        // `accepted` as an error, so this loop waits for it too.
        let outcome = sender.send(reply).await.expect("the peer takes the reply");
        assert!(
            matches!(outcome, Outcome::Accepted(_)),
            "the reply was not accepted: {outcome:?}"
        );

        let done = run.arrived();
        receiver
            .accept(info)
            .await
            .expect("the disposition reaches the peer");
        if done {
            return (receiver, senders);
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The adapter loop: this crate's own consumer and publisher, hand-driven
// ---------------------------------------------------------------------------------------------

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

/// Pull, decode, read a field, settle.
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

/// Pull, decode, answer the address the request named, settle.
async fn respond_loop_adapter(
    subscriber: &mut AmqpSubscriber,
    publisher: &AmqpPublisher,
    run: &Run,
) {
    let mut stream = pin!(subscriber.stream());
    loop {
        let message = stream
            .next()
            .await
            .expect("the subscription stays open")
            .expect("the delivery arrives");
        let order: Order = serde_json::from_slice(message.payload()).expect("the body decodes");
        black_box((order.id, order.quantity));

        let headers = message.headers();
        let reply_to = headers
            .reply_to()
            .expect("every request names a reply address");
        let mut reply_headers = HeaderMap::new();
        if let Some(correlation_id) = headers.correlation_id() {
            reply_headers.insert("correlation-id", correlation_id.to_owned());
        }
        let receipt = serde_json::to_vec(&Receipt { id: order.id }).expect("the reply encodes");
        publisher
            .publish(
                OutgoingMessage::new(reply_to, &receipt).with_headers(reply_headers),
                None,
            )
            .await
            .expect("the reply is accepted");

        let done = run.arrived();
        message.ack().await.expect("the accept reaches the peer");
        if done {
            return;
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The framework loop: the service a user writes
// ---------------------------------------------------------------------------------------------

#[subscriber(AmqpAddress::queue(installed().queue))]
async fn consume(order: &Order, ctx: &mut Context<'_, (), Run>) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    ctx.state().arrived();
    HandlerOutcome::ack()
}

#[subscriber(AmqpAddress::queue(installed().queue))]
async fn respond(
    order: &Order,
    ctx: &mut Context<'_, (), Run>,
    Out(out): Out<impl Publisher>,
) -> HandlerOutcome {
    black_box((order.id, order.quantity));
    let headers = ctx.headers();
    let reply_to = headers
        .reply_to()
        .expect("every request names a reply address");
    let mut reply_headers = HeaderMap::new();
    if let Some(correlation_id) = headers.correlation_id() {
        reply_headers.insert("correlation-id", correlation_id.to_owned());
    }
    out.message(&Receipt { id: order.id })
        .to(reply_to)
        .with_headers(reply_headers)
        .publish()
        .await
        .expect("the reply is accepted");
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

async fn start_respond(url: &str, run: Run) -> RunningApp {
    RustStream::new(AppInfo::new("amqp-bench", "0.0.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(run))
        .with_broker(AmqpBroker::new(url), |b| {
            b.include(respond).out(DefaultSlot, Publish).build();
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
// The six runs
// ---------------------------------------------------------------------------------------------

/// Feeds a run from a client of its own, which is the same for every loop.
async fn feed(
    url: &str,
    address: &str,
    messages: usize,
    reply_to: Option<&str>,
    run: &Run,
) -> (Duration, usize) {
    let mut producer = Client::open(url, "producer").await;
    let mut sender = producer.sender(address).await;
    let published = publish_all(&mut sender, messages, reply_to, run).await;
    let _ = sender.close().await;
    producer.close().await;
    published
}

async fn raw_queue(url: &str, addresses: &Addresses, messages: usize) -> Sample {
    let mut consumer = Client::open(url, "raw-consumer").await;
    let receiver = consumer.receiver(&addresses.queue).await;

    let run = Run::new(messages);
    let consuming = tokio::spawn(consume_loop_raw(receiver, run.clone()));

    let (published, throttled) = feed(url, &addresses.queue, messages, None, &run).await;
    drain(&run, "raw queue").await;

    let receiver = consuming.await.expect("the consuming task ends");
    let _ = receiver.detach().await;
    consumer.close().await;
    Sample {
        window: run.window(),
        published,
        throttled,
    }
}

async fn adapter_queue(url: &str, addresses: &Addresses, messages: usize) -> Sample {
    let (connected, mut subscriber) = subscribe(url, &addresses.queue).await;

    let run = Run::new(messages);
    let consuming = {
        let run = run.clone();
        tokio::spawn(async move {
            consume_loop_adapter(&mut subscriber, &run).await;
            subscriber
        })
    };

    let (published, throttled) = feed(url, &addresses.queue, messages, None, &run).await;
    drain(&run, "adapter queue").await;

    drop(consuming.await.expect("the consuming task ends"));
    report_broker_shutdown(connected).await;
    Sample {
        window: run.window(),
        published,
        throttled,
    }
}

async fn framework_queue(url: &str, addresses: &Addresses, messages: usize) -> Sample {
    let run = Run::new(messages);
    install(addresses);
    let app = start_consume(url, run.clone()).await;

    let (published, throttled) = feed(url, &addresses.queue, messages, None, &run).await;
    drain(&run, "framework queue").await;

    report_app_shutdown(app).await;
    Sample {
        window: run.window(),
        published,
        throttled,
    }
}

/// The requester every responding loop answers: it attaches the reply link first, so the address a
/// request names exists before any reply is sent, and drains the replies as they come back.
async fn request_all(
    url: &str,
    addresses: &Addresses,
    messages: usize,
    run: &Run,
) -> (Duration, usize) {
    let mut requester = Client::open(url, "requester").await;
    let mut replies = requester.receiver(&addresses.reply).await;
    let mut sender = requester.sender(&addresses.queue).await;

    let draining = tokio::spawn(async move {
        for answered in 0..messages {
            // A deadline rather than a bare await: a responder that died would otherwise leave
            // this task waiting for a reply nobody is going to send, holding the machine.
            let delivery = timeout(STALL, replies.recv::<Body<Value>>())
                .await
                .unwrap_or_else(|_| panic!("no reply for {STALL:?} after {answered} of {messages}"))
                .expect("the reply arrives");
            let (info, _) = delivery.into_parts();
            replies
                .accept(info)
                .await
                .expect("the disposition reaches the peer");
        }
        replies
    });

    let published = publish_all(&mut sender, messages, Some(&addresses.reply), run).await;
    let replies = draining.await.expect("the draining task ends");
    let _ = replies.detach().await;
    let _ = sender.close().await;
    requester.close().await;
    published
}

async fn raw_request_reply(url: &str, addresses: &Addresses, messages: usize) -> Sample {
    let mut responder = Client::open(url, "raw-responder").await;
    let receiver = responder.receiver(&addresses.queue).await;
    let senders = HashMap::from([(
        addresses.reply.clone(),
        responder.sender(&addresses.reply).await,
    )]);

    let run = Run::new(messages);
    let responding = tokio::spawn(respond_loop_raw(receiver, senders, run.clone()));

    let (published, throttled) = request_all(url, addresses, messages, &run).await;
    drain(&run, "raw request/reply").await;

    let (receiver, senders) = responding.await.expect("the responding task ends");
    let _ = receiver.detach().await;
    for (_, sender) in senders {
        let _ = sender.close().await;
    }
    responder.close().await;
    Sample {
        window: run.window(),
        published,
        throttled,
    }
}

async fn adapter_request_reply(url: &str, addresses: &Addresses, messages: usize) -> Sample {
    let (connected, mut subscriber) = subscribe(url, &addresses.queue).await;

    let run = Run::new(messages);
    let responding = {
        let run = run.clone();
        let out = connected.publisher();
        tokio::spawn(async move {
            respond_loop_adapter(&mut subscriber, &out, &run).await;
            subscriber
        })
    };

    let (published, throttled) = request_all(url, addresses, messages, &run).await;
    drain(&run, "adapter request/reply").await;

    drop(responding.await.expect("the responding task ends"));
    report_broker_shutdown(connected).await;
    Sample {
        window: run.window(),
        published,
        throttled,
    }
}

async fn framework_request_reply(url: &str, addresses: &Addresses, messages: usize) -> Sample {
    let run = Run::new(messages);
    install(addresses);
    let app = start_respond(url, run.clone()).await;

    let (published, throttled) = request_all(url, addresses, messages, &run).await;
    drain(&run, "framework request/reply").await;

    report_app_shutdown(app).await;
    Sample {
        window: run.window(),
        published,
        throttled,
    }
}

// ---------------------------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Queue,
    RequestReply,
}

/// Which of the three loops a sample came from.
#[derive(Clone, Copy, Debug)]
enum Carrier {
    Raw,
    Adapter,
    Framework,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Self::Queue => "queue, 512 B JSON, accept each",
            Self::RequestReply => "request/reply, 512 B JSON request, JSON reply",
        }
    }

    /// Deliveries the probe run takes to measure the raw loop's rate.
    ///
    /// The probe is the warm-up as well, and a responder that answers every request is an order
    /// slower than a consumer that only accepts one, so the two scenarios cannot share a count
    /// without one of them spending a minute on it.
    const fn probe(self) -> usize {
        match self {
            Self::Queue => 20_000,
            Self::RequestReply => 2_000,
        }
    }

    /// Round trips one delivery costs the consuming side, for the `broker_bound` arithmetic.
    ///
    /// A plain delivery costs none: `AMQP` 1.0 pushes transfers against credit the receiver
    /// replenishes in the background, and a disposition is written without an answer being waited
    /// for. Answering a request costs exactly one, because the reply is sent unsettled and the
    /// publisher waits for the peer's disposition before the loop takes the next delivery.
    const fn round_trips_per_delivery(self) -> f64 {
        match self {
            Self::Queue => 0.0,
            Self::RequestReply => 1.0,
        }
    }

    async fn run(
        self,
        carrier: Carrier,
        url: &str,
        addresses: &Addresses,
        messages: usize,
    ) -> Sample {
        match (self, carrier) {
            (Self::Queue, Carrier::Raw) => raw_queue(url, addresses, messages).await,
            (Self::Queue, Carrier::Adapter) => adapter_queue(url, addresses, messages).await,
            (Self::Queue, Carrier::Framework) => framework_queue(url, addresses, messages).await,
            (Self::RequestReply, Carrier::Raw) => raw_request_reply(url, addresses, messages).await,
            (Self::RequestReply, Carrier::Adapter) => {
                adapter_request_reply(url, addresses, messages).await
            }
            (Self::RequestReply, Carrier::Framework) => {
                framework_request_reply(url, addresses, messages).await
            }
        }
    }
}

/// Median, smallest and largest of the kept rounds.
#[derive(Clone, Copy, Debug)]
struct Stats {
    median: f64,
    min: f64,
    max: f64,
}

impl Stats {
    fn of(mut rates: Vec<f64>) -> Self {
        rates.sort_by(f64::total_cmp);
        let middle = rates.len() / 2;
        let median = if rates.len().is_multiple_of(2) {
            f64::midpoint(rates[middle - 1], rates[middle])
        } else {
            rates[middle]
        };
        Self {
            median,
            min: rates[0],
            max: rates[rates.len() - 1],
        }
    }

    fn spread(self) -> f64 {
        self.max - self.min
    }
}

#[derive(Debug)]
struct Measured {
    scenario: Scenario,
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

async fn measure(
    scenario: Scenario,
    url: &str,
    rounds: usize,
    seconds: f64,
    round_trip: Duration,
) -> Measured {
    // The probe is the warm-up as well: its result is thrown away, and the rate it measured sets
    // a count that makes every run below last at least `seconds`.
    let probe_messages = scenario.probe();
    let probe = scenario
        .run(Carrier::Raw, url, &Addresses::fresh(), probe_messages)
        .await;
    let messages = ((probe.rate(probe_messages) * seconds * MARGIN) as usize)
        .clamp(probe_messages, MAX_MESSAGES);
    println!(
        "{}: {messages} messages per run ({:.0} msg/s probed, producer {:.0} msg/s, {} waits)",
        scenario.name(),
        probe.rate(probe_messages),
        probe.published_rate(probe_messages),
        probe.throttled
    );

    let mut raws = Vec::with_capacity(rounds);
    let mut adapters = Vec::with_capacity(rounds);
    let mut frameworks = Vec::with_capacity(rounds);
    for round in 0..=rounds {
        let raw = scenario
            .run(Carrier::Raw, url, &Addresses::fresh(), messages)
            .await;
        let adapter = scenario
            .run(Carrier::Adapter, url, &Addresses::fresh(), messages)
            .await;
        let framework = scenario
            .run(Carrier::Framework, url, &Addresses::fresh(), messages)
            .await;
        if round == 0 {
            continue;
        }
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

    let raw = Stats::of(raws);
    let adapter = Stats::of(adapters);
    let framework = Stats::of(frameworks);
    let difference = (raw.median - framework.median).abs();
    // The flag is arithmetic on measurements, not an inference: what one delivery spends waiting
    // for the transport to answer, against what one delivery costs the raw client altogether.
    let per_message = 1.0 / raw.median;
    let waiting = round_trip.as_secs_f64() * scenario.round_trips_per_delivery();
    Measured {
        scenario,
        messages,
        rounds,
        raw,
        adapter,
        framework,
        overhead_percent: (raw.median - framework.median) / raw.median * 100.0,
        adapter_overhead_percent: (raw.median - adapter.median) / raw.median * 100.0,
        verdict: if difference < raw.spread().max(framework.spread()) {
            "indistinguishable"
        } else {
            "measured"
        },
        broker_bound: waiting >= per_message / 2.0,
    }
}

fn document(measured: &[Measured], round_trip: Duration, round_trips: usize) -> String {
    let mut out = format!(
        "{{\n  \"round_trip_micros\": {},\n  \"round_trip_samples\": {round_trips},\n  \
         \"scenarios\": [\n",
        round_trip.as_micros()
    );
    for (index, row) in measured.iter().enumerate() {
        let comma = if index + 1 == measured.len() { "" } else { "," };
        write!(
            out,
            concat!(
                "    {{\n",
                "      \"name\": \"{name}\",\n",
                "      \"unit\": \"msg/s\",\n",
                "      \"messages\": {messages},\n",
                "      \"pairs\": {rounds},\n",
                "      \"raw\": {{ \"median\": {raw_median:.0}, \"min\": {raw_min:.0},",
                " \"max\": {raw_max:.0} }},\n",
                "      \"adapter\": {{ \"median\": {ad_median:.0}, \"min\": {ad_min:.0},",
                " \"max\": {ad_max:.0} }},\n",
                "      \"framework\": {{ \"median\": {fw_median:.0}, \"min\": {fw_min:.0},",
                " \"max\": {fw_max:.0} }},\n",
                "      \"overhead_percent\": {overhead:.1},\n",
                "      \"adapter_overhead_percent\": {adapter_overhead:.1},\n",
                "      \"verdict\": \"{verdict}\",\n",
                "      \"broker_bound\": {broker_bound}\n",
                "    }}{comma}\n",
            ),
            name = row.scenario.name(),
            messages = row.messages,
            rounds = row.rounds,
            raw_median = row.raw.median,
            raw_min = row.raw.min,
            raw_max = row.raw.max,
            ad_median = row.adapter.median,
            ad_min = row.adapter.min,
            ad_max = row.adapter.max,
            fw_median = row.framework.median,
            fw_min = row.framework.min,
            fw_max = row.framework.max,
            overhead = row.overhead_percent,
            adapter_overhead = row.adapter_overhead_percent,
            verdict = row.verdict,
            broker_bound = row.broker_bound,
            comma = comma,
        )
        .expect("writing to a String");
    }
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
/// probe run, where it would panic in the statistics with every kept round discarded.
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

    let measured: Vec<Measured> = [Scenario::Queue, Scenario::RequestReply]
        .into_iter()
        .map(|scenario| runtime.block_on(measure(scenario, &url, rounds, seconds, round_trip)))
        .collect();

    println!();
    for row in &measured {
        println!(
            "{}: raw {:.0}, adapter {:.0} ({:+.1}%), framework {:.0} ({:+.1}%) msg/s ({}{})",
            row.scenario.name(),
            row.raw.median,
            row.adapter.median,
            -row.adapter_overhead_percent,
            row.framework.median,
            -row.overhead_percent,
            row.verdict,
            if row.broker_bound {
                ", broker-bound"
            } else {
                ""
            }
        );
    }

    std::fs::write(&out, document(&measured, round_trip, round_trips))
        .expect("the summary is written");
    println!("\nwrote {out}");
}
