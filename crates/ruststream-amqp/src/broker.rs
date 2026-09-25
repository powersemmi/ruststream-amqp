//! The broker ladder: [`AmqpBroker`] -> [`ConnectedAmqpBroker`].
//!
//! Construction is synchronous and I/O-free; all network work happens in the consuming
//! [`Broker::connect`], and the connected form holds the live connection directly. One shared
//! cell remains so publishers can be handed out while the application is still being assembled,
//! before `connect` runs.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};

use fe2o3_amqp::connection::{Connection, ConnectionHandle, Error as ConnectionError};
use fe2o3_amqp::sasl_profile::SaslProfile;
use fe2o3_amqp::session::{Session, SessionHandle};
use fe2o3_amqp::{Receiver, Sender};
use fe2o3_amqp_types::messaging::Source;
use fe2o3_amqp_types::primitives::{Array, Symbol};
use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, DefaultPublish, DescribeServer, ServerSpec, Subscribe,
};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, OnceCell, mpsc, watch};
use url::Url;

use crate::address::{AmqpAddress, Settle};
#[cfg(feature = "asyncapi")]
use crate::bindings;
use crate::config::Sasl;
use crate::error::{AmqpError, box_err};
use crate::publisher::{AmqpPublish, AmqpPublisher};
use crate::subscriber::AmqpSubscriber;

/// The AMQP ports a URL that names none falls back to, as the specification assigns them.
const AMQP_PORT: u16 = 5672;
const AMQPS_PORT: u16 = 5671;

/// Connects the socket the connection will run on, with Nagle's algorithm off.
///
/// The client opens a socket of its own when given a URL, and leaves that option at the kernel's
/// default. AMQP is small-write-then-wait on both sides - a publish waits for its disposition, a
/// reply waits for the request - which is the pattern Nagle's algorithm holds back until the
/// peer's delayed acknowledgement arrives. That is tens of milliseconds per exchange on an idle
/// local network, and nothing in a log names it. So the socket is opened here and handed over
/// configured.
///
/// # Errors
///
/// Returns [`AmqpError::Connect`] when the URL names no reachable address, when the connection is
/// refused, or when the option cannot be set on the socket.
async fn open_socket(url: &Url) -> Result<TcpStream, AmqpError> {
    let addresses = url
        .socket_addrs(|| match url.scheme() {
            "amqps" => Some(AMQPS_PORT),
            _ => Some(AMQP_PORT),
        })
        .map_err(|e| AmqpError::Connect(box_err(e)))?;
    let stream = TcpStream::connect(&*addresses)
        .await
        .map_err(|e| AmqpError::Connect(box_err(e)))?;
    stream
        .set_nodelay(true)
        .map_err(|e| AmqpError::Connect(box_err(e)))?;
    Ok(stream)
}

/// The live connection state shared by the connected form and every handle derived from it.
///
/// Why runtime checks exist here at all: publishers may be handed out before `connect` and may
/// outlive `shutdown` (aliasing), so the dead-connection path must be a runtime error - the
/// typed ladder covers only the owner's handle.
pub(crate) struct AmqpCore {
    pub(crate) conn: Mutex<ConnectionHandle<()>>,
    /// The shared session publishers attach their links on. Subscriptions get their own session
    /// each, so one slow consumer cannot exhaust the shared flow window.
    pub(crate) session: Mutex<SessionHandle<()>>,
    /// The sender links attached on that session, one per address, owned here rather than by the
    /// publisher handle that attached them. See [`SenderLink`] for why the connection has to own
    /// them.
    senders: Mutex<HashMap<String, Arc<SenderLink>>>,
    /// The pump tasks of the subscriptions on this connection, each ending a session of its own.
    pub(crate) pumps: Pumps,
    pub(crate) closed: AtomicBool,
    pub(crate) container_id: String,
    link_seq: AtomicU64,
}

impl AmqpCore {
    pub(crate) fn ensure_open(&self) -> Result<(), AmqpError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(AmqpError::NotConnected);
        }
        Ok(())
    }

    /// The sender link for `address`, attached on first use and shared by every publisher on this
    /// connection.
    // The map guard intentionally spans the attach so two callers cannot race a double-attach
    // for the same address.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn sender_for(&self, address: &str) -> Result<Arc<SenderLink>, AmqpError> {
        let mut senders = self.senders.lock().await;
        if let Some(link) = senders.get(address) {
            return Ok(Arc::clone(link));
        }
        let sender = ConnectedAmqpBroker::attach_sender(self, address).await?;
        let link = Arc::new(SenderLink(Mutex::new(Some(sender))));
        senders.insert(address.to_owned(), Arc::clone(&link));
        Ok(link)
    }

    /// Closes every attached sender link, reporting the first failure once all of them have been
    /// attempted. Runs before the session ends, because the peer answers each close with a detach
    /// the session still has to route.
    async fn close_senders(&self) -> Result<(), AmqpError> {
        let links: Vec<(String, Arc<SenderLink>)> =
            self.senders.lock().await.drain().collect::<Vec<_>>();
        let mut first_error = None;
        for (address, link) in links {
            if let Err(err) = link.close(&address).await {
                first_error.get_or_insert(err);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Closes the connection, which takes the proof that no subscription session is still ending:
    /// a session's end that reaches the peer after the close is answered on a connection already
    /// in `CloseSent`, and the client reports that as `IllegalState`.
    async fn close_connection(
        &self,
        _sessions_ended: SessionsEnded,
    ) -> Result<(), ConnectionError> {
        self.conn.lock().await.close().await
    }

    /// A process-unique link name; `AMQP` link names must be unique per connection.
    pub(crate) fn link_name(&self, role: &str) -> String {
        let seq = self.link_seq.fetch_add(1, Ordering::Relaxed);
        format!("{}-{role}-{seq}", self.container_id)
    }

    pub(crate) fn correlation_id(&self) -> String {
        let seq = self.link_seq.fetch_add(1, Ordering::Relaxed);
        format!("{}-corr-{seq}", self.container_id)
    }
}

/// The subscriptions' pump tasks, as the connection sees them: a signal that stops them and a way
/// to wait until every one has ended its session.
///
/// Why this is tracked at run time: a subscription's session is ended by its pump task, and that
/// task outlives the subscriber handle that started it, because dropping a handle cannot await a
/// session end. The connection therefore cannot close when its handles are gone; it closes when
/// the tasks say they are done, and [`SessionsEnded`] is how they say it.
pub(crate) struct Pumps {
    stop: watch::Sender<bool>,
    /// Cloned into each pump and dropped when the pump has ended its session; taken out by
    /// shutdown, after which no pump can start.
    running: StdMutex<Option<mpsc::Sender<()>>>,
    ended: Mutex<mpsc::Receiver<()>>,
}

/// What a pump task holds for as long as its session is live.
pub(crate) struct PumpGuard {
    pub(crate) stop: watch::Receiver<bool>,
    /// Never sent on: its drop is the signal that the session has ended.
    _running: mpsc::Sender<()>,
}

/// Proof that every subscription session on the connection has ended, which the connection's
/// close requires. Only [`Pumps::stop_all`] makes one.
pub(crate) struct SessionsEnded(());

impl Pumps {
    fn new() -> Self {
        let (stop, _) = watch::channel(false);
        let (running, ended) = mpsc::channel(1);
        Self {
            stop,
            running: StdMutex::new(Some(running)),
            ended: Mutex::new(ended),
        }
    }

    /// The guard a new pump task holds.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] once shutdown has begun.
    pub(crate) fn guard(&self) -> Result<PumpGuard, AmqpError> {
        let running = self
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .ok_or(AmqpError::NotConnected)?;
        Ok(PumpGuard {
            stop: self.stop.subscribe(),
            _running: running,
        })
    }

    /// Stops every pump and waits until each has ended its session.
    async fn stop_all(&self) -> SessionsEnded {
        let running = self
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        drop(running);
        self.stop.send_replace(true);
        // Every guard is a sender; the channel reports its end once the last one is dropped.
        let _ = self.ended.lock().await.recv().await;
        SessionsEnded(())
    }
}

/// One sender link on the shared publisher session, held in a slot the connection can empty.
///
/// Why the slot, and why the connection owns the link at all: the client detaches a link from
/// its `Drop`, which cannot await, so a dropped link only queues its closing detach. Shutdown
/// closes every link and waits for the peer's answer before it ends the session carrying them,
/// and that takes the link itself rather than a handle that may already be gone. Emptying the
/// slot hands the link to `close`, which awaits that answer, and leaves a publish that raced the
/// teardown with an empty slot to report `NotConnected` against rather than a link the peer has
/// already forgotten.
pub(crate) struct SenderLink(Mutex<Option<Sender>>);

impl SenderLink {
    /// Runs `f` against the live link, or reports [`AmqpError::NotConnected`] once it is closed.
    // The guard intentionally spans the call: one link cannot carry two transfers at once, so
    // serialising publishers on the slot is the point rather than an oversight.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn with<F, T>(&self, f: F) -> Result<T, AmqpError>
    where
        F: AsyncFnOnce(&mut Sender) -> Result<T, AmqpError>,
    {
        let mut slot = self.0.lock().await;
        let sender = slot.as_mut().ok_or(AmqpError::NotConnected)?;
        f(sender).await
    }

    async fn close(&self, address: &str) -> Result<(), AmqpError> {
        // Take the link out under the lock and close it outside: a publish that races the
        // teardown then finds an empty slot instead of waiting on the close.
        let taken = {
            let mut slot = self.0.lock().await;
            slot.take()
        };
        let Some(sender) = taken else {
            return Ok(());
        };
        sender.close().await.map_err(|e| AmqpError::Detach {
            address: address.to_owned(),
            source: box_err(e),
        })
    }
}

impl std::fmt::Debug for SenderLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SenderLink").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AmqpCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpCore")
            .field("container_id", &self.container_id)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

pub(crate) type CoreCell = Arc<OnceCell<Arc<AmqpCore>>>;

/// The container id a service presents when it names none of its own.
const DEFAULT_CONTAINER_ID: &str = "ruststream";

/// An `AMQP` 1.0 broker for the `RustStream` messaging framework.
///
/// `new` is synchronous and records only configuration; the runtime dials once at startup via
/// the consuming [`Broker::connect`]. That is what lets a service compose with the synchronous
/// `#[ruststream::app]` builder.
///
/// # Examples
///
/// ```
/// use ruststream_amqp::{AmqpBroker, Sasl};
///
/// let broker = AmqpBroker::new("amqp://localhost:5672")
///     .sasl(Sasl::plain("svc", "secret"))
///     .container_id("orders-svc");
/// # let _ = broker;
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct AmqpBroker {
    url: String,
    container_id: Option<String>,
    sasl: Option<Sasl>,
    // Shared with publishers handed out before connect; the consuming connect fills it.
    cell: CoreCell,
}

impl AmqpBroker {
    /// Records the connection URL (`amqp://` or, with a TLS feature, `amqps://`). No I/O.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            container_id: None,
            sasl: None,
            cell: Arc::new(OnceCell::new()),
        }
    }

    /// Sets the SASL profile. A URL with userinfo (`amqp://user:pass@host`) selects PLAIN
    /// implicitly; an explicit profile set here wins.
    pub fn sasl(mut self, sasl: Sasl) -> Self {
        self.sasl = Some(sasl);
        self
    }

    /// Sets the `AMQP` container id identifying this service to the broker. Defaults to
    /// `"ruststream"`.
    pub fn container_id(mut self, id: impl Into<String>) -> Self {
        self.container_id = Some(id.into());
        self
    }

    /// A publisher sharing this broker's connection cell; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> AmqpPublisher {
        AmqpPublisher::new(Arc::clone(&self.cell))
    }

    /// The container this service presents on the connection: the name it set, or the default.
    fn container(&self) -> &str {
        self.container_id.as_deref().unwrap_or(DEFAULT_CONTAINER_ID)
    }

    /// The host and port a client connects to, over the protocol it speaks.
    fn coordinate(&self) -> ServerSpec {
        ServerSpec::from_url(&self.url, "amqp1").protocol_version("1.0")
    }
}

impl Broker for AmqpBroker {
    type Error = AmqpError;
    type Connected = ConnectedAmqpBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let container_id = self.container().to_owned();
        let core = self
            .cell
            .get_or_try_init(async || {
                let url =
                    Url::parse(self.url.as_str()).map_err(|e| AmqpError::Connect(box_err(e)))?;
                let stream = open_socket(&url).await?;

                // The client's own `open` reads these off the URL; opening the socket here means
                // reading them here, and the order is the client's: an explicit profile first,
                // the URL's credentials over it.
                let mut builder = Connection::builder()
                    .container_id(container_id.clone())
                    .scheme(url.scheme());
                if let Some(hostname) = url.host_str() {
                    builder = builder.hostname(hostname).sasl_hostname(hostname);
                }
                if let Some(domain) = url.domain() {
                    builder = builder.domain(domain);
                }
                if let Some(sasl) = &self.sasl {
                    builder = builder.sasl_profile(sasl.profile.clone());
                }
                if let Ok(profile) = SaslProfile::try_from(&url) {
                    builder = builder.sasl_profile(profile);
                }
                let mut conn = builder
                    .open_with_stream(stream)
                    .await
                    .map_err(|e| AmqpError::Connect(box_err(e)))?;
                let session = Session::begin(&mut conn)
                    .await
                    .map_err(|e| AmqpError::Session(box_err(e)))?;
                Ok::<_, AmqpError>(Arc::new(AmqpCore {
                    conn: Mutex::new(conn),
                    session: Mutex::new(session),
                    senders: Mutex::new(HashMap::new()),
                    pumps: Pumps::new(),
                    closed: AtomicBool::new(false),
                    container_id,
                    link_seq: AtomicU64::new(0),
                }))
            })
            .await?
            .clone();
        Ok(ConnectedAmqpBroker {
            core,
            cell: self.cell,
        })
    }
}

/// `DescribeServer` reports the host and port the service connects to, which is what the
/// `AsyncAPI` document records for it. Credentials in the URL are not part of that coordinate and
/// do not reach the document: `ServerSpec::from_url` drops the userinfo an `amqp://` URL may
/// carry, so no broker crate has to remember to.
///
/// The protocol is `amqp1`, which is the specification's key for `AMQP` 1.0; `amqp` is the key for
/// `AMQP` 0.9.1, a different protocol that shares the scheme and the port. Neither a reader of the
/// document nor a tool generating a client from it can tell the two apart from the host, so the
/// version is spelled out beside the key.
impl DescribeServer for AmqpBroker {
    #[cfg(not(feature = "asyncapi"))]
    fn describe_server(&self) -> ServerSpec {
        self.coordinate()
    }

    /// The server binding carries the container id this service presents on the connection, which
    /// is how an operator finds its links in the broker's own console.
    #[cfg(feature = "asyncapi")]
    fn describe_server(&self) -> ServerSpec {
        self.coordinate()
            .bindings(bindings::server(self.container()))
    }
}

/// The typed witness that `connect` succeeded: holds the live connection directly.
#[derive(Debug)]
pub struct ConnectedAmqpBroker {
    pub(crate) core: Arc<AmqpCore>,
    // Keeps the cell of publishers handed out before connect alive and filled.
    cell: CoreCell,
}

impl ConnectedAmqpBroker {
    /// A publisher from the connected form. It rides the same cell-backed publisher type as the
    /// early path; by now `connect` has filled the cell, so it resolves immediately.
    #[must_use]
    pub fn publisher(&self) -> AmqpPublisher {
        AmqpPublisher::new(Arc::clone(&self.cell))
    }

    /// Opens a subscription described by `address`.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError`] when the descriptor is invalid, the session cannot begin, or the
    /// link cannot attach.
    pub async fn subscribe_address(
        &self,
        address: AmqpAddress,
    ) -> Result<AmqpSubscriber, AmqpError> {
        address.validate()?;
        self.core.ensure_open()?;
        // Taken before the session begins, so a subscription opening on one handle races no
        // shutdown on another: once the guard is out, the connection waits for this session
        // before it closes, and once shutdown has begun no session begins.
        let guard = self.core.pumps.guard()?;

        // Each subscription runs on its own session: flow-control windows are per session, so a
        // slow consumer must not share one with the publishers or with other subscriptions.
        let session = {
            let mut conn = self.core.conn.lock().await;
            Session::begin(&mut conn)
                .await
                .map_err(|e| AmqpError::Session(box_err(e)))?
        };

        let subscriber = AmqpSubscriber::attach(&self.core, session, address, guard).await?;
        Ok(subscriber)
    }

    /// Attaches a sender link for `address` on the shared publisher session.
    pub(crate) async fn attach_sender(core: &AmqpCore, address: &str) -> Result<Sender, AmqpError> {
        let mut session = core.session.lock().await;
        Sender::attach(&mut session, core.link_name("sender"), address)
            .await
            .map_err(|e| AmqpError::Attach {
                address: address.to_owned(),
                source: box_err(e),
            })
    }

    /// Attaches a dynamically addressed receiver (the broker names the source), for
    /// request/reply.
    pub(crate) async fn attach_dynamic_receiver(core: &AmqpCore) -> Result<Receiver, AmqpError> {
        let mut session = core.session.lock().await;
        Receiver::builder()
            .name(core.link_name("reply"))
            .source(Source::builder().dynamic(true).build())
            .auto_accept(true)
            .attach(&mut session)
            .await
            .map_err(|e| AmqpError::Attach {
                address: "(dynamic)".to_owned(),
                source: box_err(e),
            })
    }
}

/// Builds the receiver source for a descriptor, advertising the queue/topic capability when the
/// descriptor names one.
pub(crate) fn source_for(address: &AmqpAddress) -> Source {
    let mut source = Source::builder().address(address.address()).build();
    if let Some(capability) = address.capability() {
        source.capabilities = Some(Array::from(vec![Symbol::from(capability)]));
    }
    source
}

impl ConnectedBroker for ConnectedAmqpBroker {
    type Error = AmqpError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.core.closed.store(true, Ordering::Release);
        // Teardown runs inwards - the subscriptions' sessions, the publisher links, then the
        // session carrying them, then the connection - because each layer has to still be able to
        // route the peer's answer to the one inside it. Every step runs even after an earlier one
        // fails, so a stuck link cannot leave the connection open, and the report is the innermost
        // failure: the outer ones after it are its consequences, not independent faults.
        let sessions_ended = self.core.pumps.stop_all().await;
        let senders_result = self.core.close_senders().await;
        let session_result = {
            let mut session = self.core.session.lock().await;
            session.end().await
        };
        let conn_result = self.core.close_connection(sessions_ended).await;
        senders_result?;
        session_result.map_err(|e| AmqpError::Session(box_err(e)))?;
        conn_result.map_err(|e| AmqpError::Connect(box_err(e)))?;
        Ok(())
    }
}

impl Subscribe for ConnectedAmqpBroker {
    type Subscriber = AmqpSubscriber;

    /// An `AMQP` 1.0 node is one address for both roles: a receiver attaches its source to it, a
    /// sender its target. A bare name is therefore also where a deferred copy is published to
    /// reach the subscription again, which is what makes `out_retry` usable on this broker without
    /// the mount site naming a destination.
    type Copies = AddressedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_address(AmqpAddress::raw(name)).await
    }
}

impl DefaultPublish for ConnectedAmqpBroker {
    type Policy = AmqpPublish;
}

// Re-exported for the subscriber module without making Settle a broker concern.
pub(crate) fn is_at_most_once(settle: Settle) -> bool {
    matches!(settle, Settle::AtMostOnce)
}

#[cfg(test)]
mod tests {
    use super::{AmqpBroker, DescribeServer};

    /// The URL carries the credentials the connection needs, and the description is published in
    /// the service's `AsyncAPI` document, so the two must not be the same string. The parsing is
    /// the core's (`ServerSpec::from_url`); what this holds is that the broker goes through it.
    #[test]
    fn a_url_carrying_credentials_describes_a_server_without_them() {
        let spec = AmqpBroker::new("amqp://artemis:artemis@broker.example.com:5672/prod")
            .describe_server();

        assert_eq!(spec.host.as_deref(), Some("broker.example.com:5672"));
        assert_eq!(spec.protocol, "amqp1");
        assert_eq!(spec.protocol_version.as_deref(), Some("1.0"));

        let host = spec.host.expect("a networked broker describes a host");
        assert!(!host.contains("artemis"), "the description leaked {host:?}");
        assert!(!host.contains('@'), "the description leaked {host:?}");
    }
}
