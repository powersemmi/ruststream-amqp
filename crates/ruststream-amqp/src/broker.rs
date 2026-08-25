//! The broker ladder: [`AmqpBroker`] -> [`ConnectedAmqpBroker`].
//!
//! Construction is synchronous and I/O-free; all network work happens in the consuming
//! [`Broker::connect`], and the connected form holds the live connection directly. One shared
//! cell remains so publishers can be handed out while the application is still being assembled,
//! before `connect` runs.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use fe2o3_amqp::connection::{Connection, ConnectionHandle};
use fe2o3_amqp::session::{Session, SessionHandle};
use fe2o3_amqp::{Receiver, Sender};
use fe2o3_amqp_types::messaging::Source;
use fe2o3_amqp_types::primitives::{Array, Symbol};
use ruststream::{Broker, ConnectedBroker, DefaultPublish, DescribeServer, ServerSpec, Subscribe};
use tokio::sync::{Mutex, OnceCell};

use crate::address::{AmqpAddress, Settle};
use crate::config::Sasl;
use crate::error::{AmqpError, box_err};
use crate::publisher::{AmqpPublish, AmqpPublisher};
use crate::subscriber::AmqpSubscriber;

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

/// One sender link on the shared publisher session, held in a slot the connection can empty.
///
/// Why the slot, and why the connection owns the link at all: the client detaches a link from
/// its `Drop`, which cannot await, so it fires a closing detach and destroys the link's relay in
/// the same breath. The peer's echoing detach then has nowhere to go, and the session answers an
/// unroutable handle by ending itself with an error - taking down every other publisher's links
/// with it. Emptying the slot instead hands the link to `close`, which awaits that echo while
/// the relay is still alive, and leaves a publish that raced the teardown with an empty slot to
/// report `NotConnected` against rather than a link the peer has already forgotten.
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
}

impl Broker for AmqpBroker {
    type Error = AmqpError;
    type Connected = ConnectedAmqpBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let container_id = self
            .container_id
            .clone()
            .unwrap_or_else(|| "ruststream".to_owned());
        let core = self
            .cell
            .get_or_try_init(async || {
                let mut builder = Connection::builder().container_id(container_id.clone());
                if let Some(sasl) = &self.sasl {
                    builder = builder.sasl_profile(sasl.profile.clone());
                }
                let mut conn = builder
                    .open(self.url.as_str())
                    .await
                    .map_err(|e| AmqpError::Connect(box_err(e)))?;
                let session = Session::begin(&mut conn)
                    .await
                    .map_err(|e| AmqpError::Session(box_err(e)))?;
                Ok::<_, AmqpError>(Arc::new(AmqpCore {
                    conn: Mutex::new(conn),
                    session: Mutex::new(session),
                    senders: Mutex::new(HashMap::new()),
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

impl DescribeServer for AmqpBroker {
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::new(
            self.url
                .trim_start_matches("amqps://")
                .trim_start_matches("amqp://"),
            "amqp",
        )
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

        // Each subscription runs on its own session: flow-control windows are per session, so a
        // slow consumer must not share one with the publishers or with other subscriptions.
        let session = {
            let mut conn = self.core.conn.lock().await;
            Session::begin(&mut conn)
                .await
                .map_err(|e| AmqpError::Session(box_err(e)))?
        };

        let subscriber = AmqpSubscriber::attach(&self.core, session, address).await?;
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
        // Teardown runs inwards - links, then the session carrying them, then the connection -
        // because each layer has to still be able to route the peer's answer to the one inside
        // it. Every step runs even after an earlier one fails, so a stuck link cannot leave the
        // connection open, and the report is the innermost failure: the outer ones after it are
        // its consequences, not independent faults.
        let senders_result = self.core.close_senders().await;
        let session_result = {
            let mut session = self.core.session.lock().await;
            session.end().await
        };
        let conn_result = {
            let mut conn = self.core.conn.lock().await;
            conn.close().await
        };
        senders_result?;
        session_result.map_err(|e| AmqpError::Session(box_err(e)))?;
        conn_result.map_err(|e| AmqpError::Connect(box_err(e)))?;
        Ok(())
    }
}

impl Subscribe for ConnectedAmqpBroker {
    type Subscriber = AmqpSubscriber;

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
