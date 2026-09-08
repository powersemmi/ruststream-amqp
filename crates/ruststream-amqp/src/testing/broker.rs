//! [`AmqpTestBroker`]: the in-process transport and its connected form.

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, HeaderMap, OutgoingMessage, RawMessage, Subscribe,
};

use crate::address::AmqpAddress;
use crate::error::AmqpError;
use crate::publisher::AmqpPublish;
use crate::testing::publisher::AmqpTestPublisher;
#[cfg(feature = "transaction")]
use crate::testing::publisher::AmqpTestTxnPublisher;
use crate::testing::router::AddressRouter;
use crate::testing::subscriber::AmqpTestSubscriber;

/// Shared state of one in-process broker: the router, the harness coordinator, and the transport's
/// liveness.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
    /// Mirrors the real transport: handles that alias a shut-down connection must report an error
    /// rather than route into a dead router.
    closed: AtomicBool,
    /// Names the private reply addresses of in-process requests, as the peer's dynamic terminus
    /// names them on a server.
    reply_seq: AtomicU64,
}

impl TestState {
    pub(crate) fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: HeaderMap) {
        self.router
            .publish(name, payload, headers, self.coordinator());
    }

    /// `Ok` while the transport is live, [`AmqpError::NotConnected`] once it has shut down - the
    /// error the real handles report for the same misuse.
    pub(crate) fn ensure_live(&self) -> Result<(), AmqpError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(AmqpError::NotConnected);
        }
        Ok(())
    }

    pub(crate) fn next_reply_address(&self) -> String {
        let seq = self.reply_seq.fetch_add(1, Ordering::Relaxed);
        format!("amqp-test-reply-{seq}")
    }
}

/// An in-process stand-in for [`AmqpBroker`](crate::AmqpBroker): same core routing, no server.
///
/// # Examples
///
/// ```
/// use ruststream_amqp::testing::AmqpTestBroker;
///
/// let broker = AmqpTestBroker::new();
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct AmqpTestBroker {
    state: Arc<TestState>,
}

impl AmqpTestBroker {
    /// Creates an empty in-process broker. Synchronous and I/O-free, like the real `new`.
    pub fn new() -> Self {
        Self::default()
    }

    /// A publisher usable before `connect`, mirroring the real broker's early-publisher path.
    #[must_use]
    pub fn publisher(&self) -> AmqpTestPublisher {
        AmqpTestPublisher::new(Arc::clone(&self.state))
    }
}

impl Broker for AmqpTestBroker {
    type Error = AmqpError;
    type Connected = ConnectedAmqpTestBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedAmqpTestBroker { state: self.state }))
    }
}

/// The connected form of [`AmqpTestBroker`]; implements
/// [`TestableBroker`](ruststream::testing::TestableBroker) for the harness and the conformance
/// suite.
#[derive(Debug, Clone)]
pub struct ConnectedAmqpTestBroker {
    state: Arc<TestState>,
}

impl ConnectedAmqpTestBroker {
    /// A publisher from the connected form, mirroring
    /// [`ConnectedAmqpBroker::publisher`](crate::ConnectedAmqpBroker::publisher).
    #[must_use]
    pub fn publisher(&self) -> AmqpTestPublisher {
        AmqpTestPublisher::new(Arc::clone(&self.state))
    }

    /// A transactional publisher from the connected form, mirroring
    /// [`ConnectedAmqpBroker::transactional_publisher`](crate::ConnectedAmqpBroker::transactional_publisher).
    #[cfg(feature = "transaction")]
    #[must_use]
    pub fn transactional_publisher(&self) -> AmqpTestTxnPublisher {
        AmqpTestTxnPublisher::new(Arc::clone(&self.state))
    }

    /// Opens a subscription described by `address`, mirroring
    /// [`ConnectedAmqpBroker::subscribe_address`](crate::ConnectedAmqpBroker::subscribe_address),
    /// so a handler declared with the production descriptor mounts here unchanged.
    ///
    /// The descriptor keeps its meaning here. The address is what the stand-in routes by; the
    /// terminus decides how, so [`queue`](AmqpAddress::queue) subscriptions on one address compete
    /// for each message and [`topic`](AmqpAddress::topic) ones each get a copy, which is what makes
    /// a work-queue service testable in process at all. The settle mode holds too (an at-most-once
    /// delivery arrives settled and its `ack` reports
    /// [`AckError::Unsupported`](ruststream::AckError::Unsupported), as it does against a server),
    /// and so does the batch deadline, which is the framework's own buffer on both brokers.
    ///
    /// One option has no counterpart here: [`credit`](AmqpAddress::credit) is link flow control,
    /// which keeps unsent messages on the broker until the subscription has room. A channel cannot
    /// hold them the same way without becoming a broker-side queue, and nothing a handler observes
    /// would change, so the in-process subscription is unbounded: no test can assert a prefetch
    /// window, and none should. The live suite covers what credit does to a real link.
    ///
    /// Two more differences are the router's, not the descriptor's, and the module docs of the
    /// registry state them: a message published to an address with no live subscription is logged
    /// and dropped rather than stored, and a released delivery returns to the subscription that had
    /// it rather than to the address, so a competing consumer does not pick it up.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::InvalidAddress`] for a descriptor that cannot form a subscription (an
    /// empty address, zero credit), which is what the real broker rejects before any I/O.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Broker;
    /// use ruststream_amqp::AmqpAddress;
    /// use ruststream_amqp::testing::AmqpTestBroker;
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), ruststream_amqp::AmqpError> {
    /// let broker = AmqpTestBroker::new().connect().await?;
    /// let subscriber = broker.subscribe_address(AmqpAddress::queue("orders")).await?;
    /// # let _ = subscriber;
    /// # Ok(())
    /// # }
    /// ```
    // The descriptor is taken by value because that is the shape of the contract: the real
    // broker's method consumes it, and `SubscriptionSource::subscribe` hands it over. A reference
    // here would make the two brokers spell the same call differently.
    #[allow(clippy::needless_pass_by_value)]
    pub fn subscribe_address(
        &self,
        address: AmqpAddress,
    ) -> impl Future<Output = Result<AmqpTestSubscriber, AmqpError>> {
        ready(self.open(&address))
    }

    /// The one place a subscription is registered, so the descriptor path and the name path
    /// cannot drift apart.
    fn open(&self, address: &AmqpAddress) -> Result<AmqpTestSubscriber, AmqpError> {
        address.validate()?;
        self.state.ensure_live()?;
        let (id, requeue, rx) = self
            .state
            .router
            .subscribe(address.address().to_owned(), address.routing());
        Ok(AmqpTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
            self.state.coordinator().cloned(),
            address,
        ))
    }
}

impl ConnectedBroker for ConnectedAmqpTestBroker {
    type Error = AmqpError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        // Publishers handed out earlier alias this transport and outlive it, so they have to see
        // the closure rather than route into a cleared router.
        self.state.closed.store(true, Ordering::Release);
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedAmqpTestBroker {
    type Subscriber = AmqpTestSubscriber;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        // A bare name is a verbatim address on the real broker; it is one here too, so both
        // subscription paths carry the same meaning.
        ready(self.open(&AmqpAddress::raw(name)))
    }
}

impl TestableBroker for ConnectedAmqpTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.state.publish(
            message.name(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(ConnectedAmqpTestBroker);

/// The default reply publisher is the production policy, so a handler that names no publisher
/// replies through the same declaration on both brokers.
impl DefaultPublish for ConnectedAmqpTestBroker {
    type Policy = AmqpPublish;
}
