//! [`AmqpTestBroker`]: the in-process transport and its connected form.

use std::future::{Future, ready};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher,
    RawMessage, Subscribe,
};

use crate::address::AmqpAddress;
use crate::error::AmqpError;
use crate::testing::router::AddressRouter;
use crate::testing::subscriber::AmqpTestSubscriber;

/// Shared state of one in-process broker: the router plus the harness coordinator.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
}

impl TestState {
    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: ruststream::HeaderMap) {
        self.router
            .publish(name, payload, headers, self.coordinator());
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
        AmqpTestPublisher {
            state: Arc::clone(&self.state),
        }
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
    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> AmqpTestPublisher {
        AmqpTestPublisher {
            state: Arc::clone(&self.state),
        }
    }

    /// Opens a subscription described by `address`, mirroring
    /// [`ConnectedAmqpBroker::subscribe_address`](crate::ConnectedAmqpBroker::subscribe_address),
    /// so a handler declared with the production descriptor mounts here unchanged.
    ///
    /// What the descriptor decides on the client is reproduced: the address the stand-in routes
    /// by, the settle mode (an at-most-once delivery arrives settled and its `ack` reports
    /// [`AckError::Unsupported`](ruststream::AckError::Unsupported), as it does against a server),
    /// and the batch deadline, which is the framework's own buffer on both brokers. What the
    /// protocol decides is dropped, because there is no protocol here:
    /// [`credit`](AmqpAddress::credit) is link flow control, and the queue/topic distinction is a
    /// terminus capability the peer honours, so every subscription fans out like a topic. A test
    /// asserting that competing consumers on one queue each see a delivery once would therefore
    /// assert something a real broker never holds up; that case belongs in the live suite.
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
        let (id, requeue, rx) = self.state.router.subscribe(address.address().to_owned());
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

/// Publisher for the in-process broker.
#[derive(Debug, Clone)]
pub struct AmqpTestPublisher {
    state: Arc<TestState>,
}

impl Publisher for AmqpTestPublisher {
    type Error = AmqpError;

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        );
        ready(Ok(()))
    }
}

/// The publish policy for [`AmqpTestPublisher`], mirroring
/// [`AmqpPublish`](crate::AmqpPublish) on the real broker.
///
/// # Examples
///
/// ```
/// use ruststream_amqp::testing::AmqpTestPublish;
///
/// let policy = AmqpTestPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct AmqpTestPublish;

impl PublishPolicy<ConnectedAmqpTestBroker> for AmqpTestPublish {
    type Live = AmqpTestPublisher;

    fn pair(
        self,
        connected: &ConnectedAmqpTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

impl DefaultPublish for ConnectedAmqpTestBroker {
    type Policy = AmqpTestPublish;
}
