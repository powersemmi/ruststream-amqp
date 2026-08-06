//! [`AmqpTestBroker`]: the in-process transport and its connected form.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher,
    RawMessage, Subscribe,
};

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

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: ruststream::Headers) {
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

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        Ok(ConnectedAmqpTestBroker { state: self.state })
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
}

impl ConnectedBroker for ConnectedAmqpTestBroker {
    type Error = AmqpError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.state.router.clear();
        Ok(())
    }
}

impl Subscribe for ConnectedAmqpTestBroker {
    type Subscriber = AmqpTestSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        let (id, requeue, rx) = self.state.router.subscribe(name.to_owned());
        Ok(AmqpTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
            self.state.coordinator().cloned(),
        ))
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

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        );
        Ok(())
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

    async fn pair(self, connected: &ConnectedAmqpTestBroker) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
    }
}

impl DefaultPublish for ConnectedAmqpTestBroker {
    type Policy = AmqpTestPublish;
}
