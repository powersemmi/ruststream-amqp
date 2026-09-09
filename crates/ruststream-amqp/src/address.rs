//! [`AmqpAddress`]: the subscription descriptor.
//!
//! `AMQP` 1.0 standardises the wire but not the meaning of an address, so addressing stays
//! explicit: `queue` for anycast (competing consumers), `topic` for multicast (fan-out), and
//! `raw` for deployments with their own convention. The queue/topic constructors advertise the
//! matching terminus capability (`"queue"` / `"topic"`), which is how `ActiveMQ` Artemis and other
//! products disambiguate; `raw` sends the address verbatim with no capability.

use std::num::NonZeroU32;
use std::time::Duration;

use ruststream::{SubscriptionSource, nonzero};

use crate::broker::ConnectedAmqpBroker;
use crate::error::AmqpError;
use crate::subscriber::AmqpSubscriber;

/// Default protocol-level credit (prefetch) granted to a subscription.
pub const DEFAULT_CREDIT: NonZeroU32 = nonzero!(256);

/// Default deadline closing a partial batch on a batch subscription.
pub const DEFAULT_BATCH_WAIT: Duration = Duration::from_millis(10);

/// Delivery guarantee of a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Settle {
    /// Deliveries are settled by the handler: `ack` accepts, `nack` releases or rejects. The
    /// default.
    #[default]
    AtLeastOnce,
    /// Deliveries are settled on receipt; `ack`/`nack` report
    /// [`AckError::Unsupported`](ruststream::AckError::Unsupported).
    AtMostOnce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Queue,
    Topic,
    Raw,
}

/// A subscription descriptor for an `AMQP` 1.0 address.
///
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]` decorator:
///
/// ```
/// use ruststream::nonzero;
/// use ruststream_amqp::AmqpAddress;
///
/// let source = AmqpAddress::queue("orders").credit(nonzero!(64));
/// # let _ = source;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct AmqpAddress {
    address: String,
    kind: Kind,
    credit: NonZeroU32,
    settle: Settle,
    batch_wait: Duration,
}

impl AmqpAddress {
    fn of(address: String, kind: Kind) -> Self {
        Self {
            address,
            kind,
            credit: DEFAULT_CREDIT,
            settle: Settle::default(),
            batch_wait: DEFAULT_BATCH_WAIT,
        }
    }

    /// An anycast address: competing consumers, each message delivered to one of them.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_amqp::AmqpAddress;
    /// let source = AmqpAddress::queue("orders");
    /// # let _ = source;
    /// ```
    pub fn queue(name: impl Into<String>) -> Self {
        Self::of(name.into(), Kind::Queue)
    }

    /// A multicast address: fan-out, each message delivered to every subscriber.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_amqp::AmqpAddress;
    /// let source = AmqpAddress::topic("events");
    /// # let _ = source;
    /// ```
    pub fn topic(name: impl Into<String>) -> Self {
        Self::of(name.into(), Kind::Topic)
    }

    /// A verbatim address, for deployments with their own addressing convention
    /// (`"/queues/orders"` on `RabbitMQ` 4.x, a fully qualified queue on Artemis).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_amqp::AmqpAddress;
    /// let source = AmqpAddress::raw("/queues/orders");
    /// # let _ = source;
    /// ```
    pub fn raw(address: impl Into<String>) -> Self {
        Self::of(address.into(), Kind::Raw)
    }

    /// Sets the protocol-level credit (prefetch): how many unsettled deliveries the broker may
    /// have in flight to this subscription. Defaults to [`DEFAULT_CREDIT`].
    ///
    /// The count is a [`NonZeroU32`] because a subscription granted no credit receives nothing:
    /// zero is not a quieter setting but a stalled subscription, so it is unrepresentable here.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::nonzero;
    /// use ruststream_amqp::AmqpAddress;
    ///
    /// let source = AmqpAddress::queue("orders").credit(nonzero!(64));
    /// # let _ = source;
    /// ```
    ///
    /// A zero is rejected while the service is compiled, not when the subscription opens:
    ///
    /// ```compile_fail
    /// use ruststream::nonzero;
    /// use ruststream_amqp::AmqpAddress;
    ///
    /// let source = AmqpAddress::queue("orders").credit(nonzero!(0));
    /// # let _ = source;
    /// ```
    pub fn credit(mut self, credit: NonZeroU32) -> Self {
        self.credit = credit;
        self
    }

    /// Sets the delivery guarantee. Defaults to [`Settle::AtLeastOnce`].
    pub fn settle(mut self, settle: Settle) -> Self {
        self.settle = settle;
        self
    }

    /// Caps how long a partial batch waits for more deliveries on a batch subscription, counted
    /// from its first one. Defaults to [`DEFAULT_BATCH_WAIT`].
    ///
    /// `AMQP` 1.0 has no batch pull, so the batches are assembled on the client and this deadline
    /// is what trades latency for fuller batches under a trickle of traffic. It has no effect on a
    /// single-message subscription, where every delivery goes out as it arrives.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use ruststream_amqp::AmqpAddress;
    /// let source = AmqpAddress::queue("orders").batch_wait(Duration::from_millis(50));
    /// # let _ = source;
    /// ```
    pub fn batch_wait(mut self, batch_wait: Duration) -> Self {
        self.batch_wait = batch_wait;
        self
    }

    /// The address string sent to the broker.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    pub(crate) fn credit_value(&self) -> u32 {
        self.credit.get()
    }

    pub(crate) fn settle_value(&self) -> Settle {
        self.settle
    }

    pub(crate) fn batch_wait_value(&self) -> Duration {
        self.batch_wait
    }

    /// The terminus capability this descriptor advertises, when one applies.
    pub(crate) fn capability(&self) -> Option<&'static str> {
        match self.kind {
            Kind::Queue => Some("queue"),
            Kind::Topic => Some("topic"),
            Kind::Raw => None,
        }
    }

    /// Rejects descriptors that cannot form a subscription, before any I/O.
    ///
    /// Only the address is checked here: the credit is a [`NonZeroU32`], so an unusable one
    /// cannot reach this point.
    pub(crate) fn validate(&self) -> Result<(), AmqpError> {
        if self.address.is_empty() {
            return Err(AmqpError::InvalidAddress(
                "address must be non-empty".into(),
            ));
        }
        Ok(())
    }
}

impl SubscriptionSource<ConnectedAmqpBroker> for AmqpAddress {
    type Subscriber = AmqpSubscriber;

    fn name(&self) -> &str {
        self.address()
    }

    async fn subscribe(self, connected: &ConnectedAmqpBroker) -> Result<AmqpSubscriber, AmqpError> {
        connected.subscribe_address(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_address_is_rejected_before_io() {
        assert!(matches!(
            AmqpAddress::queue("").validate(),
            Err(AmqpError::InvalidAddress(_))
        ));
    }

    /// `credit(0)` does not compile: `nonzero!(0)` fails const evaluation, and a runtime zero
    /// cannot be built either. What is left to pin is that the default is the one that applies.
    #[test]
    fn the_credit_defaults_and_takes_an_override() {
        assert_eq!(AmqpAddress::queue("orders").credit_value(), 256);
        assert_eq!(
            AmqpAddress::queue("orders")
                .credit(nonzero!(64))
                .credit_value(),
            64
        );
    }

    #[test]
    fn the_batch_deadline_defaults_and_takes_an_override() {
        assert_eq!(
            AmqpAddress::queue("q").batch_wait_value(),
            DEFAULT_BATCH_WAIT
        );
        assert_eq!(
            AmqpAddress::queue("q")
                .batch_wait(Duration::from_millis(50))
                .batch_wait_value(),
            Duration::from_millis(50)
        );
    }

    #[test]
    fn constructors_pick_the_matching_capability() {
        assert_eq!(AmqpAddress::queue("q").capability(), Some("queue"));
        assert_eq!(AmqpAddress::topic("t").capability(), Some("topic"));
        assert_eq!(AmqpAddress::raw("/queues/q").capability(), None);
    }
}
