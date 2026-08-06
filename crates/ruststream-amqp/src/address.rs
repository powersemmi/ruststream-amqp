//! [`AmqpAddress`]: the subscription descriptor.
//!
//! `AMQP` 1.0 standardises the wire but not the meaning of an address, so addressing stays
//! explicit: `queue` for anycast (competing consumers), `topic` for multicast (fan-out), and
//! `raw` for deployments with their own convention. The queue/topic constructors advertise the
//! matching terminus capability (`"queue"` / `"topic"`), which is how `ActiveMQ` Artemis and other
//! products disambiguate; `raw` sends the address verbatim with no capability.

use ruststream::SubscriptionSource;

use crate::broker::ConnectedAmqpBroker;
use crate::error::AmqpError;
use crate::subscriber::AmqpSubscriber;

/// Default protocol-level credit (prefetch) granted to a subscription.
pub const DEFAULT_CREDIT: u32 = 256;

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
/// use ruststream_amqp::AmqpAddress;
///
/// let source = AmqpAddress::queue("orders").credit(64);
/// # let _ = source;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct AmqpAddress {
    address: String,
    kind: Kind,
    credit: u32,
    settle: Settle,
}

impl AmqpAddress {
    fn of(address: String, kind: Kind) -> Self {
        Self {
            address,
            kind,
            credit: DEFAULT_CREDIT,
            settle: Settle::default(),
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
    pub fn credit(mut self, credit: u32) -> Self {
        self.credit = credit;
        self
    }

    /// Sets the delivery guarantee. Defaults to [`Settle::AtLeastOnce`].
    pub fn settle(mut self, settle: Settle) -> Self {
        self.settle = settle;
        self
    }

    /// The address string sent to the broker.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    pub(crate) fn credit_value(&self) -> u32 {
        self.credit
    }

    pub(crate) fn settle_value(&self) -> Settle {
        self.settle
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
    pub(crate) fn validate(&self) -> Result<(), AmqpError> {
        if self.address.is_empty() {
            return Err(AmqpError::InvalidAddress(
                "address must be non-empty".into(),
            ));
        }
        if self.credit == 0 {
            return Err(AmqpError::InvalidAddress(
                "credit must be at least 1".into(),
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

    #[test]
    fn zero_credit_is_rejected_before_io() {
        assert!(matches!(
            AmqpAddress::queue("orders").credit(0).validate(),
            Err(AmqpError::InvalidAddress(_))
        ));
    }

    #[test]
    fn constructors_pick_the_matching_capability() {
        assert_eq!(AmqpAddress::queue("q").capability(), Some("queue"));
        assert_eq!(AmqpAddress::topic("t").capability(), Some("topic"));
        assert_eq!(AmqpAddress::raw("/queues/q").capability(), None);
    }
}
