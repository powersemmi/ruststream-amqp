//! [`AmqpAddress`]: the subscription descriptor.
//!
//! `AMQP` 1.0 standardises the wire but not the meaning of an address, so addressing stays
//! explicit: `queue` for anycast (competing consumers), `topic` for multicast (fan-out), and
//! `raw` for deployments with their own convention. The queue/topic constructors advertise the
//! matching terminus capability (`"queue"` / `"topic"`), which is how `ActiveMQ` Artemis and other
//! products disambiguate; `raw` sends the address verbatim with no capability.

use std::future::{Future, ready};
use std::num::NonZeroU32;
use std::time::Duration;

#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::runtime::IntoSource;
use ruststream::{
    AddressedCopies, RedeliveryAddress, RedeliveryAddressed, SubscriptionSource, nonzero,
};

#[cfg(feature = "asyncapi")]
use crate::bindings;

use crate::broker::ConnectedAmqpBroker;
use crate::error::AmqpError;
use crate::subscriber::AmqpSubscriber;

/// Default link credit granted to a subscription: the window of deliveries the broker may send
/// ahead of the handler's settlements.
pub const DEFAULT_CREDIT: NonZeroU32 = nonzero!(256);

/// Default deadline closing a partial batch on a batch subscription.
pub const DEFAULT_BATCH_WAIT: Duration = Duration::from_millis(10);

/// Delivery guarantee of a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Settle {
    /// Deliveries are settled by the handler. The default.
    ///
    /// `ack` sends `accepted`. A retry, `nack(true)`, sends `modified` with `delivery-failed`,
    /// never `released`, so the broker counts the attempt in `delivery-count`. A drop,
    /// `nack(false)`, sends `rejected`, and the broker's dead-letter policy decides what follows.
    #[default]
    AtLeastOnce,
    /// Deliveries are settled on receipt, so a message is gone whether the handler finished or
    /// not; `ack` and `nack` report
    /// [`AckError::Unsupported`](ruststream::AckError::Unsupported).
    AtMostOnce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Queue,
    Topic,
    Raw,
}

/// How the subscriptions on one address share its traffic: the terminus capability seen from the
/// consuming end.
///
/// This is the difference between a work queue and a broadcast, so the in-process transport
/// reproduces it instead of handing every message to everyone and hoping the deployment agrees.
#[cfg(feature = "testing")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Routing {
    /// Competing consumers: each message goes to exactly one of the subscriptions.
    Anycast,
    /// Fan-out: every subscription gets its own copy.
    Multicast,
}

/// A subscription descriptor for an `AMQP` 1.0 address.
///
/// Implements [`SubscriptionSource`], so it can sit inline in the `#[subscriber(..)]` decorator:
///
/// ```
/// # mod demo {
/// use ruststream_amqp::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(AmqpAddress::queue("orders").credit(nonzero!(64)))]
/// async fn fulfil(order: &Order) -> HandlerOutcome {
///     println!("fulfilling order {}", order.id);
///     HandlerOutcome::ack()
/// }
/// #
/// # #[ruststream::app]
/// # fn app() -> impl App {
/// #     RustStream::new(AppInfo::new("orders", "0.1.0"))
/// #         .with_broker(AmqpBroker::new("amqp://localhost:5672"), |b| {
/// #             b.include(fulfil);
/// #         })
/// # }
/// # }
/// # fn main() {}
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
    /// # mod demo {
    /// use ruststream_amqp::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// // Every replica of the service competes for "orders": each order is handled once.
    /// #[subscriber(AmqpAddress::queue("orders"))]
    /// async fn fulfil(order: &Order) -> HandlerOutcome {
    ///     println!("fulfilling order {}", order.id);
    ///     HandlerOutcome::ack()
    /// }
    /// #
    /// # #[ruststream::app]
    /// # fn app() -> impl App {
    /// #     RustStream::new(AppInfo::new("orders", "0.1.0"))
    /// #         .with_broker(AmqpBroker::new("amqp://localhost:5672"), |b| {
    /// #             b.include(fulfil);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn queue(name: impl Into<String>) -> Self {
        Self::of(name.into(), Kind::Queue)
    }

    /// A multicast address: fan-out, each message delivered to every subscriber.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use ruststream_amqp::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Event {
    ///     kind: String,
    /// }
    ///
    /// // Every service subscribed to "events" gets its own copy of each event.
    /// #[subscriber(AmqpAddress::topic("events"))]
    /// async fn audit(event: &Event) -> HandlerOutcome {
    ///     println!("audit: {}", event.kind);
    ///     HandlerOutcome::ack()
    /// }
    /// #
    /// # #[ruststream::app]
    /// # fn app() -> impl App {
    /// #     RustStream::new(AppInfo::new("orders", "0.1.0"))
    /// #         .with_broker(AmqpBroker::new("amqp://localhost:5672"), |b| {
    /// #             b.include(audit);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
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
    /// # mod demo {
    /// use ruststream_amqp::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// // RabbitMQ 4.x addresses a queue by its v2 path; the string reaches the broker as written.
    /// #[subscriber(AmqpAddress::raw("/queues/orders"))]
    /// async fn fulfil(order: &Order) -> HandlerOutcome {
    ///     println!("fulfilling order {}", order.id);
    ///     HandlerOutcome::ack()
    /// }
    /// #
    /// # #[ruststream::app]
    /// # fn app() -> impl App {
    /// #     RustStream::new(AppInfo::new("orders", "0.1.0"))
    /// #         .with_broker(AmqpBroker::new("amqp://localhost:5672"), |b| {
    /// #             b.include(fulfil);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn raw(address: impl Into<String>) -> Self {
        Self::of(address.into(), Kind::Raw)
    }

    /// Sets the link credit: the window of deliveries the broker may send ahead of the handler's
    /// settlements. Defaults to [`DEFAULT_CREDIT`].
    ///
    /// The subscription grants the full credit when it attaches, and grants it again each time
    /// half of it has been settled. The broker then never runs more than `credit` deliveries
    /// ahead of the last grant, but the deliveries held unsettled can outnumber the credit: with a
    /// credit of 4, the second settlement brings four more while two are still open.
    ///
    /// The count is a [`NonZeroU32`] because a subscription granted no credit receives nothing:
    /// zero is not a quieter setting but a stalled subscription, so it is unrepresentable here.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use ruststream_amqp::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// // The broker sends up to 64 orders ahead of the handler's settlements.
    /// #[subscriber(AmqpAddress::queue("orders").credit(nonzero!(64)))]
    /// async fn fulfil(order: &Order) -> HandlerOutcome {
    ///     println!("fulfilling order {}", order.id);
    ///     HandlerOutcome::ack()
    /// }
    /// #
    /// # #[ruststream::app]
    /// # fn app() -> impl App {
    /// #     RustStream::new(AppInfo::new("orders", "0.1.0"))
    /// #         .with_broker(AmqpBroker::new("amqp://localhost:5672"), |b| {
    /// #             b.include(fulfil);
    /// #         })
    /// # }
    /// # }
    /// # fn main() {}
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
    /// # mod demo {
    /// use std::time::Duration;
    ///
    /// use ruststream_amqp::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// // Up to 32 orders per call, or whatever arrived within 50 ms of the first one.
    /// #[subscriber(AmqpAddress::queue("orders").batch_wait(Duration::from_millis(50)))]
    /// async fn fulfil(orders: &[Order]) -> HandlerOutcome {
    ///     let ids: Vec<u64> = orders.iter().map(|order| order.id).collect();
    ///     println!("fulfilling orders {ids:?}");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .with_broker(AmqpBroker::new("amqp://localhost:5672"), |b| {
    ///             b.include(fulfil.batch(nonzero!(32)));
    ///         })
    /// }
    /// # }
    /// # fn main() {}
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

    /// Where a publisher on this broker reaches this subscription again.
    ///
    /// One `AMQP` 1.0 node serves both roles: a receiver attaches its source to the address, a
    /// sender its target. The address is therefore the answer for all three kinds, which is what
    /// lets the framework's deferred `retry_after` fallback work on this broker without native
    /// delayed redelivery. On a `queue` terminus the copy competes for consumers like any other
    /// message, and on a `topic` terminus every subscriber sees it: that is the terminus the
    /// declaration asked for, not a property of the retry.
    fn redelivery_target(&self) -> RedeliveryAddress {
        RedeliveryAddress::new(self.address.clone())
    }

    /// How this descriptor's terminus shares the address's traffic, for the in-process transport.
    ///
    /// `queue` competes, `topic` fans out. A `raw` address declares no capability, so on a server
    /// the peer's own configuration decides; in process there is no configuration to consult, and
    /// the transport delivers each message once rather than inventing a fan-out the deployment may
    /// not have. A service that wants the broadcast asserted says `topic`, which is also what the
    /// products needing the capability have to be told.
    #[cfg(feature = "testing")]
    pub(crate) fn routing(&self) -> Routing {
        match self.kind {
            Kind::Topic => Routing::Multicast,
            Kind::Queue | Kind::Raw => Routing::Anycast,
        }
    }

    /// The terminus capability this descriptor advertises, when one applies.
    pub(crate) fn capability(&self) -> Option<&'static str> {
        match self.kind {
            Kind::Queue => Some("queue"),
            Kind::Topic => Some("topic"),
            Kind::Raw => None,
        }
    }

    /// What this subscription tells a reader of the generated document: the node it reads, the
    /// terminus it asked for, and the link it reads over.
    ///
    /// The answer does not depend on the transport, so a document built from a test says what the
    /// deployment's does.
    #[cfg(feature = "asyncapi")]
    fn describe_channel(&self) -> Bindings {
        bindings::channel(
            &self.address,
            self.capability(),
            self.credit.get(),
            self.settle,
        )
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

/// The descriptor mounts on the manual path the way it does on the attribute: the
/// `subscriber(..)` constructor takes it as its source, unchanged.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use std::future::{Future, ready};
///
/// use ruststream_amqp::prelude::*;
/// # #[cfg(feature = "asyncapi")]
/// # use ruststream::schemars::JsonSchema;
/// use serde::Deserialize;
///
/// # #[cfg_attr(feature = "asyncapi", derive(JsonSchema))]
/// # #[cfg_attr(feature = "asyncapi", schemars(crate = "ruststream::schemars"))]
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// struct TakeOrder;
///
/// impl Handle<Order> for TakeOrder {
///     fn handle(
///         &self,
///         order: &Order,
///         _outs: &(),
///         _ctx: &mut Context<'_>,
///     ) -> impl Future<Output = Result<(), HandlerOutcome>> {
///         tracing::info!(order.id, "order taken");
///         ready(Ok(()))
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         AmqpBroker::new("amqp://localhost:5672"),
///         |b| {
///             b.include(subscriber(AmqpAddress::queue("orders"), TakeOrder).build());
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
impl IntoSource for AmqpAddress {
    type Source = Self;

    fn into_source(self) -> Self {
        self
    }
}

impl SubscriptionSource<ConnectedAmqpBroker> for AmqpAddress {
    type Subscriber = AmqpSubscriber;

    /// `AMQP` 1.0 has no delayed redelivery and no wire-level way to declare a node with a
    /// delivery limit and a dead-letter address, so a spent delivery is this process's to move:
    /// the runtime publishes the copies and this descriptor says where they land.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        self.address()
    }

    async fn subscribe(self, connected: &ConnectedAmqpBroker) -> Result<AmqpSubscriber, AmqpError> {
        connected.subscribe_address(self).await
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self) -> Bindings {
        self.describe_channel()
    }
}

impl RedeliveryAddressed<ConnectedAmqpBroker> for AmqpAddress {
    // The answer is the descriptor's own address, so nothing is asked of the connection and the
    // call needs no state machine.
    fn redelivery_address(
        &self,
        _connected: &ConnectedAmqpBroker,
    ) -> impl Future<Output = Result<RedeliveryAddress, AmqpError>> + Send {
        ready(Ok(self.redelivery_target()))
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
