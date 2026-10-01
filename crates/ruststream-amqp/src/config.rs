//! Connection configuration: SASL profiles.

use fe2o3_amqp::sasl_profile::SaslProfile;

/// How the broker authenticates the connection, mapped onto the client's SASL profiles.
///
/// Constructed with the per-mechanism constructors and passed to
/// [`AmqpBroker::sasl`](crate::AmqpBroker::sasl). A URL of the form `amqp://user:pass@host` also
/// selects PLAIN implicitly; an explicit profile set here wins.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_amqp::prelude::*;
/// # use serde::Deserialize;
/// #
/// # #[derive(Deserialize)]
/// # struct Order {
/// #     id: u64,
/// # }
/// #
/// # #[subscriber(AmqpAddress::queue("orders"))]
/// # async fn handle(order: &Order) -> HandlerOutcome {
/// #     println!("got order {}", order.id);
/// #     HandlerOutcome::ack()
/// # }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         AmqpBroker::new("amqp://broker.internal:5672").sasl(Sasl::plain("svc", "secret")),
///         |b| {
///             b.include(handle);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct Sasl {
    pub(crate) profile: SaslProfile,
}

impl Sasl {
    /// SASL ANONYMOUS: no credentials, for brokers that allow unauthenticated connections.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use ruststream_amqp::prelude::*;
    /// # use serde::Deserialize;
    /// #
    /// # #[derive(Deserialize)]
    /// # struct Order {
    /// #     id: u64,
    /// # }
    /// #
    /// # #[subscriber(AmqpAddress::queue("orders"))]
    /// # async fn handle(order: &Order) -> HandlerOutcome {
    /// #     println!("got order {}", order.id);
    /// #     HandlerOutcome::ack()
    /// # }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    ///         // The broker admits unauthenticated connections.
    ///         AmqpBroker::new("amqp://broker.internal:5672").sasl(Sasl::anonymous()),
    ///         |b| {
    ///             b.include(handle);
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn anonymous() -> Self {
        Self {
            profile: SaslProfile::Anonymous,
        }
    }

    /// SASL PLAIN: username and password.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use ruststream_amqp::prelude::*;
    /// # use serde::Deserialize;
    /// #
    /// # #[derive(Deserialize)]
    /// # struct Order {
    /// #     id: u64,
    /// # }
    /// #
    /// # #[subscriber(AmqpAddress::queue("orders"))]
    /// # async fn handle(order: &Order) -> HandlerOutcome {
    /// #     println!("got order {}", order.id);
    /// #     HandlerOutcome::ack()
    /// # }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    ///         AmqpBroker::new("amqp://broker.internal:5672").sasl(Sasl::plain("svc", "secret")),
    ///         |b| {
    ///             b.include(handle);
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn plain(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            profile: SaslProfile::Plain {
                username: username.into(),
                password: password.into(),
            },
        }
    }

    /// SASL EXTERNAL: authentication established outside SASL, typically a TLS client
    /// certificate.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use ruststream_amqp::prelude::*;
    /// # use serde::Deserialize;
    /// #
    /// # #[derive(Deserialize)]
    /// # struct Order {
    /// #     id: u64,
    /// # }
    /// #
    /// # #[subscriber(AmqpAddress::queue("orders"))]
    /// # async fn handle(order: &Order) -> HandlerOutcome {
    /// #     println!("got order {}", order.id);
    /// #     HandlerOutcome::ack()
    /// # }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    ///         // Authenticated by the TLS client certificate; no SASL credentials.
    ///         AmqpBroker::new("amqps://broker.internal:5671").sasl(Sasl::external()),
    ///         |b| {
    ///             b.include(handle);
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn external() -> Self {
        Self {
            profile: SaslProfile::External,
        }
    }
}
