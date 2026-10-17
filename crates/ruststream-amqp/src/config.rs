//! Connection configuration: SASL profiles.

use fe2o3_amqp::sasl_profile::SaslProfile;
use url::Url;

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

/// The SASL profile a connection opens with: the one set with
/// [`AmqpBroker::sasl`](crate::AmqpBroker::sasl), or else PLAIN from the URL's userinfo, or else
/// none, which leaves the choice to the client.
pub(crate) fn sasl_profile(explicit: Option<&Sasl>, url: &Url) -> Option<SaslProfile> {
    explicit.map_or_else(
        || SaslProfile::try_from(url).ok(),
        |sasl| Some(sasl.profile.clone()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(explicit: Option<&Sasl>, url: &str) -> Option<SaslProfile> {
        sasl_profile(explicit, &Url::parse(url).expect("the URL parses"))
    }

    fn plain(profile: Option<SaslProfile>) -> Option<(String, String)> {
        match profile {
            Some(SaslProfile::Plain { username, password }) => Some((username, password)),
            _ => None,
        }
    }

    #[test]
    fn an_explicit_profile_wins_over_the_userinfo() {
        let explicit = Sasl::plain("svc", "secret");
        let chosen = profile(Some(&explicit), "amqp://other:wrong@broker:5672");
        assert_eq!(plain(chosen), Some(("svc".to_owned(), "secret".to_owned())));

        let chosen = profile(Some(&Sasl::anonymous()), "amqp://other:wrong@broker:5672");
        assert!(
            matches!(chosen, Some(SaslProfile::Anonymous)),
            "got {chosen:?}"
        );
    }

    #[test]
    fn the_userinfo_selects_plain_without_an_explicit_profile() {
        let chosen = profile(None, "amqp://svc:secret@broker:5672");
        assert_eq!(plain(chosen), Some(("svc".to_owned(), "secret".to_owned())));
    }

    #[test]
    fn a_url_without_a_user_and_a_password_selects_nothing() {
        for url in [
            "amqp://broker:5672",
            "amqp://svc@broker:5672",
            "amqp://:secret@broker:5672",
        ] {
            let chosen = profile(None, url);
            assert!(chosen.is_none(), "{url}: got {chosen:?}");
        }
    }
}
