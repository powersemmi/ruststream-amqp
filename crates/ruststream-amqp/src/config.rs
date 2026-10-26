//! Connection configuration: SASL profiles.

use std::borrow::Cow;

use fe2o3_amqp::sasl_profile::SaslProfile;
use percent_encoding::percent_decode_str;
use url::Url;

use crate::error::AmqpError;

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
/// #     tracing::info!(order.id, "accepted");
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
    /// #     tracing::info!(order.id, "accepted");
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
    /// #     tracing::info!(order.id, "accepted");
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

    /// SASL EXTERNAL: the broker takes an identity established outside SASL, the client
    /// certificate the connection presents.
    ///
    /// It needs an `amqps://` URL and the certificate set with `AmqpBroker::tls` (the `rustls` or
    /// the `native-tls` feature). Without that step the connection presents no certificate and
    /// the broker refuses the login; on an `amqp://` URL the broker refuses to start.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(any(feature = "rustls", feature = "native-tls"))]
    /// # mod demo {
    /// use std::env;
    ///
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
    /// #     tracing::info!(order.id, "accepted");
    /// #     HandlerOutcome::ack()
    /// # }
    ///
    /// #[ruststream::app]
    /// fn app() -> impl App {
    ///     let pem = |name: &str| env::var(name).unwrap_or_default();
    ///     let tls = Tls::new().client_identity(pem("AMQP_CERT_PEM"), pem("AMQP_KEY_PEM"));
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    ///         // The broker identifies the service by its certificate; no credentials travel here.
    ///         AmqpBroker::new("amqps://broker.internal:5671")
    ///             .tls(tls)
    ///             .sasl(Sasl::external()),
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
///
/// A URL carries its userinfo percent-encoded, so a password with a reserved character
/// (`p%40ss`) is decoded before it becomes a credential.
///
/// # Errors
///
/// Returns [`AmqpError::Connect`] when the decoded userinfo is not UTF-8, which a PLAIN credential
/// has to be.
pub(crate) fn sasl_profile(
    explicit: Option<&Sasl>,
    url: &Url,
) -> Result<Option<SaslProfile>, AmqpError> {
    if let Some(sasl) = explicit {
        return Ok(Some(sasl.profile.clone()));
    }
    // The client's own reading of a URL: a user and a password select PLAIN, anything less selects
    // nothing.
    let (username, Some(password)) = (url.username(), url.password()) else {
        return Ok(None);
    };
    if username.is_empty() {
        return Ok(None);
    }
    let decode = |part: &str| {
        percent_decode_str(part)
            .decode_utf8()
            .map(Cow::into_owned)
            .map_err(|_| {
                AmqpError::Connect(Box::from(
                    "the URL's userinfo is not UTF-8 once percent-decoded",
                ))
            })
    };
    Ok(Some(SaslProfile::Plain {
        username: decode(username)?,
        password: decode(password)?,
    }))
}

#[cfg(test)]
mod tests {
    use std::hash::{BuildHasher, RandomState};

    use super::*;

    fn profile(explicit: Option<&Sasl>, url: &str) -> Result<Option<SaslProfile>, AmqpError> {
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
        // Drawn afresh for every run, so no credential lives in the source.
        let password = format!("{:016x}", RandomState::new().hash_one("password"));
        let explicit = Sasl::plain("svc", password.as_str());
        let chosen = profile(Some(&explicit), "amqp://other:wrong@broker:5672").expect("valid");
        assert_eq!(plain(chosen), Some(("svc".to_owned(), password)));

        let chosen =
            profile(Some(&Sasl::anonymous()), "amqp://other:wrong@broker:5672").expect("valid");
        assert!(
            matches!(chosen, Some(SaslProfile::Anonymous)),
            "got {chosen:?}"
        );
    }

    #[test]
    fn the_userinfo_selects_plain_percent_decoded() {
        let chosen = profile(None, "amqp://svc%2Ba:p%40ss%3Aw@broker:5672").expect("valid");
        assert_eq!(
            plain(chosen),
            Some(("svc+a".to_owned(), "p@ss:w".to_owned()))
        );
    }

    #[test]
    fn a_url_without_a_user_and_a_password_selects_nothing() {
        for url in [
            "amqp://broker:5672",
            "amqp://svc@broker:5672",
            "amqp://:secret@broker:5672",
        ] {
            let chosen = profile(None, url).expect("valid");
            assert!(chosen.is_none(), "{url}: got {chosen:?}");
        }
    }

    #[test]
    fn userinfo_that_is_not_utf8_once_decoded_is_refused() {
        for url in [
            "amqp://%FF:secret@broker:5672",
            "amqp://svc:%C3@broker:5672",
        ] {
            let refused = profile(None, url).expect_err("no PLAIN credential is not UTF-8");
            assert!(
                matches!(refused, AmqpError::Connect(_)),
                "{url}: got {refused}"
            );
        }
    }
}
