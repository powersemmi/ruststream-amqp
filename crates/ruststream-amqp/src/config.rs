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
/// use ruststream_amqp::Sasl;
///
/// let sasl = Sasl::plain("svc", "secret");
/// # let _ = sasl;
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
    /// use ruststream_amqp::Sasl;
    /// let sasl = Sasl::anonymous();
    /// # let _ = sasl;
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
    /// use ruststream_amqp::Sasl;
    /// let sasl = Sasl::plain("svc", "secret");
    /// # let _ = sasl;
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
    /// use ruststream_amqp::Sasl;
    /// let sasl = Sasl::external();
    /// # let _ = sasl;
    /// ```
    pub fn external() -> Self {
        Self {
            profile: SaslProfile::External,
        }
    }
}
