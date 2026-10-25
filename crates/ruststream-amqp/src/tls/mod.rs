//! Connection configuration: the TLS settings of an `amqps://` connection.

use std::fmt;

#[cfg(feature = "native-tls")]
use native_tls::{Certificate, Identity, TlsConnector as NativeConnector};
#[cfg(all(feature = "rustls", not(feature = "native-tls")))]
use std::sync::Arc;
#[cfg(all(feature = "rustls", not(feature = "native-tls")))]
use tokio_rustls::rustls::pki_types::pem::PemObject;
#[cfg(all(feature = "rustls", not(feature = "native-tls")))]
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
#[cfg(all(feature = "rustls", not(feature = "native-tls")))]
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use url::Url;

use crate::error::AmqpError;

mod open;

pub(crate) use open::establish;

/// The TLS connector the client opens an `amqps://` connection through. With both TLS features
/// enabled it is the `native-tls` one, as it is for the client's own default.
#[cfg(feature = "native-tls")]
pub(crate) type Connector = tokio_native_tls::TlsConnector;
#[cfg(all(feature = "rustls", not(feature = "native-tls")))]
pub(crate) type Connector = tokio_rustls::TlsConnector;

/// The TLS settings of an `amqps://` connection: the certificate authorities that verify the
/// broker, and the certificate the service presents to it.
///
/// Passed to [`AmqpBroker::tls`](crate::AmqpBroker::tls). Every setting is PEM text. The
/// certificate authorities named here are trusted beside the default ones of the TLS feature, and
/// the client certificate is what [`Sasl::external`](crate::Sasl::external) logs in with on a broker
/// that maps a certificate to a user.
///
/// The settings are read when the broker connects: a PEM that holds no certificate, or a key that
/// does not parse, fails startup with [`AmqpError::Connect`]. The key is PKCS #8 (`BEGIN PRIVATE
/// KEY`), the form both TLS features read. Neither the certificates nor the key reach the broker's
/// `Debug` output or the generated `AsyncAPI` document.
///
/// # Examples
///
/// ```
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
///     // The deployment hands the service its certificate and key as PEM text.
///     let pem = |name: &str| env::var(name).unwrap_or_default();
///     let tls = Tls::new()
///         .root_certificates(pem("AMQP_CA_PEM"))
///         .client_identity(pem("AMQP_CERT_PEM"), pem("AMQP_KEY_PEM"));
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
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
#[derive(Clone, Default)]
#[must_use]
pub struct Tls {
    roots: Vec<Vec<u8>>,
    identity: Option<ClientIdentity>,
}

/// The certificate chain and the key the service presents.
#[derive(Clone)]
struct ClientIdentity {
    chain: Vec<u8>,
    key: Vec<u8>,
}

impl Tls {
    /// TLS that verifies the broker against the default certificate authorities of the TLS
    /// feature and presents no client certificate, as an `amqps://` URL does on its own.
    ///
    /// # Examples
    ///
    /// ```
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
    ///     // The broker's certificate is signed by the company's own authority.
    ///     let ca = env::var("AMQP_CA_PEM").unwrap_or_default();
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    ///         AmqpBroker::new("amqps://broker.internal:5671")
    ///             .tls(Tls::new().root_certificates(ca))
    ///             .sasl(Sasl::plain("orders-svc", "secret")),
    ///         |b| {
    ///             b.include(handle);
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Trusts the certificate authorities in `pem`, one certificate or several, beside the
    /// default ones. Each call adds to the authorities the earlier calls named.
    ///
    /// # Examples
    ///
    /// ```
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
    ///     let ca = env::var("AMQP_CA_PEM").unwrap_or_default();
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    ///         AmqpBroker::new("amqps://broker.internal:5671")
    ///             .tls(Tls::new().root_certificates(ca))
    ///             .sasl(Sasl::plain("orders-svc", "secret")),
    ///         |b| {
    ///             b.include(handle);
    ///         },
    ///     )
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn root_certificates(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.roots.push(pem.into());
        self
    }

    /// Presents the client certificate in `chain_pem` (the service's certificate first, then any
    /// intermediates), proven by the PKCS #8 key in `key_pem`. A later call replaces it.
    ///
    /// # Examples
    ///
    /// ```
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
    ///         // The broker logs the service in by the certificate it presents.
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
    pub fn client_identity(
        mut self,
        chain_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.identity = Some(ClientIdentity {
            chain: chain_pem.into(),
            key: key_pem.into(),
        });
        self
    }

    /// The connector these settings describe.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Connect`] when a PEM holds no certificate, or when the client
    /// certificate or its key does not parse.
    #[cfg(feature = "native-tls")]
    pub(crate) fn connector(&self) -> Result<Connector, AmqpError> {
        let mut builder = NativeConnector::builder();
        for pem in &self.roots {
            let certificates =
                Certificate::stack_from_pem(pem).map_err(|e| refused("root_certificates", &e))?;
            if certificates.is_empty() {
                return Err(refused("root_certificates", &NO_CERTIFICATE));
            }
            for certificate in certificates {
                builder.add_root_certificate(certificate);
            }
        }
        if let Some(identity) = &self.identity {
            let identity = Identity::from_pkcs8(&identity.chain, &identity.key)
                .map_err(|e| refused("client_identity", &e))?;
            builder.identity(identity);
        }
        let connector = builder
            .build()
            .map_err(|e| refused("the TLS settings", &e))?;
        Ok(Connector::from(connector))
    }

    /// The connector these settings describe.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Connect`] when a PEM holds no certificate, or when the client
    /// certificate or its key does not parse.
    #[cfg(all(feature = "rustls", not(feature = "native-tls")))]
    pub(crate) fn connector(&self) -> Result<Connector, AmqpError> {
        // The client's own default trusts these, so naming an authority adds to them.
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        for pem in &self.roots {
            let certificates = certificates(pem).map_err(|e| refused("root_certificates", &e))?;
            for certificate in certificates {
                roots
                    .add(certificate)
                    .map_err(|e| refused("root_certificates", &e))?;
            }
        }
        let config = ClientConfig::builder().with_root_certificates(roots);
        let config = match &self.identity {
            Some(identity) => {
                let chain =
                    certificates(&identity.chain).map_err(|e| refused("client_identity", &e))?;
                let key = PrivateKeyDer::from_pem_slice(&identity.key)
                    .map_err(|e| refused("client_identity", &e))?;
                config
                    .with_client_auth_cert(chain, key)
                    .map_err(|e| refused("client_identity", &e))?
            }
            None => config.with_no_client_auth(),
        };
        Ok(Connector::from(Arc::new(config)))
    }
}

/// What a PEM without a single certificate in it is refused with.
const NO_CERTIFICATE: &str = "the PEM holds no certificate";

/// The refusal of the setting `setting`, naming it, so the startup error points at the call to fix.
fn refused(setting: &str, cause: &dyn fmt::Display) -> AmqpError {
    AmqpError::Connect(format!("the TLS setting {setting} is unusable: {cause}").into())
}

/// The certificates in `pem`, at least one.
#[cfg(all(feature = "rustls", not(feature = "native-tls")))]
fn certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, String> {
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    if certificates.is_empty() {
        return Err(NO_CERTIFICATE.to_owned());
    }
    Ok(certificates)
}

/// Lists what is configured and never the bytes: the key is a secret, and a broker is logged.
impl fmt::Debug for Tls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tls")
            .field("root_certificates", &self.roots.len())
            .field("client_identity", &self.identity.is_some())
            .finish()
    }
}

/// Refuses TLS settings on a URL that does not open TLS: the client would connect in the clear
/// and drop them, certificate included.
///
/// # Errors
///
/// Returns [`AmqpError::Connect`] when the scheme is not `amqps`.
pub(crate) fn check_scheme(url: &Url) -> Result<(), AmqpError> {
    if url.scheme() == "amqps" {
        return Ok(());
    }
    Err(AmqpError::Connect(
        format!(
            "TLS settings need an amqps:// URL; a {}:// connection opens no TLS",
            url.scheme()
        )
        .into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A PEM block of the given label around bytes that are no certificate and no key.
    fn garbage(label: &str) -> String {
        format!("-----BEGIN {label}-----\nAAAA\n-----END {label}-----\n")
    }

    fn refusal(tls: &Tls) -> String {
        match tls.connector() {
            Ok(_) => panic!("the TLS settings unexpectedly built a connector"),
            Err(AmqpError::Connect(reason)) => reason.to_string(),
            Err(other) => panic!("expected a connect error, got {other}"),
        }
    }

    #[test]
    fn default_settings_build_a_connector() {
        assert!(Tls::new().connector().is_ok());
    }

    #[test]
    fn a_root_pem_without_a_certificate_is_refused_naming_the_setting() {
        for pem in [String::new(), "not a pem".to_owned()] {
            let reason = refusal(&Tls::new().root_certificates(pem));
            assert!(reason.contains("root_certificates"), "{reason}");
        }
    }

    #[test]
    fn a_client_identity_that_does_not_parse_is_refused_naming_the_setting() {
        let tls = Tls::new().client_identity(garbage("CERTIFICATE"), garbage("PRIVATE KEY"));
        let reason = refusal(&tls);
        assert!(reason.contains("client_identity"), "{reason}");

        let tls = Tls::new().client_identity("", "");
        let reason = refusal(&tls);
        assert!(reason.contains("client_identity"), "{reason}");
    }

    #[test]
    fn debug_output_carries_no_key() {
        let marker = "MARKER-OF-THE-PRIVATE-KEY";
        let tls = Tls::new()
            .root_certificates("ca")
            .client_identity("chain", marker);
        let shown = format!("{tls:?}");
        assert!(!shown.contains(marker), "{shown}");
        assert!(shown.contains("client_identity: true"), "{shown}");
    }

    #[test]
    fn tls_settings_need_an_amqps_url() {
        let refused = check_scheme(&Url::parse("amqp://broker:5672").expect("parses"))
            .expect_err("an amqp:// URL opens no TLS");
        assert!(refused.to_string().contains("amqps://"), "{refused}");
        check_scheme(&Url::parse("amqps://broker:5671").expect("parses")).expect("amqps opens TLS");
    }
}
