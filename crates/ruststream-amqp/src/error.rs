//! The crate-level error type.

use std::error::Error as StdError;

/// Errors returned by the AMQP 1.0 broker.
///
/// One enum for the whole crate, variants by source, per the `RustStream` broker conventions. The
/// wrapped sources are boxed `std` errors so the public API does not leak `fe2o3-amqp` types.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AmqpError {
    /// Opening the connection (TCP, TLS, SASL, or the AMQP handshake) failed.
    #[error("amqp connection error: {0}")]
    Connect(#[source] Box<dyn StdError + Send + Sync>),

    /// Beginning a session on the live connection failed.
    #[error("amqp session error: {0}")]
    Session(#[source] Box<dyn StdError + Send + Sync>),

    /// Attaching a link (sender or receiver) failed.
    #[error("amqp link attach error for '{address}': {source}")]
    Attach {
        /// The address the link was attached to.
        address: String,
        /// The client's attach failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// Closing a link during shutdown failed, so the peer may still consider it attached.
    #[error("amqp link close error for '{address}': {source}")]
    Detach {
        /// The address the link was attached to.
        address: String,
        /// The client's detach failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The transport failed while sending a message.
    #[error("amqp publish error to '{address}': {source}")]
    Publish {
        /// The address the message was published to.
        address: String,
        /// The client's send failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The peer settled an outgoing message with a non-accepted outcome (rejected, released, or
    /// modified), so the message is not on the broker.
    #[error("amqp publish to '{address}' not accepted: {outcome}")]
    PublishNotAccepted {
        /// The address the message was published to.
        address: String,
        /// A description of the peer's outcome, including the error condition when one was
        /// carried.
        outcome: String,
    },

    /// The transport failed while receiving a message.
    #[error("amqp receive error on '{address}': {source}")]
    Receive {
        /// The source address of the subscription.
        address: String,
        /// The client's receive failure.
        #[source]
        source: Box<dyn StdError + Send + Sync>,
    },

    /// A delivery arrived whose body the crate cannot expose as bytes (an `AMQP` value section
    /// that is neither binary nor a string).
    #[error("amqp delivery on '{address}' has an unsupported body section")]
    UnsupportedBody {
        /// The source address of the subscription.
        address: String,
    },

    /// A request/reply round trip did not produce a reply within the caller's timeout.
    #[error("amqp request timed out")]
    RequestTimeout,

    /// The handle is used before `connect` filled the shared connection, or after `shutdown`.
    #[error("amqp broker is not connected")]
    NotConnected,

    /// A subscription descriptor is invalid.
    #[error("invalid amqp address: {0}")]
    InvalidAddress(String),

    /// A transaction operation was invoked in a state that cannot serve it.
    #[cfg(feature = "transaction")]
    #[error("amqp transaction error: {0}")]
    Transaction(String),
}

/// Boxes a client error into the crate's `Box<dyn StdError>` source form.
pub(crate) fn box_err<E>(err: E) -> Box<dyn StdError + Send + Sync>
where
    E: StdError + Send + Sync + 'static,
{
    Box::new(err)
}
