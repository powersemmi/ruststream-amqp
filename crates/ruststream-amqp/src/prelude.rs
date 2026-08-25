//! The imports a service on `AMQP` 1.0 writes every time, in one glob.
//!
//! The broker, the address descriptor, the publish policies, the framework capability traits this
//! broker implements, and the framework's own prelude. Two broker preludes may be globbed into
//! one file; items they share unify.
//!
//! # Policy names
//!
//! The policies arrive under their concept name, with the broker prefix stripped:
//!
//! | Crate root | Here |
//! |---|---|
//! | `AmqpPublish` | [`Publish`] |
//! | `AmqpTransactionalPublish` (feature `transaction`) | [`TransactionalPublish`] |
//!
//! [`Publish`] is the publish policy, not the framework's publish builder of the same name that a
//! handler enters with `message(..)` or `raw(..)`. A policy ends in `Publish` and the capability
//! trait of its live form ends in `Publisher`, so [`TransactionalPublish`] is what a mount site
//! attaches and `TransactionalPublisher` is what the resulting handle implements.
//!
//! # Examples
//!
//! ```
//! use ruststream_amqp::prelude::*;
//!
//! async fn handle(order: &[u8], ctx: &mut Context<'_>) -> HandlerResult {
//!     let _ = (order.len(), ctx.name());
//!     HandlerResult::Ack
//! }
//!
//! let broker = AmqpBroker::new("amqp://localhost:5672");
//! let orders = AmqpAddress::queue("orders").credit(64);
//! let policy = Publish;
//! # let _ = (handle, broker, orders, policy);
//! ```

pub use ruststream::prelude::*;

// `Partitioned` is deliberately not re-exported, though `AmqpMessage` implements it: the core also
// surfaces `partition_key` as a defaulted method on `IncomingMessage`, which this glob carries, so
// adding the capability trait makes the plain `msg.partition_key()` ambiguous (E0034).
pub use ruststream::RequestReply;

pub use crate::{AmqpAddress, AmqpBroker, AmqpPublish as Publish};

#[cfg(feature = "transaction")]
pub use crate::AmqpTransactionalPublish as TransactionalPublish;
#[cfg(feature = "transaction")]
pub use ruststream::TransactionalPublisher;
