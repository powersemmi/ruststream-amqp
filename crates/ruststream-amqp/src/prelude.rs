//! The imports a service on `AMQP` 1.0 writes every time, in one glob.
//!
//! The broker, the address descriptor, the publish policies, the framework capability traits this
//! broker implements, and the framework's own prelude. Two broker preludes may be globbed into
//! one file; items they share unify.
//!
//! # Policy names
//!
//! The policies keep their crate-root names, [`AmqpPublish`] and (with the `transaction` feature)
//! `AmqpTransactionalPublish`. The unprefixed concept names belong to the framework's prelude:
//! `Publish` there is the slot capability a manual handler bounds its `Out` entry with, and a
//! policy exported here under that name would shadow it - silently, since an explicit re-export
//! wins over a glob - leaving that bound unwritable through this glob. The prefix therefore stays
//! on every policy, not only on the ones the framework already names.
//!
//! A policy ends in `Publish` and the capability trait of its live form ends in `Publisher`, so
//! `AmqpTransactionalPublish` is what a mount site attaches and `TransactionalPublisher` is what
//! the resulting handle implements.
//!
//! # Examples
//!
//! ```
//! use ruststream_amqp::prelude::*;
//!
//! async fn handle(order: &str, ctx: &mut Context<'_>) -> HandlerOutcome {
//!     let _ = (order.len(), ctx.name());
//!     HandlerOutcome::ack()
//! }
//!
//! let broker = AmqpBroker::new("amqp://localhost:5672");
//! let orders = AmqpAddress::queue("orders").credit(64);
//! let policy = AmqpPublish;
//! # let _ = (handle, broker, orders, policy);
//! ```

pub use ruststream::prelude::*;

// `Partitioned` is deliberately not re-exported, though `AmqpMessage` implements it: the core also
// surfaces `partition_key` as a defaulted method on `IncomingMessage`, which this glob carries, so
// adding the capability trait makes the plain `msg.partition_key()` ambiguous (E0034).
pub use ruststream::RequestReply;

pub use crate::{AmqpAddress, AmqpBroker, AmqpPublish};

#[cfg(feature = "transaction")]
pub use crate::AmqpTransactionalPublish;
#[cfg(feature = "transaction")]
pub use ruststream::TransactionalPublisher;
