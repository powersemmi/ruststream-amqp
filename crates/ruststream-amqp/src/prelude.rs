//! The imports a service on `AMQP` 1.0 writes every time, in one glob.
//!
//! The broker, the address descriptor, the publish policies, the framework capability traits this
//! broker implements, and the framework's own prelude. Two broker preludes may be globbed into
//! one file; items they share unify.
//!
//! # Two vocabularies
//!
//! A handler body names capabilities, and imports `ruststream::prelude::*` alone: a slot is bound
//! with the broker capability trait the body needs (`Out<impl Publisher>`,
//! `Out<impl TransactionalPublisher>`, `Out<impl RequestReply>`), and the concrete publisher
//! arrives from the mount site. A routes file names policies, and imports this glob, where they
//! arrive with the broker prefix stripped:
//!
//! | Crate root | Here |
//! |---|---|
//! | [`AmqpPublish`] | [`Publish`] |
//! | `AmqpTransactionalPublish` (feature `transaction`) | `TransactionalPublish` |
//!
//! A mount site therefore reads `b.include(handler).publisher(Publish)` on every broker, and
//! moving a service between brokers is a change of one import rather than of every include site.
//! The two vocabularies never share a name: a policy ends in `Publish`, and the capability trait
//! of its live form ends in `Publisher`. The prefixed originals stay exported here as well, for a
//! file that globs two broker preludes and has to say which `Publish` it means.
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
//! let policy = Publish;
//! # let _ = (handle, broker, orders, policy);
//! ```

pub use ruststream::prelude::*;

// `Partitioned` is deliberately not re-exported, though `AmqpMessage` implements it: the core also
// surfaces `partition_key` as a defaulted method on `IncomingMessage`, which this glob carries, so
// adding the capability trait makes the plain `msg.partition_key()` ambiguous (E0034).
pub use ruststream::RequestReply;

pub use crate::{AmqpAddress, AmqpBroker, AmqpPublish, AmqpPublish as Publish};

#[cfg(feature = "transaction")]
pub use crate::{AmqpTransactionalPublish, AmqpTransactionalPublish as TransactionalPublish};
#[cfg(feature = "transaction")]
pub use ruststream::TransactionalPublisher;
