//! The imports a service on `AMQP` 1.0 writes every time, in one glob.
//!
//! The broker and its authentication profile, the address descriptor and its delivery guarantee,
//! the crate's error, the publish policies, the capability traits [`RequestReply`] and (with the
//! `transaction` feature) `TransactionalPublisher`, and the framework's own prelude.
//! [`Partitioned`](ruststream::Partitioned) stays out, though [`AmqpMessage`](crate::AmqpMessage)
//! implements it: in scope it makes `msg.partition_key()` ambiguous with the method of the same
//! name on `IncomingMessage`, so a service that reads partition keys imports it itself. Two broker
//! preludes may be globbed into one file; items they share unify.
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
//! A mount site therefore reads `b.include(handler).out_reply(Publish)` on every broker, and
//! moving a service between brokers is a change of one import rather than of every include site.
//! The two vocabularies never share a name: a policy ends in `Publish`, and the capability trait
//! of its live form ends in `Publisher`. The prefixed originals stay exported here as well, for a
//! file that globs two broker preludes and has to say which `Publish` it means.
//!
//! # Examples
//!
//! A routes file whose every name comes from the glob:
//!
//! ```
//! # mod demo {
//! use ruststream_amqp::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Deserialize)]
//! struct Order {
//!     id: u64,
//! }
//!
//! #[derive(Deserialize)]
//! struct Event {
//!     kind: String,
//! }
//!
//! #[derive(Serialize, Outgoing)]
//! #[outgoing(name = "receipts")]
//! struct Receipt {
//!     order_id: u64,
//! }
//!
//! #[subscriber(AmqpAddress::queue("orders").credit(nonzero!(64)), reply)]
//! async fn issue_receipt(order: &Order) -> Receipt {
//!     Receipt { order_id: order.id }
//! }
//!
//! #[subscriber(AmqpAddress::topic("events").settle(Settle::AtMostOnce))]
//! async fn audit(event: &Event, ctx: &mut Context<'_>) -> HandlerOutcome {
//!     println!("{}: {}", ctx.name(), event.kind);
//!     HandlerOutcome::ack()
//! }
//!
//! #[ruststream::app]
//! fn app() -> impl App {
//!     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
//!         AmqpBroker::new("amqp://localhost:5672").sasl(Sasl::plain("svc", "secret")),
//!         |b| {
//!             b.include(issue_receipt).out_reply(Publish);
//!             b.include(audit);
//!         },
//!     )
//! }
//! # }
//! # fn main() {}
//! ```

pub use ruststream::prelude::*;

// `Partitioned` is deliberately not re-exported, though `AmqpMessage` implements it: the core also
// surfaces `partition_key` as a defaulted method on `IncomingMessage`, which this glob carries, so
// adding the capability trait makes the plain `msg.partition_key()` ambiguous (E0034).
pub use ruststream::RequestReply;

// The broker's own surface a service names while it builds the app and declares its
// subscriptions. The publisher, the message and the header constant stay explicit imports: a
// service that names them has left the broker-agnostic path.
pub use crate::{
    AmqpAddress, AmqpBroker, AmqpError, AmqpPublish, AmqpPublish as Publish, Sasl, Settle,
};

#[cfg(feature = "transaction")]
pub use crate::{AmqpTransactionalPublish, AmqpTransactionalPublish as TransactionalPublish};
#[cfg(feature = "transaction")]
pub use ruststream::TransactionalPublisher;
