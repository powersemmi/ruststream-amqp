//! `AMQP` 1.0 broker implementation for `RustStream`.
//!
//! One protocol crate for the whole `AMQP` 1.0 family: `ActiveMQ` Artemis, `RabbitMQ` 4.x (its
//! `AMQP` 1.0 stack, distinct from the 0.9.1 protocol `ruststream-lapin` speaks), Azure Service
//! Bus, and every other broker speaking the ISO-standard protocol. Handlers, routers, codecs,
//! and middleware come from the framework; this crate supplies the transport over
//! [`fe2o3-amqp`](https://docs.rs/fe2o3-amqp).
//!
//! - Acknowledgement maps to protocol dispositions: `ack` accepts, `nack(requeue = true)`
//!   releases, `nack(requeue = false)` rejects (the broker's dead-letter policy applies).
//! - Request/reply is native: `reply-to`, `correlation-id`, and a dynamic receiver link.
//! - Headers ride `application-properties` (well-known ones the `properties` section), so no
//!   envelope format is invented and non-Rust peers see plain `AMQP` messages.
//! - Back-pressure is the protocol's own credit-based flow control, surfaced as the
//!   [`AmqpAddress::credit`] prefetch.
//! - Pages are assembled on the client: a transfer carries one message, so a page handler gets
//!   the size it named and [`AmqpAddress::page_wait`] decides how long a partial page waits.

#![forbid(unsafe_code)]

mod address;
mod broker;
mod config;
mod error;
mod message;
pub mod prelude;
mod publisher;
mod subscriber;
#[cfg(feature = "testing")]
pub mod testing;
#[cfg(feature = "transaction")]
mod txn;

pub use address::{AmqpAddress, DEFAULT_CREDIT, DEFAULT_PAGE_WAIT, Settle};
pub use broker::{AmqpBroker, ConnectedAmqpBroker};
pub use config::Sasl;
pub use error::AmqpError;
pub use message::{AmqpMessage, PARTITION_KEY_HEADER};
pub use publisher::{AmqpPublish, AmqpPublisher};
pub use subscriber::AmqpSubscriber;
#[cfg(feature = "transaction")]
pub use txn::{AmqpTransactionalPublish, AmqpTxnPublisher};
