#![doc = include_str!("README.md")]
#![forbid(unsafe_code)]

mod address;
#[cfg(feature = "asyncapi")]
mod bindings;
mod broker;
mod config;
mod error;
#[cfg(feature = "testing")]
mod in_process;
mod message;
pub mod prelude;
mod publisher;
mod subscriber;
#[cfg(feature = "transaction")]
mod txn;

pub use address::{AmqpAddress, DEFAULT_BATCH_WAIT, DEFAULT_CREDIT, Settle};
pub use broker::{AmqpBroker, ConnectedAmqpBroker};
pub use config::Sasl;
pub use error::AmqpError;
pub use message::{AmqpMessage, PARTITION_KEY_HEADER};
pub use publisher::{AmqpPublish, AmqpPublisher};
pub use subscriber::AmqpSubscriber;
#[cfg(feature = "transaction")]
pub use txn::{AmqpTransactionalPublish, AmqpTxnPublisher};
