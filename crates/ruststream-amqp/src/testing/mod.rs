//! In-process test support, behind the `testing` feature.
//!
//! [`AmqpTestBroker`] is a handler-stub transport that reproduces the crate's core routing in
//! memory - no server, no `AMQP` wire - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so
//! application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness. It routes by exact address match and does
//! not simulate broker-specific semantics (dead-letter policies, credit, redelivery timing);
//! exercise those against a real broker.

mod broker;
mod router;
mod subscriber;

pub use broker::{AmqpTestBroker, AmqpTestPublish, AmqpTestPublisher, ConnectedAmqpTestBroker};
pub use subscriber::{AmqpTestMessage, AmqpTestSubscriber};
