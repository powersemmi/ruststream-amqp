//! In-process test support, behind the `testing` feature.
//!
//! [`AmqpTestBroker`] is a handler-stub transport that reproduces the crate's core routing in
//! memory - no server, no `AMQP` wire - and implements
//! [`TestableBroker`](ruststream::testing::TestableBroker) on its connected form, so
//! application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness. It routes by exact address match and does
//! not simulate broker-specific semantics (dead-letter policies, credit, redelivery timing);
//! those are verified end to end against a real broker.
//!
//! [`AmqpAddress`](crate::AmqpAddress) resolves against this broker as well, so a handler is
//! mounted here with the declaration it carries in production rather than a bare address string.
//! [`ConnectedAmqpTestBroker::subscribe_address`] documents which of the descriptor's options
//! keep their meaning in process and which the stand-in has nothing to honour them with.

mod broker;
mod router;
mod subscriber;

pub use broker::{AmqpTestBroker, AmqpTestPublish, AmqpTestPublisher, ConnectedAmqpTestBroker};
pub use subscriber::{AmqpTestMessage, AmqpTestSubscriber};
