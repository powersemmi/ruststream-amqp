//! The imports a service on `AMQP` 1.0 writes every time, in one glob.
//!
//! The framework's own prelude rides along, so a service file imports this one and nothing else.
//! That reads against the core's note that brokers stay explicit imports - but the reason for
//! that note is that which broker a service runs on is the one thing every service states for
//! itself, and importing *this* crate's prelude is exactly that statement. The broker-specificity
//! lives in the crate path, so re-exporting the core glob adds no ambiguity.
//!
//! What comes from here is the surface a service names: the broker, the address descriptor that
//! carries the `AMQP` options, the publish policies, and - re-exported from the framework - the
//! capability traits this broker's live forms implement.
//!
//! That last part makes the glob a capability manifest. It carries exactly the capabilities this
//! broker has, so a handler bounding `Out<impl RequestReply>` on a broker without native
//! request/reply never even receives the name, and the mistake reads as an unresolved import
//! rather than a bound that fails deeper in. Globbing two broker preludes into one file stays
//! safe: the same core item arriving through both paths unifies, and the compiler checks that.
//!
//! # Examples
//!
//! ```
//! use ruststream_amqp::prelude::*;
//!
//! // `Context` and `HandlerResult` arrive through the framework's prelude, `AmqpBroker` and
//! // `AmqpAddress` through this one.
//! async fn handle(order: &[u8], ctx: &mut Context<'_>) -> HandlerResult {
//!     let _ = (order.len(), ctx.name());
//!     HandlerResult::Ack
//! }
//!
//! let broker = AmqpBroker::new("amqp://localhost:5672");
//! let orders = AmqpAddress::queue("orders").credit(64);
//! # let _ = (handle, broker, orders, AmqpPublish);
//! ```

pub use ruststream::prelude::*;

// The capability manifest: the framework traits this broker's live forms implement, so the glob
// names a capability exactly when the broker has it - both the traits a service writes in a bound
// and the traits whose methods it calls on a value the runtime handed it. `AmqpPublisher` carries
// `RequestReply` natively, over `reply-to`, `correlation-id` and a dynamic receiver link.
//
// `Partitioned` is the one exception, though `AmqpMessage` implements it: the core also surfaces
// `partition_key` as a defaulted method on `IncomingMessage`, which this glob already carries, so
// re-exporting the capability trait would make the natural `msg.partition_key()` ambiguous (E0034)
// rather than reachable. `DescribeServer` is out for the ordinary reason - contract machinery the
// `asyncapi` feature reads off the broker, never a name a service writes.
pub use ruststream::RequestReply;

pub use crate::{AmqpAddress, AmqpBroker, AmqpPublish};

// Gated with the capability itself: without the feature there is no transactional publisher, so
// the manifest must not name the trait either.
#[cfg(feature = "transaction")]
pub use crate::AmqpTransactionalPublish;
#[cfg(feature = "transaction")]
pub use ruststream::TransactionalPublisher;

// Deliberately absent, so that what a service does import says which layer it is working at:
//
// - The `testing` module: feature-gated broker-author tooling, not user API, and a test file
//   importing it says so.
// - The live and connected forms (`AmqpPublisher`, `AmqpTxnPublisher`, `ConnectedAmqpBroker`,
//   `AmqpSubscriber`, `AmqpMessage`): the runtime hands these to a handler already paired, the
//   same reason the core's prelude leaves `OutgoingMessage` out.
// - `AmqpError`: a service names errors where it handles them.
// - `Sasl`, `Settle`, `DEFAULT_CREDIT`, `PARTITION_KEY_HEADER`: configuration and constants,
//   named at the one call site that configures them.
