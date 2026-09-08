//! In-process test support, behind the `testing` feature.
//!
//! [`AmqpTestBroker`] is a transport that reproduces this crate's behaviour in memory - no server,
//! no `AMQP` wire - and implements [`TestableBroker`](ruststream::testing::TestableBroker) on its
//! connected form, so application handlers can be unit-tested with the
//! [`TestApp`](ruststream::testing::TestApp) harness.
//!
//! The whole production declaration resolves against it, so a service is tested as the wiring it
//! ships rather than a rewritten copy of it: [`AmqpAddress`](crate::AmqpAddress) opens
//! subscriptions here, and the publish policies [`AmqpPublish`](crate::AmqpPublish) and
//! [`AmqpTransactionalPublish`](crate::AmqpTransactionalPublish) pair into the in-process
//! publishers below, with their capabilities - request/reply and transactional posting - carried
//! over rather than missing. There is no test-only policy type to swap in at the mount site.
//!
//! # What it does not reproduce
//!
//! Everything the emulation can hold exactly it holds, including the ones that decide whether a
//! test means anything: the queue/topic terminus (competing consumers versus a copy each), the
//! settle mode, batching, transaction visibility, and reply correlation. What is left is what a
//! broker holds and a process cannot, and each of these makes an assertion unsound rather than
//! merely imprecise, so it belongs in the live suite (`just test-brokers`) instead:
//!
//! - **No storage.** A message published to an address with no live subscription is recorded in
//!   the publish log and dropped; a server would hold it until a consumer attaches. Open the
//!   subscriptions first.
//! - **No broker-side redelivery.** A released delivery returns to the subscription that had it,
//!   never to a competing consumer, and there is no dead-letter policy behind
//!   `nack(requeue = false)` - `ack` and a terminal `nack` differ only in name here.
//! - **No durability.** A committed transaction is atomic in the sense a handler can observe
//!   (nothing before the commit, the whole buffer after it), but the buffer lives in this process:
//!   nothing survives a crash, and no broker-side transaction timeout or fencing exists.
//! - **No flow control.** [`credit`](crate::AmqpAddress::credit) has no counterpart, because
//!   holding messages back the way a link does would make the router a broker-side queue and no
//!   handler would observe the difference. Subscriptions here are unbounded.
//! - **No refusal.** A request sent where nothing consumes it is not rejected or dead-lettered as
//!   a server may reject it; it simply times out.

mod broker;
mod publisher;
mod router;
mod subscriber;

pub use broker::{AmqpTestBroker, ConnectedAmqpTestBroker};
pub use publisher::AmqpTestPublisher;
#[cfg(feature = "transaction")]
pub use publisher::AmqpTestTxnPublisher;
pub use subscriber::{AmqpTestMessage, AmqpTestSubscriber};
