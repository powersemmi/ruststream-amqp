//! The owned transaction kind: every `transaction()` call on an [`AmqpTxnPublisher`] declares a
//! broker-side transaction of its own, owned by the returned value.
//!
//! `AMQP` 1.0 allows any number of transactions on one connection, each declared on a control
//! link of its own, so the workers that share a publisher each open one without colliding.

use std::fmt;
use std::sync::Arc;

use fe2o3_amqp::transaction::{OwnedTransaction, TransactionDischarge, TransactionPosting};
use ruststream::{OutgoingFor, OwnedTransactions, Take, Transaction};
use tracing::warn;

use super::{AmqpTxnPublisher, Transport, declare};
use crate::broker::AmqpCore;
use crate::error::{AmqpError, box_err};
#[cfg(feature = "testing")]
use crate::in_process::OwnedBuffer;
use crate::message::to_amqp_message;
use crate::publisher::accepted;

/// A broker-side transaction opened by [`transaction`](OwnedTransactions::transaction) on an
/// [`AmqpTxnPublisher`], owned by this value.
///
/// [`publish`](Transaction::publish) posts a message under this transaction: the server holds it
/// back until [`commit`](Transaction::commit) makes every message of the transaction visible
/// together, and [`abort`](Transaction::abort) discards them. Both settle by consuming `self`, so
/// a double commit or a publish after settling is a compile error. Any number of these are open
/// on one publisher at a time, so the workers of one subscription each open their own on the
/// publisher they share, and the publisher keeps publishing directly while they are open.
///
/// Dropping one unsettled rolls it back on the connection's runtime and logs a warning.
///
/// # Examples
///
/// Each worker of the pool opens a transaction of its own on the publisher the workers share:
///
/// ```
/// # mod demo {
/// use ruststream_amqp::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Batch {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// struct Line {
///     batch: u64,
///     line: u64,
/// }
///
/// #[subscriber(AmqpAddress::queue("batches"), workers(4))]
/// async fn post(batch: &Batch, Out(out): Out<impl OwnedTransactions>) -> HandlerOutcome {
///     let Ok(mut txn) = out.owned_transaction().await else {
///         return HandlerOutcome::retry();
///     };
///     for line in 0..3 {
///         let entry = Line { batch: batch.id, line };
///         if txn.publish("ledger", &entry).await.is_err() {
///             let _ = txn.abort().await;
///             return HandlerOutcome::retry();
///         }
///     }
///     if txn.commit().await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("ledger", "0.1.0")).with_broker(
///         AmqpBroker::new("amqp://localhost:5672"),
///         |b| {
///             b.include(post).out(DefaultSlot, TransactionalPublish).build();
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[must_use = "a transaction does nothing until settled with commit() or abort()"]
pub struct AmqpTransaction {
    /// `Some` until `commit` or `abort` consumes the value; `Drop` rolls back what it finds here.
    open: Option<Open>,
}

/// What an owned transaction posts over: a transaction declared on the live connection, or,
/// under the `testing` feature, a buffer of the in-process transport.
// The live state is the larger variant on purpose, as on the publisher's own transport: boxing it
// would put an allocation on the service's path to shrink a test build.
#[cfg_attr(feature = "testing", allow(clippy::large_enum_variant))]
enum Open {
    Amqp(Declared),
    #[cfg(feature = "testing")]
    InProcess(OwnedBuffer),
}

/// A transaction declared on the live connection, on a control link of its own.
struct Declared {
    core: Arc<AmqpCore>,
    txn: OwnedTransaction,
    /// How many messages were posted under it, for the warning a dropped one logs.
    posted: usize,
}

impl AmqpTransaction {
    /// The open state, which `commit` and `abort` take as they consume the value.
    fn into_open(mut self) -> Open {
        self.open
            .take()
            .expect("an owned transaction holds its state until commit or abort consumes it")
    }
}

impl fmt::Debug for AmqpTransaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmqpTransaction").finish_non_exhaustive()
    }
}

impl Drop for AmqpTransaction {
    fn drop(&mut self) {
        let Some(open) = self.open.take() else {
            return;
        };
        // Destructors cannot run async work, so a drop is an abort the caller never wrote, which
        // is almost always a missing commit.
        match open {
            Open::Amqp(Declared { core, txn, posted }) => {
                warn!(
                    target: "ruststream_amqp",
                    posted,
                    "owned transaction dropped without commit or abort; it is rolled back"
                );
                // The rollback is a round trip on the control link, so it runs as a task on the
                // connection's runtime instead of blocking the thread that dropped the value.
                drop(core.runtime.spawn(async move {
                    if let Err(error) = txn.rollback().await {
                        warn!(
                            target: "ruststream_amqp",
                            %error,
                            "rolling back a dropped owned transaction failed"
                        );
                    }
                }));
            }
            #[cfg(feature = "testing")]
            Open::InProcess(buffer) => warn!(
                target: "ruststream_amqp",
                posted = buffer.len(),
                "owned transaction dropped without commit or abort; it is rolled back"
            ),
        }
    }
}

impl Transaction for AmqpTransaction {
    /// The `AMQP` 1.0 body owns its bytes, as on the publisher: the `data` section is a `Binary`
    /// the client keeps until the transfer is settled.
    type Payload = Take;
    type Error = AmqpError;
    /// The publisher's own empty settings: a transactional post carries the message and the
    /// transactional state, and nothing else.
    type Options = ();

    /// Posts `msg` under this transaction; the server holds it back until the commit.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] once the broker has shut down,
    /// [`AmqpError::UnsupportedHeader`] when a header has a value its `AMQP` property cannot
    /// hold, and [`AmqpError::Publish`] or [`AmqpError::PublishNotAccepted`] when the server
    /// refuses the post. A failed post leaves the transaction open: the caller decides between another post
    /// and an abort.
    async fn publish(
        &mut self,
        msg: OutgoingFor<'_, Take>,
        _options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let open = self
            .open
            .as_mut()
            .expect("an owned transaction holds its state until commit or abort consumes it");
        let live = match open {
            Open::Amqp(live) => live,
            #[cfg(feature = "testing")]
            Open::InProcess(buffer) => return buffer.publish(msg),
        };
        live.core.ensure_open()?;
        // The destination is the caller's string and outlives the message the conversion takes.
        let address = msg.name();
        let message = to_amqp_message(msg)?;
        let sender = live.core.sender_for(address).await?;
        let txn = &live.txn;
        let outcome = sender
            .with(async |sender| {
                txn.post(sender, message)
                    .await
                    .map_err(|e| AmqpError::Publish {
                        address: address.to_owned(),
                        source: box_err(e),
                    })
            })
            .await?;
        accepted(outcome, address)?;
        live.posted += 1;
        Ok(())
    }

    /// Discharges the transaction with success: every message posted under it becomes visible.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] once the broker has shut down, and
    /// [`AmqpError::Transaction`] when the server refuses the discharge. A failed commit has
    /// still consumed the transaction: redelivery of the inputs, not resubmission of the
    /// messages, is the recovery path.
    async fn commit(self) -> Result<(), Self::Error> {
        match self.into_open() {
            Open::Amqp(Declared { core, txn, .. }) => {
                core.ensure_open()?;
                txn.commit()
                    .await
                    .map_err(|e| AmqpError::Transaction(e.to_string()))
            }
            #[cfg(feature = "testing")]
            Open::InProcess(buffer) => buffer.commit(),
        }
    }

    /// Discharges the transaction with failure: the server discards every message posted under
    /// it.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] once the broker has shut down, and
    /// [`AmqpError::Transaction`] when the server refuses the discharge.
    async fn abort(self) -> Result<(), Self::Error> {
        match self.into_open() {
            Open::Amqp(Declared { core, txn, .. }) => {
                core.ensure_open()?;
                txn.rollback()
                    .await
                    .map_err(|e| AmqpError::Transaction(e.to_string()))
            }
            #[cfg(feature = "testing")]
            Open::InProcess(buffer) => buffer.abort(),
        }
    }
}

/// Owned transactions: every [`transaction`](OwnedTransactions::transaction) call declares a
/// broker-side transaction of its own, so any number are open at once on one publisher, next to
/// (and unaffected by) the publisher's own borrowed transaction.
impl OwnedTransactions for AmqpTxnPublisher {
    type Transaction = AmqpTransaction;

    /// Declares a transaction owned by the returned value, on a control link of its own.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] once the broker has shut down, and
    /// [`AmqpError::Transaction`] when the server refuses the control link or the declare.
    async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
        let open = match &self.transport {
            Transport::Amqp(live) => {
                live.core.ensure_open()?;
                Open::Amqp(Declared {
                    core: Arc::clone(&live.core),
                    txn: declare(&live.core).await?,
                    posted: 0,
                })
            }
            #[cfg(feature = "testing")]
            Transport::InProcess(buffer) => Open::InProcess(buffer.open_owned()?),
        };
        Ok(AmqpTransaction { open: Some(open) })
    }
}
