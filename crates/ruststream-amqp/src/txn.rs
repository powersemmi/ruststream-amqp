//! Transactional publishing over `AMQP` 1.0 transactional posting, behind the `transaction`
//! feature.
//!
//! The scope is deliberate: the client's transactional *posting* path is the one its author
//! documents as tested, so this module implements the borrowed
//! [`TransactionalPublisher`] kind (one broker-side transaction per handle) and leaves
//! transactional retirement (acks) and acquisition out.

use std::sync::Arc;

use fe2o3_amqp::transaction::{Controller, OwnedTransaction, TransactionDischarge};
use fe2o3_amqp_types::definitions::SenderSettleMode;
use fe2o3_amqp_types::transaction::Coordinator;
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher, TransactionalPublisher};
use tokio::sync::Mutex;

use crate::broker::{AmqpCore, ConnectedAmqpBroker};
use crate::error::{AmqpError, box_err};
use crate::message::to_amqp_message;
use crate::publisher::accepted;

/// The publish policy for [`AmqpTxnPublisher`]: names the transactional publishing mode as a
/// distinct policy type, so the plain publisher carries no transactional surface at all.
///
/// # Examples
///
/// ```
/// use ruststream_amqp::AmqpTransactionalPublish;
///
/// let policy = AmqpTransactionalPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct AmqpTransactionalPublish;

impl PublishPolicy<ConnectedAmqpBroker> for AmqpTransactionalPublish {
    type Live = AmqpTxnPublisher;

    async fn pair(self, connected: &ConnectedAmqpBroker) -> Result<Self::Live, PairError> {
        Ok(connected.transactional_publisher())
    }
}

impl ConnectedAmqpBroker {
    /// A transactional publisher from the connected form; synchronous, the transaction is
    /// declared by `begin_transaction`.
    #[must_use]
    pub fn transactional_publisher(&self) -> AmqpTxnPublisher {
        AmqpTxnPublisher::new(Arc::clone(&self.core))
    }
}

/// A publisher whose messages between `begin_transaction` and `commit` become visible
/// atomically, over the protocol's transactional state.
///
/// This is the borrowed transaction kind: the handle carries at most one broker-side
/// transaction, a second `begin_transaction` while one is open errors, and `commit`/`abort`
/// with no open transaction error - never a silent no-op.
pub struct AmqpTxnPublisher {
    core: Arc<AmqpCore>,
    txn: Mutex<Option<OwnedTransaction>>,
}

impl std::fmt::Debug for AmqpTxnPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpTxnPublisher").finish_non_exhaustive()
    }
}

impl AmqpTxnPublisher {
    pub(crate) fn new(core: Arc<AmqpCore>) -> Self {
        Self {
            core,
            txn: Mutex::new(None),
        }
    }
}

impl Publisher for AmqpTxnPublisher {
    type Error = AmqpError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.core.ensure_open()?;
        let sender = self.core.sender_for(msg.name()).await?;
        let message = to_amqp_message(&msg);

        let txn = self.txn.lock().await;
        if let Some(txn) = txn.as_ref() {
            let outcome = sender
                .with(async |sender| {
                    txn.post(sender, message)
                        .await
                        .map_err(|e| AmqpError::Publish {
                            address: msg.name().to_owned(),
                            source: box_err(e),
                        })
                })
                .await?;
            accepted(outcome, msg.name())
        } else {
            drop(txn);
            crate::publisher::send_message(&sender, msg.name(), message).await
        }
    }
}

impl TransactionalPublisher for AmqpTxnPublisher {
    // The slot guard intentionally spans the declare so two begins cannot race an open slot.
    #[allow(clippy::significant_drop_tightening)]
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.core.ensure_open()?;
        let mut slot = self.txn.lock().await;
        if slot.is_some() {
            // A rejected begin leaves the open transaction untouched, per the trait contract.
            return Err(AmqpError::Transaction(
                "a transaction is already open on this publisher".into(),
            ));
        }
        let txn = {
            let mut session = self.core.session.lock().await;
            // The control link is attached with snd-settle-mode Mixed rather than through
            // `OwnedTransaction::declare`, which hardcodes Unsettled: the client requires the
            // broker to echo the mode verbatim, and ActiveMQ Artemis answers Mixed. Declare and
            // discharge transfers are still sent unsettled per transfer, as the spec requires.
            let controller = Controller::builder()
                .name(self.core.link_name("txn"))
                .coordinator(Coordinator::default())
                .sender_settle_mode(SenderSettleMode::Mixed)
                .attach(&mut session)
                .await
                .map_err(|e| AmqpError::Transaction(e.to_string()))?;
            OwnedTransaction::declare_with_controller(controller, None)
                .await
                .map_err(|e| AmqpError::Transaction(e.to_string()))?
        };
        *slot = Some(txn);
        Ok(())
    }

    async fn commit(&self) -> Result<(), Self::Error> {
        let txn = self.txn.lock().await.take().ok_or_else(|| {
            AmqpError::Transaction("no transaction is open on this publisher".into())
        })?;
        // A failed discharge has still consumed the transaction: the slot stays empty and the
        // next begin_transaction starts fresh, so the handle never wedges.
        txn.commit()
            .await
            .map_err(|e| AmqpError::Transaction(e.to_string()))
    }

    async fn abort(&self) -> Result<(), Self::Error> {
        let txn = self.txn.lock().await.take().ok_or_else(|| {
            AmqpError::Transaction("no transaction is open on this publisher".into())
        })?;
        txn.rollback()
            .await
            .map_err(|e| AmqpError::Transaction(e.to_string()))
    }
}
