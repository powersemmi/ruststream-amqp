//! Transactional publishing over `AMQP` 1.0 transactional posting, behind the `transaction`
//! feature.
//!
//! The scope is deliberate: the client's transactional *posting* path is the one its author
//! documents as tested, so this module implements the borrowed
//! [`TransactionalPublisher`] kind (one broker-side transaction per handle) and leaves
//! transactional retirement (acks) and acquisition out.

// Without the `testing` feature a connection link has one variant, so a `match` on it has a
// single arm; the matches stay so that the in-process arm has its place when the feature is on.
#![cfg_attr(
    not(feature = "testing"),
    allow(clippy::infallible_destructuring_match)
)]

use std::fmt;
use std::future::{Future, ready};
use std::sync::Arc;

use fe2o3_amqp::transaction::{
    Controller, OwnedTransaction, TransactionDischarge, TransactionPosting,
};
use fe2o3_amqp_types::definitions::SenderSettleMode;
use fe2o3_amqp_types::transaction::Coordinator;
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{OutgoingFor, PairError, PublishPolicy, Publisher, Take, TransactionalPublisher};

#[cfg(feature = "asyncapi")]
use crate::bindings;
use tokio::sync::Mutex;

use crate::broker::{AmqpCore, ConnectedAmqpBroker, Link};
use crate::error::{AmqpError, box_err};
#[cfg(feature = "testing")]
use crate::in_process::TxnBuffer;
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

    fn pair(
        self,
        connected: &ConnectedAmqpBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.transactional_publisher()))
    }

    /// The sender link this policy attaches puts its target on the destination the document
    /// reports, and the extension names that node address.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        bindings::target(channel)
    }

    /// A send here is posted under a broker-side transaction and becomes visible on the commit,
    /// which is the one thing this operation does differently from a plain publish. How the send
    /// is posted does not vary with the destination, so the name is not read here.
    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self, _channel: &str) -> Bindings {
        bindings::transactional_posting()
    }

    #[cfg(feature = "asyncapi")]
    fn reply_address_location(&self) -> Option<&'static str> {
        Some(bindings::REPLY_ADDRESS_LOCATION)
    }
}

impl ConnectedAmqpBroker {
    /// A transactional publisher from the connected form; synchronous, the transaction is
    /// declared by `begin_transaction`.
    #[must_use]
    pub fn transactional_publisher(&self) -> AmqpTxnPublisher {
        let transport = match &self.link {
            Link::Amqp(core) => Transport::Amqp(AmqpTxn {
                core: Arc::clone(core),
                txn: Mutex::new(None),
            }),
            #[cfg(feature = "testing")]
            Link::InProcess(bus) => Transport::InProcess(TxnBuffer::new(Arc::clone(bus))),
        };
        AmqpTxnPublisher { transport }
    }
}

/// A publisher whose messages between `begin_transaction` and `commit` become visible
/// atomically, over the protocol's transactional state.
///
/// This is the borrowed transaction kind: the handle carries at most one broker-side
/// transaction, a second `begin_transaction` while one is open errors, and `commit`/`abort`
/// with no open transaction error - never a silent no-op.
pub struct AmqpTxnPublisher {
    transport: Transport,
}

/// What a transactional publisher posts over: a controller on the live connection, or, under the
/// `testing` feature, the in-process transport's buffer.
///
/// Without the feature there is one variant, so the type is the live state itself.
// The live state is the larger variant on purpose: it is the one a production build has, and
// boxing it would put an allocation on the service's own path to shrink a test build.
#[cfg_attr(feature = "testing", allow(clippy::large_enum_variant))]
enum Transport {
    Amqp(AmqpTxn),
    #[cfg(feature = "testing")]
    InProcess(TxnBuffer),
}

#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Transport>() == size_of::<AmqpTxn>());

/// The live state: the connection, and the broker-side transaction open on it.
struct AmqpTxn {
    core: Arc<AmqpCore>,
    txn: Mutex<Option<OwnedTransaction>>,
}

impl fmt::Debug for AmqpTxnPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmqpTxnPublisher").finish_non_exhaustive()
    }
}

impl Publisher for AmqpTxnPublisher {
    /// The `AMQP` 1.0 body owns its bytes: the `data` section is a `Binary`, which is a vector
    /// the client keeps until the transfer is settled.
    type Payload = Take;
    type Error = AmqpError;

    /// The same empty settings as the plain publisher: a transactional post carries the message
    /// and the transactional state, and this crate adds no `header` section to either.
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingFor<'_, Take>,
        _options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let live = match &self.transport {
            Transport::Amqp(live) => live,
            #[cfg(feature = "testing")]
            Transport::InProcess(buffer) => return buffer.publish(msg),
        };
        live.core.ensure_open()?;
        // The destination is the caller's string and outlives the message the conversion takes.
        let address = msg.name();
        let sender = live.core.sender_for(address).await?;
        let message = to_amqp_message(msg);

        let txn = live.txn.lock().await;
        if let Some(txn) = txn.as_ref() {
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
            accepted(outcome, address)
        } else {
            drop(txn);
            crate::publisher::send_message(&sender, address, message).await
        }
    }
}

impl TransactionalPublisher for AmqpTxnPublisher {
    // The slot guard intentionally spans the declare so two begins cannot race an open slot.
    #[allow(clippy::significant_drop_tightening)]
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        let live = match &self.transport {
            Transport::Amqp(live) => live,
            #[cfg(feature = "testing")]
            Transport::InProcess(buffer) => return buffer.begin(),
        };
        live.core.ensure_open()?;
        let mut slot = live.txn.lock().await;
        if slot.is_some() {
            // A rejected begin leaves the open transaction untouched, per the trait contract.
            return Err(AmqpError::Transaction(
                "a transaction is already open on this publisher".into(),
            ));
        }
        let txn = {
            let mut session = live.core.session.lock().await;
            // The control link is attached with snd-settle-mode Mixed rather than through
            // `OwnedTransaction::declare`, which hardcodes Unsettled: the client requires the
            // broker to echo the mode verbatim, and ActiveMQ Artemis answers Mixed. Declare and
            // discharge transfers are still sent unsettled per transfer, as the spec requires.
            let controller = Controller::builder()
                .name(live.core.link_name("txn"))
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
        let live = match &self.transport {
            Transport::Amqp(live) => live,
            #[cfg(feature = "testing")]
            Transport::InProcess(buffer) => return buffer.commit(),
        };
        let txn = live.txn.lock().await.take().ok_or_else(|| {
            AmqpError::Transaction("no transaction is open on this publisher".into())
        })?;
        // A failed discharge has still consumed the transaction: the slot stays empty and the
        // next begin_transaction starts fresh, so the handle never wedges.
        txn.commit()
            .await
            .map_err(|e| AmqpError::Transaction(e.to_string()))
    }

    async fn abort(&self) -> Result<(), Self::Error> {
        let live = match &self.transport {
            Transport::Amqp(live) => live,
            #[cfg(feature = "testing")]
            Transport::InProcess(buffer) => return buffer.abort(),
        };
        let txn = live.txn.lock().await.take().ok_or_else(|| {
            AmqpError::Transaction("no transaction is open on this publisher".into())
        })?;
        txn.rollback()
            .await
            .map_err(|e| AmqpError::Transaction(e.to_string()))
    }
}
