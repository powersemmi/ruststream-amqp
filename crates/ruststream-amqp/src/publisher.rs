//! [`AmqpPublisher`], its [`AmqpPublish`] policy, and native request/reply.

use std::collections::HashMap;
use std::future::{Future, ready};
use std::sync::Arc;
use std::time::Duration;

use fe2o3_amqp::Sender;
use fe2o3_amqp_types::messaging::{Message, Outcome, Properties};
use ruststream::{OutgoingMessage, PairError, PublishPolicy, Publisher, RequestReply};
use tokio::sync::Mutex;

use crate::broker::{AmqpCore, ConnectedAmqpBroker, CoreCell};
use crate::error::{AmqpError, box_err};
use crate::message::{AmqpMessage, headers_from_amqp, payload_from_body, to_amqp_message};

/// Publishes messages to `AMQP` addresses, one sender link per address, attached lazily on the
/// shared publisher session.
///
/// Buildable before `connect` (it resolves the connection through the broker's shared cell) and
/// usable until `shutdown`; afterwards every publish reports
/// [`AmqpError::NotConnected`] instead of silently succeeding against a dead connection.
#[derive(Clone)]
pub struct AmqpPublisher {
    cell: CoreCell,
    senders: Arc<Mutex<HashMap<String, Arc<Mutex<Sender>>>>>,
}

impl std::fmt::Debug for AmqpPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpPublisher").finish_non_exhaustive()
    }
}

impl AmqpPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self {
            cell,
            senders: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn core(&self) -> Result<&Arc<AmqpCore>, AmqpError> {
        let core = self.cell.get().ok_or(AmqpError::NotConnected)?;
        core.ensure_open()?;
        Ok(core)
    }

    /// The sender link for `address`, attached on first use and cached.
    // The map guard intentionally spans the attach so two callers cannot race a double-attach
    // for the same address.
    #[allow(clippy::significant_drop_tightening)]
    async fn sender_for(
        &self,
        core: &AmqpCore,
        address: &str,
    ) -> Result<Arc<Mutex<Sender>>, AmqpError> {
        let mut senders = self.senders.lock().await;
        if let Some(sender) = senders.get(address) {
            return Ok(Arc::clone(sender));
        }
        let sender = ConnectedAmqpBroker::attach_sender(core, address).await?;
        let sender = Arc::new(Mutex::new(sender));
        senders.insert(address.to_owned(), Arc::clone(&sender));
        Ok(sender)
    }
}

/// Sends one built message over a cached sender link and maps a non-accepted outcome to an
/// error, so a broker-side reject can never pass silently.
pub(crate) async fn send_message(
    sender: &Mutex<Sender>,
    address: &str,
    message: Message<fe2o3_amqp_types::messaging::Data>,
) -> Result<(), AmqpError> {
    let outcome = {
        let mut sender = sender.lock().await;
        sender.send(message).await.map_err(|e| AmqpError::Publish {
            address: address.to_owned(),
            source: box_err(e),
        })?
    };
    accepted(outcome, address)
}

pub(crate) fn accepted(outcome: Outcome, address: &str) -> Result<(), AmqpError> {
    outcome
        .accepted_or_else(|outcome| AmqpError::PublishNotAccepted {
            address: address.to_owned(),
            outcome: format!("{outcome:?}"),
        })
        .map(|_| ())
}

impl Publisher for AmqpPublisher {
    type Error = AmqpError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let core = self.core()?;
        let sender = self.sender_for(core, msg.name()).await?;
        send_message(&sender, msg.name(), to_amqp_message(&msg)).await
    }
}

impl RequestReply for AmqpPublisher {
    type Reply = AmqpMessage;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let core = self.core()?;

        // A dynamic receiver per request: the broker names a private reply address that lives
        // as long as the link. Simple and correct; a shared reply link is a later optimisation.
        let mut receiver = ConnectedAmqpBroker::attach_dynamic_receiver(core).await?;
        let reply_to = receiver
            .source()
            .as_ref()
            .and_then(|source| source.address.clone())
            .ok_or_else(|| AmqpError::Attach {
                address: "(dynamic)".to_owned(),
                source: Box::from("the broker did not assign a dynamic reply address"),
            })?;
        let correlation_id = core.correlation_id();

        let exchange = async {
            let mut message = to_amqp_message(&msg);
            let properties = message.properties.get_or_insert_with(Properties::default);
            properties.reply_to = Some(reply_to);
            properties.correlation_id = Some(correlation_id.clone().into());

            let sender = self.sender_for(core, msg.name()).await?;
            send_message(&sender, msg.name(), message).await?;

            loop {
                let delivery = receiver
                    .recv::<fe2o3_amqp_types::messaging::Body<fe2o3_amqp_types::primitives::Value>>(
                    )
                    .await
                    .map_err(|e| AmqpError::Receive {
                        address: "(dynamic)".to_owned(),
                        source: box_err(e),
                    })?;
                let message = delivery.into_message();
                let headers = headers_from_amqp(&message);
                // The private reply address makes foreign traffic unlikely, but correlate
                // anyway: a late reply to an earlier request must not resolve this one.
                if headers.correlation_id() == Some(correlation_id.as_str()) {
                    let payload = payload_from_body(message.body, "(dynamic)")?;
                    return Ok(AmqpMessage::settled(payload, headers));
                }
            }
        };

        let result = tokio::time::timeout(timeout, exchange)
            .await
            .unwrap_or(Err(AmqpError::RequestTimeout));
        if let Err((_, err)) = receiver.detach().await {
            tracing::debug!(error = %err, "amqp reply receiver detach failed");
        }
        result
    }
}

/// The publish policy for [`AmqpPublisher`]: pure declaration, constructible anywhere, paired
/// with the connected broker by the runtime after `connect`.
///
/// # Examples
///
/// ```
/// use ruststream_amqp::AmqpPublish;
///
/// let policy = AmqpPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct AmqpPublish;

impl PublishPolicy<ConnectedAmqpBroker> for AmqpPublish {
    type Live = AmqpPublisher;

    fn pair(
        self,
        connected: &ConnectedAmqpBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}
