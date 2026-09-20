//! [`AmqpPublisher`], its [`AmqpPublish`] policy, and native request/reply.

use std::future::{Future, ready};
use std::sync::Arc;
use std::time::Duration;

use fe2o3_amqp_types::messaging::{Message, Outcome, Properties};
#[cfg(feature = "asyncapi")]
use ruststream::asyncapi::Bindings;
use ruststream::{OutgoingFor, PairError, PublishPolicy, Publisher, RequestReply, Take};

#[cfg(feature = "asyncapi")]
use crate::bindings;

use crate::broker::{AmqpCore, ConnectedAmqpBroker, CoreCell, SenderLink};
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
}

impl std::fmt::Debug for AmqpPublisher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpPublisher").finish_non_exhaustive()
    }
}

impl AmqpPublisher {
    pub(crate) fn new(cell: CoreCell) -> Self {
        Self { cell }
    }

    fn core(&self) -> Result<&Arc<AmqpCore>, AmqpError> {
        let core = self.cell.get().ok_or(AmqpError::NotConnected)?;
        core.ensure_open()?;
        Ok(core)
    }
}

/// Sends one built message over a shared sender link and maps a non-accepted outcome to an
/// error, so a broker-side reject can never pass silently.
pub(crate) async fn send_message(
    link: &SenderLink,
    address: &str,
    message: Message<fe2o3_amqp_types::messaging::Data>,
) -> Result<(), AmqpError> {
    let outcome = link
        .with(async |sender| {
            sender.send(message).await.map_err(|e| AmqpError::Publish {
                address: address.to_owned(),
                source: box_err(e),
            })
        })
        .await?;
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
    /// The `AMQP` 1.0 body owns its bytes: the `data` section is a `Binary`, which is a vector
    /// the client keeps until the transfer is settled.
    type Payload = Take;
    type Error = AmqpError;

    /// No per-message settings. `AMQP` 1.0 does define fields that would qualify - `durable`,
    /// `priority` and `ttl` in the `header` section - but this crate publishes no `header` section
    /// at all, so there is nothing for a call site to adjust and nothing for a policy to default.
    type Options = ();

    async fn publish(
        &self,
        msg: OutgoingFor<'_, Take>,
        _options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let core = self.core()?;
        // The destination is the caller's string and outlives the message the conversion takes.
        let address = msg.name();
        let sender = core.sender_for(address).await?;
        send_message(&sender, address, to_amqp_message(msg)).await
    }
}

impl RequestReply for AmqpPublisher {
    type Reply = AmqpMessage;

    async fn request(
        &self,
        msg: OutgoingFor<'_, Take>,
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

        // The destination is the caller's string and outlives the message the conversion takes.
        let address = msg.name();
        let exchange = async {
            let mut message = to_amqp_message(msg);
            let properties = message.properties.get_or_insert_with(Properties::default);
            properties.reply_to = Some(reply_to);
            properties.correlation_id = Some(correlation_id.clone().into());

            let sender = core.sender_for(address).await?;
            send_message(&sender, address, message).await?;

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
                    // A reply is a message of its own, with no prior attempt to count.
                    return Ok(AmqpMessage::settled(payload, headers, None));
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

    /// The sender link this policy attaches puts its target on the destination the document
    /// reports, and the extension names that node address.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        bindings::target(channel)
    }

    /// A publish here waits for the peer's disposition and reports anything but `accepted` as an
    /// error, which is what a reader of the document needs to know about this operation. How the
    /// send is posted does not vary with the destination, so the name is not read here.
    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self, _channel: &str) -> Bindings {
        bindings::confirmed_posting()
    }

    /// A request made through this policy carries the `AMQP` `reply-to` property, and a handler
    /// reads it as the `reply-to` header, so a reply routed per delivery is routed from there.
    #[cfg(feature = "asyncapi")]
    fn reply_address_location(&self) -> Option<&'static str> {
        Some(bindings::REPLY_ADDRESS_LOCATION)
    }
}

/// The policy pairs on the in-process broker as well, so a routes file mounts
/// `.out_reply(Publish)` on either broker with no test-only policy standing in for this one. It
/// carries no settings, so nothing is silently dropped in the crossing; the live form differs, and
/// [`AmqpTestPublisher`](crate::testing::AmqpTestPublisher) documents what it reproduces.
#[cfg(feature = "testing")]
impl PublishPolicy<crate::testing::ConnectedAmqpTestBroker> for AmqpPublish {
    type Live = crate::testing::AmqpTestPublisher;

    fn pair(
        self,
        connected: &crate::testing::ConnectedAmqpTestBroker,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }

    /// The sender link this policy attaches puts its target on the destination the document
    /// reports, and the extension names that node address.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        bindings::target(channel)
    }

    /// A publish here waits for the peer's disposition and reports anything but `accepted` as an
    /// error, which is what a reader of the document needs to know about this operation. How the
    /// send is posted does not vary with the destination, so the name is not read here.
    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self, _channel: &str) -> Bindings {
        bindings::confirmed_posting()
    }

    /// A request made through this policy carries the `AMQP` `reply-to` property, and a handler
    /// reads it as the `reply-to` header, so a reply routed per delivery is routed from there.
    #[cfg(feature = "asyncapi")]
    fn reply_address_location(&self) -> Option<&'static str> {
        Some(bindings::REPLY_ADDRESS_LOCATION)
    }
}
