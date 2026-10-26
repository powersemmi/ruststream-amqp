//! Per-message `AMQP` properties a publish sets apart from its headers: the typed correlation id
//! a reply carries back to its requester.
//!
//! [`AmqpPublishOptions`] is what one publish may differ from the next in, and
//! [`AmqpPublishSteps`] puts the step that sets it on the publish builder, bounded on the options
//! type, so it appears over this crate's publisher and over no other.

use fe2o3_amqp_types::messaging::MessageId;
use fe2o3_amqp_types::primitives::{Binary, Uuid};
use ruststream::Bytes;
use ruststream::runtime::{PublishBuilder, PublishSink};

/// An `AMQP` message id in the type it travels as: the four forms `message-id` and
/// `correlation-id` take.
///
/// The header map holds bytes, so a UUID or a number written by a peer reads there as text, and
/// text sent back is a string id the peer does not match. This value keeps the type: a delivery
/// reports it through [`ReplyCorrelation`](crate::ReplyCorrelation), and a reply carries it back
/// through [`AmqpPublishSteps::correlation_id`].
///
/// # Examples
///
/// A responder answers each request with the request's own id, in the type the requester wrote
/// it, so a Java or .NET peer that correlates by a UUID or a number matches the reply:
///
/// ```
/// # mod demo {
/// use ruststream_amqp::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Quote {
///     sku: String,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// struct Price {
///     cents: u64,
/// }
///
/// #[subscriber(AmqpAddress::queue("prices"))]
/// async fn price(
///     quote: &Quote,
///     ctx: &mut Context<'_, AmqpContext>,
///     Ctx(correlation): Ctx<ReplyCorrelation>,
///     Out(out): Out<impl Publisher<Options = AmqpPublishOptions>>,
/// ) -> HandlerOutcome {
///     let Some(reply_to) = ctx.headers().reply_to().map(str::to_owned) else {
///         return HandlerOutcome::drop();
///     };
///     if let Some(AmqpId::Uuid(bytes)) = &correlation {
///         tracing::debug!(?bytes, sku = %quote.sku, "a UUID-correlated request");
///     }
///     let sent = out
///         .message(&Price { cents: 1_999 })
///         .to(reply_to)
///         .correlation_id(correlation)
///         .publish()
///         .await;
///     if sent.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AmqpId {
    /// A `message-id-ulong`.
    Ulong(u64),
    /// A `message-id-uuid`, as its sixteen bytes in network order.
    Uuid([u8; 16]),
    /// A `message-id-binary`.
    Binary(Bytes),
    /// A `message-id-string`.
    String(String),
}

impl AmqpId {
    pub(crate) fn to_message_id(&self) -> MessageId {
        match self {
            Self::Ulong(n) => MessageId::Ulong(*n),
            Self::Uuid(bytes) => MessageId::Uuid(Uuid::from(*bytes)),
            Self::Binary(bytes) => MessageId::Binary(Binary::from(bytes.to_vec())),
            Self::String(s) => MessageId::String(s.clone()),
        }
    }
}

/// The per-message `AMQP` properties one publish sets: every field optional, and a field left
/// alone is what the headers say.
///
/// # Examples
///
/// A handler that answers a request names the options in its slot bound, which is what puts the
/// [`AmqpPublishSteps`] on the builder:
///
/// ```
/// # mod demo {
/// use ruststream_amqp::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Ping {
///     seq: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// struct Pong {
///     seq: u64,
/// }
///
/// #[subscriber(AmqpAddress::queue("pings"))]
/// async fn answer_ping(
///     ping: &Ping,
///     ctx: &mut Context<'_, AmqpContext>,
///     Ctx(correlation): Ctx<ReplyCorrelation>,
///     Out(out): Out<impl Publisher<Options = AmqpPublishOptions>>,
/// ) -> HandlerOutcome {
///     let Some(reply_to) = ctx.headers().reply_to().map(str::to_owned) else {
///         return HandlerOutcome::drop();
///     };
///     let pong = Pong { seq: ping.seq };
///     let builder = out.message(&pong).to(reply_to).correlation_id(correlation);
///     if builder.publish().await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct AmqpPublishOptions {
    /// The `correlation-id` property in its own type. It wins over a `correlation-id` header.
    pub correlation_id: Option<AmqpId>,
}

/// The per-message steps of a publish through this crate's publisher.
///
/// They appear on a publish builder whose slot names [`AmqpPublishOptions`]
/// (`Out<impl Publisher<Options = AmqpPublishOptions>>`), and they write the property itself,
/// never a header.
pub trait AmqpPublishSteps {
    /// Sends this one message with `correlation-id` set to `id` in its own type; `None` leaves
    /// the property to the headers.
    ///
    /// # Examples
    ///
    /// A responder echoes the request's correlation in the type the requester wrote it:
    ///
    /// ```
    /// # mod demo {
    /// use ruststream_amqp::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Command {
    ///     name: String,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// struct Ack {
    ///     ok: bool,
    /// }
    ///
    /// #[subscriber(AmqpAddress::queue("commands"))]
    /// async fn run(
    ///     command: &Command,
    ///     ctx: &mut Context<'_, AmqpContext>,
    ///     Ctx(correlation): Ctx<ReplyCorrelation>,
    ///     Out(out): Out<impl Publisher<Options = AmqpPublishOptions>>,
    /// ) -> HandlerOutcome {
    ///     let Some(reply_to) = ctx.headers().reply_to().map(str::to_owned) else {
    ///         return HandlerOutcome::drop();
    ///     };
    ///     tracing::info!(command.name, "ran");
    ///     let sent = out
    ///         .message(&Ack { ok: true })
    ///         .to(reply_to)
    ///         .correlation_id(correlation)
    ///         .publish()
    ///         .await;
    ///     if sent.is_err() {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    /// # }
    /// ```
    #[must_use]
    fn correlation_id(self, id: impl Into<Option<AmqpId>>) -> Self;
}

impl<Sink, Body, Enc, Hdrs, Dest> AmqpPublishSteps for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = AmqpPublishOptions>,
{
    fn correlation_id(mut self, id: impl Into<Option<AmqpId>>) -> Self {
        if let Some(id) = id.into() {
            self.options_mut()
                .get_or_insert_with(AmqpPublishOptions::default)
                .correlation_id = Some(id);
        }
        self
    }
}
