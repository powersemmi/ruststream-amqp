//! [`AmqpContext`], the per-delivery context a handler reads with `Ctx<..>`, and the
//! [`Correlate`] transform that carries its correlation onto a reply.

use ruststream::runtime::{ForReply, Outgoing, PublishContext, PublishTransform, Reads};
use ruststream::{BuildContext, Bytes, ContextField, Field, IncomingMessage};

use crate::message::{AmqpMessage, ReplyId};
use crate::publish_step::{AmqpId, AmqpPublishOptions};

/// What a handler reads off one `AMQP` delivery besides its headers: built only for a handler
/// that asks for it, with a `Ctx<..>` key such as [`ReplyCorrelation`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_amqp::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Entry {
///     id: u64,
/// }
///
/// #[subscriber(AmqpAddress::queue("audit"))]
/// async fn audit(entry: &Entry, Ctx(correlation): Ctx<ReplyCorrelation>) -> HandlerOutcome {
///     tracing::info!(entry.id, "audited");
///     tracing::info!(?correlation, "audited");
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AmqpContext {
    reply_correlation: Option<AmqpId>,
}

impl AmqpContext {
    /// The id a reply to this delivery correlates to, in the type the sender wrote it: the
    /// delivery's `correlation-id`, or its `message-id` when it carries no correlation id.
    #[must_use]
    pub const fn reply_correlation(&self) -> Option<&AmqpId> {
        self.reply_correlation.as_ref()
    }
}

impl BuildContext<AmqpMessage> for AmqpContext {
    fn build(msg: &AmqpMessage) -> Self {
        let header = |name| msg.headers().get(name);
        let reply_correlation = match msg.reply_id() {
            ReplyId::None => None,
            ReplyId::Ulong(n) => Some(AmqpId::Ulong(n)),
            ReplyId::Uuid(bytes) => Some(AmqpId::Uuid(bytes)),
            // The header holds the very bytes the delivery's binary or string id carried.
            ReplyId::Binary(name) => {
                header(name).map(|b| AmqpId::Binary(Bytes::copy_from_slice(b)))
            }
            ReplyId::String(name) => header(name)
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| AmqpId::String(s.to_owned())),
        };
        Self { reply_correlation }
    }
}

/// The key reading [`AmqpContext::reply_correlation`]: `Ctx(correlation): Ctx<ReplyCorrelation>`
/// in a handler's arguments.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_amqp::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Request {
///     id: u64,
/// }
///
/// #[subscriber(AmqpAddress::queue("requests"))]
/// async fn trace(request: &Request, Ctx(correlation): Ctx<ReplyCorrelation>) -> HandlerOutcome {
///     tracing::info!(request.id, "traced");
///     if let Some(AmqpId::Ulong(n)) = correlation {
///         tracing::info!(n, "a request correlated by a number");
///     }
///     HandlerOutcome::ack()
/// }
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ReplyCorrelation;

impl Field<AmqpContext> for ReplyCorrelation {
    type Value<'a> = Option<&'a AmqpId>;

    fn get(self, src: &AmqpContext) -> Option<&AmqpId> {
        src.reply_correlation()
    }
}

impl ContextField for ReplyCorrelation {
    type Context = AmqpContext;
    type Value = Option<AmqpId>;

    fn read(self, src: &AmqpContext) -> Option<AmqpId> {
        src.reply_correlation.clone()
    }
}

/// A [`PublishTransform`] for the reply position: the reply carries the delivery's
/// [`ReplyCorrelation`] as its `correlation-id`, in the type the requester wrote it.
///
/// The correlation comes from [`AmqpContext`], which reaches the position when the handler reads
/// that context (any `Ctx<..>` key of this crate).
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream_amqp::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "receipts")]
/// struct Receipt {
///     order_id: u64,
/// }
///
/// /// Reading the correlation is what puts the context on this registration.
/// #[subscriber(AmqpAddress::queue("orders"), reply)]
/// async fn receipt(order: &Order, Ctx(_id): Ctx<ReplyCorrelation>) -> Receipt {
///     Receipt { order_id: order.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .with_broker(AmqpBroker::new("amqp://localhost:5672"), |b| {
///             b.include(receipt).out_reply(Publish).transform(Correlate);
///         })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Correlate;

impl PublishTransform<ForReply<AmqpContext>, AmqpPublishOptions> for Correlate {
    type Destination = Reads;

    fn apply(
        &self,
        _out: &mut Outgoing<'_>,
        options: &mut Option<AmqpPublishOptions>,
        cx: &PublishContext<'_, AmqpContext>,
    ) {
        if let Some(id) = cx.context(ReplyCorrelation) {
            options
                .get_or_insert_with(AmqpPublishOptions::default)
                .correlation_id = Some(id.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use fe2o3_amqp_types::messaging::{Data, Message, MessageId, Properties};
    use fe2o3_amqp_types::primitives::{Binary, Uuid};

    use super::*;
    use crate::message::{Read, read_amqp};

    /// The context a handler reads off a delivery carrying `properties`.
    fn context_of(properties: Properties) -> AmqpContext {
        let message = Message::builder()
            .properties(properties)
            .data(Data(Binary::from(Vec::new())))
            .build();
        let (headers, reply_id) = read_amqp(&message);
        let read = Read {
            payload: Bytes::new(),
            headers,
            reply_id,
        };
        AmqpContext::build(&AmqpMessage::settled(read, None))
    }

    #[test]
    fn the_reply_correlation_keeps_the_type_of_the_correlation_id() {
        let cases = [
            (MessageId::Uuid(Uuid::from([3; 16])), AmqpId::Uuid([3; 16])),
            (MessageId::Ulong(42), AmqpId::Ulong(42)),
            (
                MessageId::Binary(Binary::from(b"bin".to_vec())),
                AmqpId::Binary(Bytes::from_static(b"bin")),
            ),
            (
                MessageId::String("text".into()),
                AmqpId::String("text".into()),
            ),
        ];
        for (sent, expected) in cases {
            let properties = Properties {
                correlation_id: Some(sent),
                message_id: Some(MessageId::Ulong(1)),
                ..Properties::default()
            };
            assert_eq!(context_of(properties).reply_correlation(), Some(&expected));
        }
    }

    #[test]
    fn without_a_correlation_id_the_reply_correlates_to_the_message_id() {
        let properties = Properties {
            message_id: Some(MessageId::Uuid(Uuid::from([9; 16]))),
            ..Properties::default()
        };
        assert_eq!(
            context_of(properties).reply_correlation(),
            Some(&AmqpId::Uuid([9; 16]))
        );
        assert_eq!(context_of(Properties::default()).reply_correlation(), None);
    }
}
