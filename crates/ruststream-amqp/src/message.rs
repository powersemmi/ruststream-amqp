//! [`AmqpMessage`] and the mapping between `RustStream` headers and `AMQP` 1.0 message sections.
//!
//! Well-known `RustStream` headers ride the `AMQP` `properties` section (`content-type`,
//! `correlation-id`, `reply-to`, `message-id`, and the partition key as `group-id`); every other
//! header rides `application-properties`, so no envelope format is invented and non-RustStream
//! peers see plain `AMQP` messages.

use bytes::Bytes;
use fe2o3_amqp::link::delivery::DeliveryInfo;
use fe2o3_amqp_types::messaging::{
    ApplicationProperties, Body, Data, Message, MessageId, Properties,
};
use fe2o3_amqp_types::primitives::{Binary, SimpleValue, Symbol, Value};
use ruststream::{AckError, Headers, IncomingMessage, OutgoingMessage, Partitioned};
use tokio::sync::{mpsc, oneshot};

use crate::error::AmqpError;

/// Header carrying the partition key, mapped onto the `AMQP` `group-id` property.
///
/// Mirrors the in-memory broker's convention, so services can switch brokers without changing
/// their headers.
pub const PARTITION_KEY_HEADER: &str = "partition-key";

/// How a delivered message asks its pump task to settle it.
#[derive(Debug)]
pub(crate) enum SettleKind {
    /// Accept the delivery (ack).
    Accept,
    /// Release the delivery back to the broker for redelivery (nack with requeue).
    Release,
    /// Reject the delivery as undeliverable (nack without requeue); the broker's dead-letter
    /// policy decides what happens next.
    Reject,
}

/// A settlement request shipped from a message handle to the subscription's pump task.
#[derive(Debug)]
pub(crate) struct SettleCmd {
    pub(crate) info: DeliveryInfo,
    pub(crate) kind: SettleKind,
    pub(crate) done: oneshot::Sender<Result<(), AckError>>,
}

pub(crate) type SettleSender = mpsc::UnboundedSender<SettleCmd>;

/// A message delivered by an [`AmqpSubscriber`](crate::AmqpSubscriber).
///
/// `ack` maps to the `accept` disposition, `nack(requeue = true)` to `release`, and
/// `nack(requeue = false)` to `reject` (terminal; the broker's dead-letter policy applies).
/// Deliveries received on an at-most-once subscription are already settled, so `ack`/`nack`
/// report [`AckError::Unsupported`] instead of pretending.
pub struct AmqpMessage {
    payload: Bytes,
    headers: Headers,
    /// `None` when the delivery is already settled (at-most-once, request/reply replies).
    settle: Option<SettleHandle>,
}

struct SettleHandle {
    tx: SettleSender,
    info: DeliveryInfo,
}

impl std::fmt::Debug for AmqpMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpMessage")
            .field("payload_len", &self.payload.len())
            .field("settled", &self.settle.is_none())
            .finish_non_exhaustive()
    }
}

impl AmqpMessage {
    pub(crate) fn unsettled(
        payload: Bytes,
        headers: Headers,
        tx: SettleSender,
        info: DeliveryInfo,
    ) -> Self {
        Self {
            payload,
            headers,
            settle: Some(SettleHandle { tx, info }),
        }
    }

    pub(crate) fn settled(payload: Bytes, headers: Headers) -> Self {
        Self {
            payload,
            headers,
            settle: None,
        }
    }

    async fn settle(self, kind: SettleKind) -> Result<(), AckError> {
        let Some(SettleHandle { tx, info }) = self.settle else {
            return Err(AckError::Unsupported);
        };
        let (done, wait) = oneshot::channel();
        tx.send(SettleCmd { info, kind, done }).map_err(|_| {
            AckError::Broker(Box::from("the subscription's pump task has shut down"))
        })?;
        wait.await.map_err(|_| {
            AckError::Broker(Box::from("the subscription's pump task has shut down"))
        })?
    }
}

impl Partitioned for AmqpMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers.get(PARTITION_KEY_HEADER)
    }
}

impl IncomingMessage for AmqpMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &Headers {
        &self.headers
    }

    async fn ack(self) -> Result<(), AckError> {
        self.settle(SettleKind::Accept).await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        let kind = if requeue {
            SettleKind::Release
        } else {
            SettleKind::Reject
        };
        self.settle(kind).await
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}

/// Builds the `AMQP` message for an outgoing publish.
pub(crate) fn to_amqp_message(msg: &OutgoingMessage<'_>) -> Message<Data> {
    let headers = msg.headers();
    let mut properties = Properties::default();
    let mut has_properties = false;
    let mut application: Option<ApplicationProperties> = None;

    for (name, value) in headers.iter() {
        let text = || String::from_utf8_lossy(value).into_owned();
        match name {
            "content-type" => {
                properties.content_type = Some(Symbol::from(text()));
                has_properties = true;
            }
            "correlation-id" => {
                properties.correlation_id = Some(MessageId::String(text()));
                has_properties = true;
            }
            "reply-to" => {
                properties.reply_to = Some(text());
                has_properties = true;
            }
            "message-id" => {
                properties.message_id = Some(MessageId::String(text()));
                has_properties = true;
            }
            PARTITION_KEY_HEADER => {
                properties.group_id = Some(text());
                has_properties = true;
            }
            other => {
                let simple = std::str::from_utf8(value).map_or_else(
                    |_| SimpleValue::Binary(Binary::from(value.to_vec())),
                    |s| SimpleValue::String(s.to_owned()),
                );
                application
                    .get_or_insert_with(ApplicationProperties::default)
                    .insert(other.to_owned(), simple);
            }
        }
    }

    let mut builder = Message::builder();
    if has_properties {
        builder = builder.properties(properties);
    }
    if let Some(application) = application {
        builder = builder.application_properties(application);
    }
    builder.data(Binary::from(msg.payload().to_vec())).build()
}

/// Extracts `RustStream` headers from a delivered `AMQP` message.
pub(crate) fn headers_from_amqp<B>(message: &Message<B>) -> Headers {
    let mut headers = Headers::new();
    if let Some(properties) = &message.properties {
        if let Some(content_type) = &properties.content_type {
            headers.insert("content-type", content_type.to_string());
        }
        if let Some(correlation_id) = &properties.correlation_id {
            headers.insert("correlation-id", message_id_text(correlation_id));
        }
        if let Some(reply_to) = &properties.reply_to {
            headers.insert("reply-to", reply_to.clone());
        }
        if let Some(message_id) = &properties.message_id {
            headers.insert("message-id", message_id_text(message_id));
        }
        if let Some(group_id) = &properties.group_id {
            headers.insert(PARTITION_KEY_HEADER, group_id.clone());
        }
    }
    if let Some(application) = &message.application_properties {
        for (name, value) in application.iter() {
            headers.insert(name.clone(), simple_value_bytes(value));
        }
    }
    headers
}

/// Renders any `AMQP` message-id form as text, so it survives the byte-valued header map.
fn message_id_text(id: &MessageId) -> String {
    match id {
        MessageId::String(s) => s.clone(),
        MessageId::Uuid(u) => format!("{u:x}"),
        MessageId::Ulong(n) => n.to_string(),
        MessageId::Binary(b) => String::from_utf8_lossy(b).into_owned(),
    }
}

fn simple_value_bytes(value: &SimpleValue) -> Bytes {
    match value {
        SimpleValue::String(s) => Bytes::copy_from_slice(s.as_bytes()),
        SimpleValue::Binary(b) => Bytes::copy_from_slice(b),
        SimpleValue::Symbol(s) => Bytes::copy_from_slice(s.as_str().as_bytes()),
        other => Bytes::from(format_simple_value(other)),
    }
}

/// Scalar fallback: peers may put numbers or booleans into application-properties; text is the
/// only lossless byte form the header map can carry for them.
fn format_simple_value(value: &SimpleValue) -> String {
    match value {
        SimpleValue::Bool(v) => v.to_string(),
        SimpleValue::Ubyte(v) => v.to_string(),
        SimpleValue::Ushort(v) => v.to_string(),
        SimpleValue::Uint(v) => v.to_string(),
        SimpleValue::Ulong(v) => v.to_string(),
        SimpleValue::Byte(v) => v.to_string(),
        SimpleValue::Short(v) => v.to_string(),
        SimpleValue::Int(v) => v.to_string(),
        SimpleValue::Long(v) => v.to_string(),
        SimpleValue::Float(v) => v.to_string(),
        SimpleValue::Double(v) => v.to_string(),
        SimpleValue::Char(v) => v.to_string(),
        SimpleValue::Timestamp(v) => v.milliseconds().to_string(),
        SimpleValue::Uuid(v) => format!("{v:x}"),
        other => format!("{other:?}"),
    }
}

/// Extracts the payload bytes from a delivered body.
///
/// `Data` sections are the native byte path (what this crate publishes); a string or binary
/// `AmqpValue` from a foreign peer is accepted as bytes too. Anything else has no faithful byte
/// form and is reported as [`AmqpError::UnsupportedBody`].
pub(crate) fn payload_from_body(body: Body<Value>, address: &str) -> Result<Bytes, AmqpError> {
    match body {
        Body::Data(batch) => {
            let mut chunks = batch.into_iter();
            match (chunks.next(), chunks.next()) {
                (None, _) => Ok(Bytes::new()),
                (Some(Data(first)), None) => Ok(Bytes::from(first.into_vec())),
                (Some(Data(first)), Some(Data(second))) => {
                    let mut all = first.into_vec();
                    all.extend_from_slice(&second);
                    for Data(chunk) in chunks {
                        all.extend_from_slice(&chunk);
                    }
                    Ok(Bytes::from(all))
                }
            }
        }
        Body::Value(value) => match value.0 {
            Value::Binary(b) => Ok(Bytes::from(b.into_vec())),
            Value::String(s) => Ok(Bytes::from(s.into_bytes())),
            _ => Err(AmqpError::UnsupportedBody {
                address: address.to_owned(),
            }),
        },
        Body::Empty => Ok(Bytes::new()),
        Body::Sequence(_) => Err(AmqpError::UnsupportedBody {
            address: address.to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn well_known_headers_ride_the_properties_section() {
        let mut headers = Headers::new();
        headers.insert("content-type", "application/json");
        headers.insert("correlation-id", "corr-1");
        headers.insert("reply-to", "replies");
        headers.insert("message-id", "msg-1");
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("x-custom", "value");
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);

        let message = to_amqp_message(&outgoing);
        let properties = message.properties.as_ref().expect("properties set");
        assert_eq!(
            properties.content_type.as_ref().map(Symbol::as_str),
            Some("application/json")
        );
        assert_eq!(
            properties.correlation_id,
            Some(MessageId::String("corr-1".into()))
        );
        assert_eq!(properties.reply_to.as_deref(), Some("replies"));
        assert_eq!(properties.group_id.as_deref(), Some("user-42"));
        let application = message
            .application_properties
            .as_ref()
            .expect("application properties set");
        assert_eq!(
            application.get("x-custom"),
            Some(&SimpleValue::String("value".into()))
        );
        assert!(application.get("content-type").is_none());
    }

    #[test]
    fn headers_round_trip_through_the_amqp_sections() {
        let mut headers = Headers::new();
        headers.insert("content-type", "application/json");
        headers.insert("correlation-id", "corr-1");
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("x-custom", "value");
        let outgoing =
            OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers.clone());

        let restored = headers_from_amqp(&to_amqp_message(&outgoing));
        assert_eq!(restored.get_str("content-type"), Some("application/json"));
        assert_eq!(restored.get_str("correlation-id"), Some("corr-1"));
        assert_eq!(restored.get_str(PARTITION_KEY_HEADER), Some("user-42"));
        assert_eq!(restored.get_str("x-custom"), Some("value"));
    }

    #[test]
    fn data_body_yields_payload_bytes() {
        let outgoing = OutgoingMessage::new("orders", b"payload".as_slice());
        let message = to_amqp_message(&outgoing);
        let body = Body::<Value>::Data(vec![message.body].into());
        let payload = payload_from_body(body, "orders").expect("data body decodes");
        assert_eq!(payload.as_ref(), b"payload");
    }

    #[test]
    fn string_and_binary_values_are_accepted_as_bytes() {
        let s = Body::Value(fe2o3_amqp_types::messaging::AmqpValue(Value::String(
            "hi".into(),
        )));
        assert_eq!(
            payload_from_body(s, "a").expect("string decodes").as_ref(),
            b"hi"
        );

        let b = Body::Value(fe2o3_amqp_types::messaging::AmqpValue(Value::Binary(
            Binary::from(b"raw".to_vec()),
        )));
        assert_eq!(
            payload_from_body(b, "a").expect("binary decodes").as_ref(),
            b"raw"
        );
    }

    #[test]
    fn foreign_value_bodies_are_reported_unsupported() {
        let body = Body::Value(fe2o3_amqp_types::messaging::AmqpValue(Value::Bool(true)));
        assert!(matches!(
            payload_from_body(body, "a"),
            Err(AmqpError::UnsupportedBody { .. })
        ));
    }
}
