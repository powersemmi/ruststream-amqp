//! [`AmqpMessage`] and the mapping between `RustStream` headers and `AMQP` 1.0 message sections.
//!
//! Well-known `RustStream` headers ride the `AMQP` `properties` section (`content-type`,
//! `correlation-id`, `reply-to`, `message-id`, and the partition key as `group-id`); every other
//! header rides `application-properties`, so no envelope format is invented and non-RustStream
//! peers see plain `AMQP` messages.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use fe2o3_amqp::link::delivery::DeliveryInfo;
use fe2o3_amqp_types::messaging::{
    ApplicationProperties, Body, Data, Message, MessageId, Properties,
};
use fe2o3_amqp_types::primitives::{Binary, SimpleValue, Symbol, Value};
use ruststream::{AckError, HeaderMap, IncomingMessage, OutgoingFor, Partitioned, Str, Take};

use crate::error::AmqpError;
#[cfg(feature = "testing")]
use crate::in_process::{Delivery, Settlement};
use crate::settle::{SettleKind, SettleLine};

/// Header carrying the partition key, mapped onto the `AMQP` `group-id` property.
///
/// Mirrors the in-memory broker's convention, so services can switch brokers without changing
/// their headers.
pub const PARTITION_KEY_HEADER: &str = "partition-key";

/// A message delivered by an [`AmqpSubscriber`](crate::AmqpSubscriber).
///
/// `ack` maps to the `accept` disposition, `nack(requeue = true)` to `modified` with
/// `delivery-failed` set (so the broker counts the attempt and redelivers), and
/// `nack(requeue = false)` to `reject` (terminal; the broker's dead-letter policy applies).
/// Deliveries received on an at-most-once subscription are already settled, so `ack`/`nack`
/// report [`AckError::Unsupported`] instead of pretending.
pub struct AmqpMessage {
    payload: Bytes,
    headers: HeaderMap,
    /// The `delivery-count` of the delivery's `header` section, or `None` where it carries no
    /// header section at all. See [`AmqpMessage::redelivery_count`](IncomingMessage::redelivery_count).
    delivery_count: Option<u32>,
    /// `None` when the delivery is already settled (at-most-once, request/reply replies).
    settle: Option<SettleHandle>,
    /// How a delivery of the in-process transport settles, in place of the settle handle a live
    /// one carries. The field is there only with the `testing` feature.
    #[cfg(feature = "testing")]
    in_process: Option<Settlement>,
}

struct SettleHandle {
    line: Arc<SettleLine>,
    info: DeliveryInfo,
}

impl fmt::Debug for AmqpMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmqpMessage")
            .field("payload_len", &self.payload.len())
            .field("delivery_count", &self.delivery_count)
            .field("settled", &self.settle.is_none())
            .finish_non_exhaustive()
    }
}

impl AmqpMessage {
    pub(crate) fn unsettled(
        payload: Bytes,
        headers: HeaderMap,
        delivery_count: Option<u32>,
        line: Arc<SettleLine>,
        info: DeliveryInfo,
    ) -> Self {
        Self {
            payload,
            headers,
            delivery_count,
            settle: Some(SettleHandle { line, info }),
            #[cfg(feature = "testing")]
            in_process: None,
        }
    }

    /// A delivery that arrived already settled: an at-most-once subscription, which still carries
    /// whatever the broker counted, and a request/reply answer, which counts nothing.
    pub(crate) fn settled(payload: Bytes, headers: HeaderMap, delivery_count: Option<u32>) -> Self {
        Self {
            payload,
            headers,
            delivery_count,
            settle: None,
            #[cfg(feature = "testing")]
            in_process: None,
        }
    }

    /// A delivery of the in-process transport, reporting what a live one reports.
    #[cfg(feature = "testing")]
    pub(crate) fn in_process(delivery: Delivery, settlement: Settlement) -> Self {
        Self::settled(delivery.payload, delivery.headers, delivery.count).settling(settlement)
    }

    /// Hands the settlement of this delivery to the in-process transport.
    #[cfg(feature = "testing")]
    pub(crate) fn settling(mut self, settlement: Settlement) -> Self {
        self.in_process = Some(settlement);
        self
    }

    async fn settle(self, kind: SettleKind) -> Result<(), AckError> {
        #[cfg(feature = "testing")]
        if let Some(settlement) = self.in_process {
            return settlement.settle(kind, self.payload, self.headers, self.delivery_count);
        }
        let Some(SettleHandle { line, info }) = self.settle else {
            return Err(AckError::Unsupported);
        };
        line.settle(info, kind).await
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

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    async fn ack(self) -> Result<(), AckError> {
        self.settle(SettleKind::Accept).await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        let kind = if requeue {
            SettleKind::Modify
        } else {
            SettleKind::Reject
        };
        self.settle(kind).await
    }

    /// The broker's own count of deliveries of this message, from the `delivery-count` field of
    /// the `AMQP` `header` section, counting this delivery.
    ///
    /// A delivery with no `header` section answers `None`, which is what a message this crate
    /// published looks like: nothing has counted an attempt for it, and the framework's
    /// retry-count header carries the attempt instead. That is the answer a deferred `retry_after`
    /// copy needs, because the copy is a new message to the broker and its `delivery-count` starts
    /// over while the framework's header does not.
    fn redelivery_count(&self) -> Option<u64> {
        self.delivery_count.map(|count| u64::from(count) + 1)
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}

/// Builds the `AMQP` message for an outgoing publish.
///
/// # Errors
///
/// [`AmqpError::UnsupportedHeader`] when a header the `properties` section carries has a value
/// its property cannot hold.
pub(crate) fn to_amqp_message(msg: OutgoingFor<'_, Take>) -> Result<Message<Data>, AmqpError> {
    let (address, payload, headers) = msg.into_parts();
    build_message(address, &headers, Vec::from(payload))
}

/// Builds the `AMQP` message carrying `body` with `headers` spread over its sections: the
/// well-known ones on `properties`, every other one on `application-properties`.
///
/// A property takes a header's bytes only in a form it holds: `reply-to` and `group-id` are
/// strings, `content-type` is a symbol, and `message-id` and `correlation-id` take binary where
/// the bytes are not UTF-8. A value no form holds fails the publish instead of being rewritten.
///
/// # Errors
///
/// [`AmqpError::UnsupportedHeader`] naming the header and `address`.
pub(crate) fn build_message(
    address: &str,
    headers: &HeaderMap,
    body: Vec<u8>,
) -> Result<Message<Data>, AmqpError> {
    let mut properties = Properties::default();
    let mut has_properties = false;
    let mut application: Option<ApplicationProperties> = None;

    for (name, value) in headers.iter() {
        let refuse = |reason| AmqpError::UnsupportedHeader {
            address: address.to_owned(),
            header: name.to_owned(),
            reason,
        };
        let text = || {
            std::str::from_utf8(value)
                .map(str::to_owned)
                .map_err(|_| refuse("is a string property and takes UTF-8 only"))
        };
        match name {
            "content-type" => {
                if !value.is_ascii() {
                    return Err(refuse("is a symbol property and takes ASCII only"));
                }
                properties.content_type = Some(Symbol::from(text()?));
                has_properties = true;
            }
            "correlation-id" => {
                properties.correlation_id = Some(message_id(value));
                has_properties = true;
            }
            "reply-to" => {
                properties.reply_to = Some(text()?);
                has_properties = true;
            }
            "message-id" => {
                properties.message_id = Some(message_id(value));
                has_properties = true;
            }
            PARTITION_KEY_HEADER => {
                properties.group_id = Some(text()?);
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
    Ok(builder.data(Binary::from(body)).build())
}

/// The `message-id` form for an identifier's bytes: a string where they are UTF-8, binary where
/// they are not, so every value travels unchanged.
fn message_id(value: &[u8]) -> MessageId {
    std::str::from_utf8(value).map_or_else(
        |_| MessageId::Binary(Binary::from(value.to_vec())),
        |s| MessageId::String(s.to_owned()),
    )
}

/// Extracts `RustStream` headers from a delivered `AMQP` message.
pub(crate) fn headers_from_amqp<B>(message: &Message<B>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Some(properties) = &message.properties {
        // Every well-known key is a lowercase literal, so the map takes it as a shared static and
        // this path copies no key text per delivery.
        if let Some(content_type) = &properties.content_type {
            headers.insert(Str::from_static("content-type"), content_type.to_string());
        }
        if let Some(correlation_id) = &properties.correlation_id {
            headers.insert(
                Str::from_static("correlation-id"),
                message_id_bytes(correlation_id),
            );
        }
        if let Some(reply_to) = &properties.reply_to {
            headers.insert(Str::from_static("reply-to"), reply_to.clone());
        }
        if let Some(message_id) = &properties.message_id {
            headers.insert(Str::from_static("message-id"), message_id_bytes(message_id));
        }
        if let Some(group_id) = &properties.group_id {
            headers.insert(Str::from_static(PARTITION_KEY_HEADER), group_id.clone());
        }
    }
    if let Some(application) = &message.application_properties {
        for (name, value) in application.iter() {
            headers.insert(name.clone(), simple_value_bytes(value));
        }
    }
    headers
}

/// The header value of any `AMQP` message-id form: a string or binary id as its bytes, a UUID as
/// its hyphenated lowercase hex, a number as its decimal digits.
fn message_id_bytes(id: &MessageId) -> Bytes {
    match id {
        MessageId::String(s) => Bytes::copy_from_slice(s.as_bytes()),
        MessageId::Binary(b) => Bytes::copy_from_slice(b),
        MessageId::Uuid(u) => Bytes::from(format!("{u:x}")),
        MessageId::Ulong(n) => Bytes::from(n.to_string()),
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
    use ruststream::{BytesMut, OutgoingMessage};

    use super::*;

    /// Content equality cannot tell a hand-over from a copy, so the buffer the framework wrote is
    /// identified by its address.
    #[test]
    fn the_data_body_keeps_the_buffer_the_framework_wrote() {
        let payload = BytesMut::from(&br#"{"id":1}"#[..]);
        let written = payload.as_ptr();
        let outgoing: OutgoingFor<'_, Take> = OutgoingMessage::produced("orders", payload);

        let message = to_amqp_message(outgoing).expect("the headers fit their properties");

        assert_eq!(
            message.body.0.as_ptr(),
            written,
            "the client keeps the body until the transfer settles, so the buffer is handed over \
             rather than copied"
        );
    }

    #[test]
    fn well_known_headers_ride_the_properties_section() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json");
        headers.insert("correlation-id", "corr-1");
        headers.insert("reply-to", "replies");
        headers.insert("message-id", "msg-1");
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("x-custom", "value");
        let outgoing = OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers);

        let message = to_amqp_message(outgoing).expect("the headers fit their properties");
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
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json");
        headers.insert("correlation-id", "corr-1");
        headers.insert(PARTITION_KEY_HEADER, "user-42");
        headers.insert("x-custom", "value");
        let outgoing =
            OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers.clone());

        let restored = headers_from_amqp(
            &to_amqp_message(outgoing).expect("the headers fit their properties"),
        );
        assert_eq!(restored.get_str("content-type"), Some("application/json"));
        assert_eq!(restored.get_str("correlation-id"), Some("corr-1"));
        assert_eq!(restored.get_str(PARTITION_KEY_HEADER), Some("user-42"));
        assert_eq!(restored.get_str("x-custom"), Some("value"));
    }

    /// Bytes that are not UTF-8: a lossy conversion would turn them into replacement characters.
    const NOT_UTF8: &[u8] = &[b'i', b'd', 0xff, 0x00, 0xfe];

    fn publish_with(name: &str, value: &'static [u8]) -> Result<Message<Data>, AmqpError> {
        let mut headers = HeaderMap::new();
        headers.insert(name.to_owned(), Bytes::from_static(value));
        to_amqp_message(OutgoingMessage::new("orders", b"{}".as_slice()).with_headers(headers))
    }

    #[test]
    fn a_string_property_refuses_bytes_that_are_not_utf8() {
        for header in ["reply-to", PARTITION_KEY_HEADER, "content-type"] {
            let err = publish_with(header, NOT_UTF8)
                .expect_err("a value the property cannot hold must not be rewritten");
            match err {
                AmqpError::UnsupportedHeader {
                    address,
                    header: named,
                    ..
                } => {
                    assert_eq!(address, "orders");
                    assert_eq!(named, header);
                }
                other => panic!("{header}: got {other}"),
            }
        }
    }

    #[test]
    fn the_content_type_symbol_refuses_text_outside_ascii() {
        let err = publish_with("content-type", "text/pl\u{e4}in".as_bytes())
            .expect_err("a symbol is ASCII");
        assert!(
            matches!(err, AmqpError::UnsupportedHeader { ref header, .. } if header == "content-type"),
            "got {err}"
        );
    }

    #[test]
    fn identifiers_that_are_not_utf8_ride_their_binary_form() {
        for header in ["message-id", "correlation-id"] {
            let message = publish_with(header, NOT_UTF8).expect("an identifier may be binary");
            let properties = message.properties.as_ref().expect("properties set");
            let id = if header == "message-id" {
                &properties.message_id
            } else {
                &properties.correlation_id
            };
            assert_eq!(
                id,
                &Some(MessageId::Binary(Binary::from(NOT_UTF8.to_vec()))),
                "{header}"
            );
            assert_eq!(
                headers_from_amqp(&message).get(header),
                Some(NOT_UTF8),
                "{header} reads back as the bytes it was published with"
            );
        }
    }

    #[test]
    fn data_body_yields_payload_bytes() {
        let outgoing = OutgoingMessage::new("orders", b"payload".as_slice());
        let message = to_amqp_message(outgoing).expect("the headers fit their properties");
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
