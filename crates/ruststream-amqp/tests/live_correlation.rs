//! A reply correlates with its request in the type the requester wrote the id in, against a real
//! broker, gated behind `AMQP_TEST_URL`.
//!
//! A Java or .NET client often writes `correlation-id` as a UUID or a number and matches a reply
//! by that typed value. The requester here is the bare client, the way such a peer is, and the
//! responders are a service's handlers: one answers through a slot, one through the reply
//! position.

use std::time::Duration;

use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use fe2o3_amqp_types::messaging::Body;
use fe2o3_amqp_types::messaging::{Message, MessageId, Properties};
use fe2o3_amqp_types::primitives::{Binary, Uuid, Value};
use ruststream_amqp::prelude::*;
use serde::Serialize;

use crate::live;

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

const UUID: [u8; 16] = [
    0x5e, 0x1f, 0x3a, 0x77, 0, 1, 0x40, 2, 0x80, 3, 4, 5, 6, 7, 8, 9,
];

// A `#[subscriber(..)]` argument is evaluated where the subscription opens, so the per-run names
// are functions rather than constants.
fn requests() -> String {
    format!("it.correlation.requests.{}", std::process::id())
}

fn orders() -> String {
    format!("it.correlation.orders.{}", std::process::id())
}

fn receipts() -> String {
    format!("it.correlation.receipts.{}", std::process::id())
}

fn replies() -> String {
    format!("it.correlation.replies.{}", std::process::id())
}

#[derive(Deserialized)]
struct Body_<'a>(&'a [u8]);

#[derive(Outgoing, Serialized)]
struct Answer(Vec<u8>);

/// A reply the default codec encodes, which is the form the reply position transforms.
#[derive(Serialize, Outgoing)]
struct Receipt {
    order: String,
}

/// The responder a service writes: it answers on the requester's reply address, with the
/// request's correlation in the type it arrived in.
#[subscriber(AmqpAddress::queue(requests()))]
async fn respond(
    body: &Body_<'_>,
    ctx: &mut Context<'_, AmqpContext>,
    Ctx(correlation): Ctx<ReplyCorrelation>,
    Out(out): Out<impl Publisher<Options = AmqpPublishOptions>>,
) -> HandlerOutcome {
    let Some(reply_to) = ctx.headers().reply_to().map(str::to_owned) else {
        return HandlerOutcome::drop();
    };
    let answer = Answer(body.0.to_vec());
    let sent = out
        .message(&answer)
        .to(reply_to)
        .correlation_id(correlation)
        .publish()
        .await;
    if sent.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

/// A handler answering through the reply position; reading the correlation is what puts the
/// context on the registration the transform reads.
// The reply name lives as long as the registration, which is the whole run.
#[subscriber(AmqpAddress::queue(orders()), reply(receipts().leak()))]
async fn receipt(body: &Body_<'_>, Ctx(_id): Ctx<ReplyCorrelation>) -> Receipt {
    Receipt {
        order: String::from_utf8_lossy(body.0).into_owned(),
    }
}

fn test_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

/// Sends `properties` with a body of `payload` to `address`, as a peer that is not this crate.
async fn send(sender: &mut Sender, properties: Properties, payload: &[u8]) {
    let message = Message::builder()
        .properties(properties)
        .data(Binary::from(payload.to_vec()))
        .build();
    sender
        .send(message)
        .await
        .expect("the peer sends")
        .accepted_or_else(|outcome| outcome)
        .expect("the broker accepts the request");
}

/// The next message on `receiver`, accepted, with the correlation id it carries.
async fn next_correlation(receiver: &mut Receiver) -> (Vec<u8>, Option<MessageId>) {
    let delivery = tokio::time::timeout(RECV_TIMEOUT, receiver.recv::<Body<Value>>())
        .await
        .expect("the reply arrives")
        .expect("the reply is received");
    receiver.accept(&delivery).await.expect("the peer accepts");
    let message = delivery.into_message();
    let correlation = message
        .properties
        .as_ref()
        .and_then(|properties| properties.correlation_id.clone());
    let payload = match message.body {
        Body::Data(batch) => batch
            .into_iter()
            .flat_map(|data| data.0.into_vec())
            .collect(),
        other => panic!("a reply carries a data body, got {other:?}"),
    };
    (payload, correlation)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_carries_the_correlation_in_the_type_the_requester_wrote() {
    let Some(url) = test_url() else { return };
    let app = RustStream::new(AppInfo::new("correlation", "0.1.0"))
        .with_broker(AmqpBroker::new(url.clone()), |b| {
            b.include(respond).out(DefaultSlot, Publish).build();
            b.include(receipt).out_reply(Publish).transform(Correlate);
        })
        .start()
        .await
        .expect("the service starts");

    let mut peer = Connection::builder()
        .container_id(format!("correlation-peer-{}", std::process::id()))
        .open(url.as_str())
        .await
        .expect("the peer connects");
    let mut session = Session::begin(&mut peer)
        .await
        .expect("the peer's session begins");
    let mut to_requests = Sender::attach(&mut session, "peer-requests", requests())
        .await
        .expect("the request sender attaches");
    let mut to_orders = Sender::attach(&mut session, "peer-orders", orders())
        .await
        .expect("the order sender attaches");
    let mut from_replies = Receiver::attach(&mut session, "peer-replies", replies())
        .await
        .expect("the reply receiver attaches");
    let mut from_receipts = Receiver::attach(&mut session, "peer-receipts", receipts())
        .await
        .expect("the receipt receiver attaches");

    // A UUID and a number, each as the peer's correlation id; the reply must carry the same value
    // in the same type, or the peer does not match it.
    for (payload, id) in [
        (b"uuid".as_slice(), MessageId::Uuid(Uuid::from(UUID))),
        (b"ulong".as_slice(), MessageId::Ulong(42)),
    ] {
        let properties = Properties {
            correlation_id: Some(id.clone()),
            reply_to: Some(replies()),
            ..Properties::default()
        };
        send(&mut to_requests, properties, payload).await;
        let (answered, correlation) = next_correlation(&mut from_replies).await;
        assert_eq!(answered, payload);
        assert_eq!(
            correlation,
            Some(id),
            "the slot reply to the {payload:?} request"
        );
    }

    // A request with a message id and no correlation id: the reply correlates to the message id,
    // the way a JMS requester matches it.
    let properties = Properties {
        message_id: Some(MessageId::Ulong(7)),
        ..Properties::default()
    };
    send(&mut to_orders, properties, b"order").await;
    let (answered, correlation) = next_correlation(&mut from_receipts).await;
    assert_eq!(answered, br#"{"order":"order"}"#);
    assert_eq!(
        correlation,
        Some(MessageId::Ulong(7)),
        "the reply-position answer"
    );

    to_requests
        .close()
        .await
        .expect("the request sender closes");
    to_orders.close().await.expect("the order sender closes");
    from_replies
        .close()
        .await
        .expect("the reply receiver closes");
    from_receipts
        .close()
        .await
        .expect("the receipt receiver closes");
    session.end().await.expect("the peer's session ends");
    peer.close().await.expect("the peer's connection closes");
    app.shutdown().await.expect("the service shuts down");
}
