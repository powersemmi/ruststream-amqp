//! How headers travel through a real `AMQP` 1.0 broker, gated behind `AMQP_TEST_URL`.
//!
//! The mapping onto the message sections is unit-tested beside it; what is here is that the
//! broker keeps each form a header is sent in, so a value comes back as the bytes it left as.

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, Bytes, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher,
    Subscriber,
};
use ruststream_amqp::{AmqpAddress, AmqpBroker, AmqpError, ConnectedAmqpBroker};

use crate::live;

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// Identifier bytes that are not UTF-8, which only the binary form of the property holds.
const NOT_UTF8: &[u8] = &[b'i', b'd', 0xff, 0x00, 0xfe];

fn test_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

async fn connect(url: &str) -> ConnectedAmqpBroker {
    AmqpBroker::new(url)
        .container_id(format!("headers-{}", std::process::id()))
        .connect()
        .await
        .expect("broker connects")
}

fn unique(name: &str) -> String {
    format!("it.headers.{name}.{}", std::process::id())
}

/// A message id and a correlation id that are not UTF-8 travel as binary and come back as the
/// same bytes; a reply address that is not UTF-8 has no form in its string property, so that
/// publish fails before anything reaches the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identifiers_keep_their_bytes_and_a_string_property_refuses_others() {
    let Some(url) = test_url() else { return };
    let connected = connect(&url).await;
    let address = unique("ids");
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();

    let mut refused = HeaderMap::new();
    refused.insert("reply-to", Bytes::from_static(NOT_UTF8));
    let err = publisher
        .publish(
            OutgoingMessage::new(&address, b"refused".as_slice()).with_headers(refused),
            None,
        )
        .await
        .expect_err("a reply address that is not UTF-8 must not be rewritten");
    assert!(
        matches!(&err, AmqpError::UnsupportedHeader { header, .. } if header == "reply-to"),
        "got {err}"
    );

    let mut headers = HeaderMap::new();
    headers.insert("message-id", Bytes::from_static(NOT_UTF8));
    headers.insert("correlation-id", Bytes::from_static(NOT_UTF8));
    publisher
        .publish(
            OutgoingMessage::new(&address, b"kept".as_slice()).with_headers(headers),
            None,
        )
        .await
        .expect("publish succeeds");

    let mut stream = pin!(subscriber.stream());
    let message = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    // The refused publish sent nothing, so the first delivery is the one that went out.
    assert_eq!(message.payload(), b"kept");
    assert_eq!(message.headers().get("message-id"), Some(NOT_UTF8));
    assert_eq!(message.headers().get("correlation-id"), Some(NOT_UTF8));
    message.ack().await.expect("ack succeeds");

    connected.shutdown().await.expect("shutdown succeeds");
}
