//! What the in-process transport is handed on the publish path.
//!
//! These publishers declare `Take`, so the framework hands them the buffer it wrote and the header
//! map the publish filled. Content equality cannot tell a hand-over from a copy, so a payload is
//! identified by its address; a header map cannot be identified that way at all, because `Bytes`
//! keeps the data pointer across a clone, so the map is measured by the allocations the publish
//! makes.
#![cfg(feature = "testing")]

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{Broker, BytesMut, IncomingMessage, OutgoingMessage, Publisher, Subscriber};
use ruststream_amqp::AmqpAddress;
use ruststream_amqp::testing::AmqpTestBroker;

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_routed_publish_keeps_the_buffer_the_framework_wrote() {
    let broker = AmqpTestBroker::new().connect().await.expect("connect");
    let mut subscriber = broker
        .subscribe_address(AmqpAddress::queue("orders"))
        .await
        .expect("subscription opens");

    let payload = BytesMut::from(&br#"{"id":1}"#[..]);
    let written = payload.as_ptr();
    broker
        .publisher()
        .publish(OutgoingMessage::produced("orders", payload), None)
        .await
        .expect("publish failed");

    let mut stream = pin!(subscriber.stream());
    let delivery = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");

    assert_eq!(
        delivery.payload().as_ptr(),
        written,
        "the router keeps the payload, so the publish hands the buffer over rather than copying it"
    );
}
