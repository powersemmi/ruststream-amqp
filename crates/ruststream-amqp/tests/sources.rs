//! The declarations a service subscribes with, in its own app under the harness, and the in-process
//! transport under them.
//!
//! [`AmqpAddress`] is the crate's only subscription source, so these cases pin it: every address
//! kind opens a subscription in the service's app with the handler untouched, and the options the
//! in-process transport reproduces keep the meaning they carry against a server - the terminus
//! among them, since whether consumers compete or each get a copy is the difference between a work
//! queue and a broadcast. What has no in-process counterpart (credit) is covered by the live suite
//! in `live_descriptor.rs`.

#![cfg(feature = "testing")]

use std::pin::pin;
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::testing::{InProcess, TestApp, TestableBroker};
use ruststream::{AckError, ConnectedBroker, IncomingMessage, OutgoingMessage, Subscriber};
use ruststream_amqp::prelude::*;
use ruststream_amqp::{AmqpMessage, AmqpSubscriber, ConnectedAmqpBroker};
use serde::{Deserialize, Serialize};

/// The broker's address; the in-process mode dials nothing.
const URL: &str = "amqp://broker.example.com:5672";

const WAIT: Duration = Duration::from_secs(1);

/// The derive names no destination, so the publishes below keep saying `to(..)`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// The three addresses a handler is declared on, paired with the id published to each.
const MOUNTS: [(&str, u64); 3] = [("orders", 1), ("events", 2), ("/queues/audit", 3)];

// `credit` is a protocol setting the in-process transport has no analogue for; naming it must
// still leave the handler mountable, since a service does not rewrite its declaration to be
// tested.
#[subscriber(AmqpAddress::queue("orders").credit(nonzero!(64)))]
async fn from_a_queue(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(AmqpAddress::topic("events"))]
async fn from_a_topic(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(AmqpAddress::raw("/queues/audit"))]
async fn from_a_raw_address(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

#[subscriber(AmqpAddress::queue("bulk").batch_wait(Duration::from_millis(50)))]
async fn in_batches(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

/// The service's app, on the broker it is handed.
fn app_on(broker: AmqpBroker) -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(from_a_queue);
        b.include(from_a_topic);
        b.include(from_a_raw_address);
        b.include(in_batches.batch(nonzero!(2usize)));
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_address_kind_mounts() {
    let tb = TestApp::start(app_on(AmqpBroker::new(URL)))
        .await
        .expect("startup failed");

    for (address, id) in MOUNTS {
        tb.broker::<AmqpBroker>()
            .message(&Order { id })
            .to(address)
            .publish()
            .await
            .expect("publish failed");
    }

    for (address, id) in MOUNTS {
        tb.broker::<AmqpBroker>()
            .subscriber(address)
            .assert_called_once()
            .with(&Order { id })
            .settled(HandlerOutcome::ack());
    }

    tb.shutdown().await.expect("shutdown failed");
}

// The descriptor carries the batch deadline, and the batches are assembled by the same
// client-side buffer on either transport, so a batch mount takes the descriptor as readily as a
// bare name does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_mount_takes_the_descriptor_too() {
    // A publisher taken off the broker before it connects publishes as an external client does,
    // without driving the reaction to a standstill on every message - which would close each
    // batch after a single delivery.
    let broker = AmqpBroker::new(URL);
    let producer = broker.publisher();
    let tb = TestApp::start(app_on(broker))
        .await
        .expect("startup failed");

    for id in 0..4 {
        producer
            .message(&Order { id })
            .to("bulk")
            .publish()
            .await
            .expect("publish failed");
    }
    tb.settle().await.expect("the run settles");

    let batches: Vec<Vec<Order>> = tb.broker::<AmqpBroker>().subscriber("bulk").batches();
    assert_eq!(
        batches.concat(),
        (0..4).map(|id| Order { id }).collect::<Vec<_>>(),
        "every order must reach the batch handler, in order",
    );

    tb.shutdown().await.expect("shutdown failed");
}

/// The broker connected in process, for the cases whose subject is the transport itself.
async fn in_process() -> ConnectedAmqpBroker {
    AmqpBroker::new(URL)
        .connect_in_process()
        .await
        .expect("connect failed")
}

// The settle mode is an option the transport reproduces rather than drops: an at-most-once
// delivery arrives settled, so its settlement reports `Unsupported` here exactly as it does
// against a server (`integration_amqp.rs::at_most_once_reports_ack_unsupported`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_at_most_once_descriptor_settles_on_receipt() {
    let broker = in_process().await;
    let mut subscriber = broker
        .subscribe_address(AmqpAddress::queue("fire").settle(Settle::AtMostOnce))
        .await
        .expect("subscription opens");
    broker
        .publisher()
        .message(&Order { id: 1 })
        .to("fire")
        .publish()
        .await
        .expect("publish failed");

    let message = next_delivery(&mut subscriber).await;
    assert!(matches!(message.ack().await, Err(AckError::Unsupported)));
}

// The descriptor is validated on either transport, so a mount that cannot form a subscription
// fails in a test as early as it does in production.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_invalid_descriptor_is_rejected_before_the_subscription_opens() {
    let broker = in_process().await;
    let err = broker
        .subscribe_address(AmqpAddress::queue(""))
        .await
        .expect_err("an empty address cannot form a subscription");
    assert!(matches!(err, AmqpError::InvalidAddress(_)), "got {err}");
}

/// The next delivery's payload, or a panic naming the wait that ran out.
async fn next_payload<S>(stream: &mut S) -> Vec<u8>
where
    S: Stream<Item = Result<AmqpMessage, AmqpError>> + Unpin,
{
    let message = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok");
    let payload = message.payload().to_vec();
    message.ack().await.expect("ack succeeds");
    payload
}

// The work-queue case: a queue terminus hands each message to one of its consumers, so a service
// that splits work across handlers is testable in process instead of passing a broadcast that a
// real broker would never perform.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_queue_subscriptions_share_the_address() {
    let broker = in_process().await;
    let mut first = broker
        .subscribe_address(AmqpAddress::queue("work"))
        .await
        .expect("the first subscription opens");
    let mut second = broker
        .subscribe_address(AmqpAddress::queue("work"))
        .await
        .expect("the second subscription opens");

    let producer = broker.publisher();
    for id in 0..4_u8 {
        producer
            .publish(OutgoingMessage::new("work", [b'0' + id].as_slice()), None)
            .await
            .expect("publish failed");
    }

    let mut first_stream = Box::pin(first.stream());
    let mut second_stream = Box::pin(second.stream());
    let mut seen = Vec::new();
    for _ in 0..2 {
        seen.push(next_payload(&mut first_stream).await);
        seen.push(next_payload(&mut second_stream).await);
    }
    seen.sort_unstable();
    assert_eq!(
        seen,
        vec![b"0".to_vec(), b"1".to_vec(), b"2".to_vec(), b"3".to_vec()],
        "each message must reach exactly one of the competing consumers, and none may be lost",
    );
}

// The broadcast case, which the same address kind must not silently give: a topic terminus copies
// every message to every subscription.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_subscriptions_each_get_a_copy() {
    let broker = in_process().await;
    let mut first = broker
        .subscribe_address(AmqpAddress::topic("events"))
        .await
        .expect("the first subscription opens");
    let mut second = broker
        .subscribe_address(AmqpAddress::topic("events"))
        .await
        .expect("the second subscription opens");

    broker
        .publisher()
        .publish(
            OutgoingMessage::new("events", b"broadcast".as_slice()),
            None,
        )
        .await
        .expect("publish failed");

    let mut first_stream = Box::pin(first.stream());
    let mut second_stream = Box::pin(second.stream());
    assert_eq!(next_payload(&mut first_stream).await, b"broadcast");
    assert_eq!(next_payload(&mut second_stream).await, b"broadcast");
}

// The delivery counter is another option the transport reproduces rather than drops. AMQP 1.0
// counts failed attempts in the `delivery-count` field of a message's `header` section, and a
// message nothing has counted an attempt for carries no header section at all - which is what a
// message this crate published looks like. The framework reads that count to apply a
// registration's cap, so the two transports have to answer the same way
// (`integration_amqp.rs::a_requeued_delivery_reports_the_broker_count`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_requeued_delivery_carries_one_more_counted_attempt() {
    let broker = in_process().await;
    let mut subscriber = broker
        .subscribe_address(AmqpAddress::queue("counted"))
        .await
        .expect("subscription opens");
    broker
        .publisher()
        .publish(OutgoingMessage::new("counted", b"once".as_slice()), None)
        .await
        .expect("publish failed");

    let first = next_delivery(&mut subscriber).await;
    assert_eq!(
        first.redelivery_count(),
        None,
        "a fresh delivery carries no count, so the framework's own header decides the attempt",
    );
    first.nack(true).await.expect("the requeue succeeds");

    let second = next_delivery(&mut subscriber).await;
    assert_eq!(
        second.redelivery_count(),
        Some(2),
        "the requeue counted one failed attempt, so this delivery is the second",
    );
    second.ack().await.expect("ack succeeds");
}

// A delivery handed back after its subscription detached stays on the address, and a consumer
// still attached there takes it, as the broker hands a released message to one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delivery_released_after_its_subscription_detached_reaches_another_consumer() {
    let broker = in_process().await;
    let mut first = broker
        .subscribe_address(AmqpAddress::queue("handover"))
        .await
        .expect("the first subscription opens");
    let mut second = broker
        .subscribe_address(AmqpAddress::queue("handover"))
        .await
        .expect("the second subscription opens");
    broker
        .publisher()
        .publish(OutgoingMessage::new("handover", b"work".as_slice()), None)
        .await
        .expect("publish failed");

    // The rotation starts at the first subscription.
    let held = next_delivery(&mut first).await;
    drop(first);
    held.nack(true).await.expect("the release succeeds");

    let again = next_delivery(&mut second).await;
    assert_eq!(again.payload(), b"work");
    assert_eq!(again.redelivery_count(), Some(2));
}

// A subscription still open when the connection shuts down ends with it: its stream yields what it
// had received and then ends, as a live subscription's does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_open_at_shutdown_ends_its_stream() {
    let broker = in_process().await;
    let mut subscriber = broker
        .subscribe_address(AmqpAddress::queue("closing"))
        .await
        .expect("subscription opens");
    broker
        .publisher()
        .publish(OutgoingMessage::new("closing", b"last".as_slice()), None)
        .await
        .expect("publish failed");
    broker.shutdown().await.expect("shutdown failed");

    let mut stream = pin!(subscriber.stream());
    let last = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("the received delivery is yielded")
        .expect("stream is open")
        .expect("delivery is ok");
    assert_eq!(last.payload(), b"last");
    let end = tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("the stream ends rather than waiting for a closed connection");
    assert!(end.is_none(), "the stream must end after the shutdown");
}

/// The next delivery off a subscriber, leaving it free to be polled again.
async fn next_delivery(subscriber: &mut AmqpSubscriber) -> AmqpMessage {
    let mut stream = pin!(subscriber.stream());
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("delivery arrives")
        .expect("stream is open")
        .expect("delivery is ok")
}

// What a live harness waits on: the broker's own answer to whom a publish reaches. An address
// routes to its topic subscriptions, a copy each, and to one of its queue subscriptions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_routing_answer_is_one_queue_consumer_and_every_topic_subscription() {
    let broker = in_process().await;
    // Held open for the length of the case, as an app holds its subscriptions.
    let _open = futures::future::try_join_all(
        [
            AmqpAddress::queue("work"),
            AmqpAddress::queue("work"),
            AmqpAddress::topic("news"),
            AmqpAddress::topic("news"),
        ]
        .map(|address| broker.subscribe_address(address)),
    )
    .await
    .expect("the subscriptions open");
    let names = ["work", "work", "news", "news", "other"];

    assert_eq!(broker.routes("work", &names), [0]);
    assert_eq!(broker.routes("news", &names), [2, 3]);
    assert_eq!(broker.routes("nowhere", &names), [0_usize; 0]);
}

// A subscription that detached no longer counts: the answer follows the subscriptions attached
// now, which are the ones the transport delivers to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_detached_subscription_leaves_the_routing_answer() {
    let broker = in_process().await;
    let _queue = broker
        .subscribe_address(AmqpAddress::queue("mixed"))
        .await
        .expect("the queue subscription opens");
    let topic = broker
        .subscribe_address(AmqpAddress::topic("mixed"))
        .await
        .expect("the topic subscription opens");
    let names = ["mixed", "mixed"];
    assert_eq!(broker.routes("mixed", &names), [0, 1]);

    drop(topic);
    assert_eq!(broker.routes("mixed", &names), [0]);
}
