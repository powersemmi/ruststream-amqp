//! What the in-process transport is handed on the publish path.
//!
//! These publishers declare `Take`, so the framework hands them the buffer it wrote and the header
//! map the publish filled. Content equality cannot tell a hand-over from a copy, so a payload is
//! identified by its address; a header map cannot be identified that way at all, because `Bytes`
//! keeps the data pointer across a clone, so the map is measured by the allocations the publish
//! makes.
#![cfg(feature = "testing")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, BytesMut, HeaderMap, IncomingMessage, OutgoingFor, OutgoingMessage, Publisher,
    Subscriber, Take,
};
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

/// Counts this thread's allocations, so the cost of one publish can be read off directly.
/// Thread-local rather than global: the other tests of this binary run beside it and their
/// allocations are none of this measurement's business.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        // SAFETY: the layout is the caller's, forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What this thread has allocated so far.
fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

/// A message with two headers, in the form a taking publisher is handed.
fn two_header_message() -> OutgoingFor<'static, Take> {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/json");
    headers.insert("x-tenant", "acme");
    OutgoingMessage::produced("orders", BytesMut::from(&br#"{"id":1}"#[..])).with_headers(headers)
}

/// The publish hands the router the map it was given rather than a copy of it. What is left in
/// the count is the router's own bookkeeping: the address it logs the message under and the
/// snapshot it keeps of every published message, which is the stand-in's record, not a transport
/// cost. Reintroducing the clone puts the count back up.
#[tokio::test]
async fn a_publish_with_no_subscriber_spends_nothing_on_its_header_map() {
    let broker = AmqpTestBroker::new().connect().await.expect("connect");
    let publisher = broker.publisher();

    // The log's own growth is not the subject: one publish outside the counted region leaves it
    // with room for the next. The message the region measures is built outside it too.
    publisher
        .publish(two_header_message(), None)
        .await
        .expect("publish failed");
    let msg = two_header_message();

    let before = allocations();
    publisher.publish(msg, None).await.expect("publish failed");
    let spent = allocations() - before;

    assert_eq!(
        spent, 8,
        "the router's log entry and its snapshot of the message, and nothing for the header map \
         the publish handed over"
    );
}
