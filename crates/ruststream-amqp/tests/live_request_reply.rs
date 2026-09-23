//! How fast a request/reply round trip actually is against a real broker, gated behind
//! `AMQP_TEST_URL`.
//!
//! The pattern is small-write-then-wait on both sides, which is the case Nagle's algorithm is
//! wrong for: a reply written on a connection that is also writing dispositions waits for the
//! peer's delayed acknowledgement before it leaves the kernel. That wait is tens of milliseconds
//! and nothing in a log names it, so the deadline below is what says the socket is configured the
//! way a messaging client needs.
//!
//! Start a broker with `just brokers-up` (`ActiveMQ` Artemis), then:
//! `AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::{Duration, Instant};

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, HeaderMap, IncomingMessage, OutgoingMessage, Publisher, RequestReply,
    Subscriber,
};
use ruststream_amqp::{AmqpAddress, AmqpBroker, ConnectedAmqpBroker};

mod live;

/// Round trips the run makes. Enough that one outlier cannot carry the average, few enough that
/// the run stays under a second either way and a failure is read from the report rather than
/// waited for.
const EXCHANGES: u32 = 25;

/// What one round trip may take on an idle local broker.
///
/// The crate's own cost is under a millisecond; a delayed acknowledgement is 23 to 45. The
/// deadline sits an order of magnitude above the first and well under the second, so it separates
/// the two rather than measuring the machine.
const PER_EXCHANGE: Duration = Duration::from_millis(10);

/// How long one request may wait for its answer before the test gives up on the broker.
const WAIT: Duration = Duration::from_secs(10);

/// The broker URL, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing one is a failure.
fn test_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

async fn connect(url: &str) -> ConnectedAmqpBroker {
    AmqpBroker::new(url)
        .container_id(format!("rr-{}", std::process::id()))
        .connect()
        .await
        .expect("broker connects")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_reply_round_trip_is_not_paced_by_a_tcp_timer() {
    let Some(url) = test_url() else { return };
    let address = format!("rr.pace.{}", std::process::id());

    let responder = connect(&url).await;
    let mut subscriber = responder
        .subscribe_address(AmqpAddress::queue(&address))
        .await
        .expect("subscription opens");
    let answers = responder.publisher();
    let answering = tokio::spawn(async move {
        let mut stream = pin!(subscriber.stream());
        for _ in 0..EXCHANGES {
            let request = stream
                .next()
                .await
                .expect("the stream is open")
                .expect("the delivery is ok");
            let reply_to = request
                .headers()
                .reply_to()
                .expect("the request names a reply address")
                .to_owned();
            let mut headers = HeaderMap::new();
            if let Some(correlation_id) = request.headers().correlation_id() {
                headers.insert("correlation-id", correlation_id.to_owned());
            }
            request.ack().await.expect("ack succeeds");
            answers
                .publish(
                    OutgoingMessage::new(&reply_to, b"pong".as_slice()).with_headers(headers),
                    None,
                )
                .await
                .expect("the reply is published");
        }
    });

    let requester = connect(&url).await;
    let publisher = requester.publisher();
    let started = Instant::now();
    for _ in 0..EXCHANGES {
        let reply = publisher
            .request(OutgoingMessage::new(&address, b"ping".as_slice()), WAIT)
            .await
            .expect("the responder answers");
        assert_eq!(reply.payload(), b"pong");
    }
    let per_exchange = started.elapsed() / EXCHANGES;

    answering.await.expect("the responder finishes");
    requester.shutdown().await.expect("shutdown succeeds");
    responder.shutdown().await.expect("shutdown succeeds");

    assert!(
        per_exchange < PER_EXCHANGE,
        "a round trip took {per_exchange:?}, which is a delayed acknowledgement rather than this \
         crate: the connection's socket is left with Nagle's algorithm on"
    );
}
