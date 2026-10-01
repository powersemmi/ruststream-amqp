//! A clean shutdown reports no error, gated behind `AMQP_TEST_URL`.
//!
//! A subscription runs on a session of its own, ended by the task that pumps its deliveries. The
//! connection closes only after every such session has ended: a session that ends after the close
//! is answered on a connection already in `CloseSent`, and the client then reports the ordinary
//! stop as `IllegalState`. The race lost roughly one stop in four, so one cycle proves nothing and
//! the test runs many.
//!
//! Start a broker with `just brokers-up` (`ActiveMQ` Artemis), then:
//! `AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::time::Duration;

use futures::StreamExt;
#[cfg(feature = "testing")]
use ruststream::testing::InProcess;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_amqp::{AmqpAddress, AmqpBroker, AmqpSubscriber, ConnectedAmqpBroker};

mod live;

/// Start-and-stop cycles per test: enough that a race lost one stop in four cannot pass by luck.
const CYCLES: usize = 16;

/// Messages published per cycle. One is consumed; the rest are in flight on the link when the
/// stop begins, which is the load under which the subscription's teardown is slowest.
const IN_FLIGHT: usize = 64;

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// The broker URL, or `None` to skip. Under `RUSTSTREAM_REQUIRE_LIVE` a missing one is a failure.
fn test_url() -> Option<String> {
    live::url("AMQP_TEST_URL")
}

/// Connects, subscribes, and consumes one delivery; the subscription is returned still open.
async fn consume_one(
    url: &str,
    address: &str,
    cycle: usize,
) -> (ConnectedAmqpBroker, AmqpSubscriber) {
    let connected = AmqpBroker::new(url)
        .container_id(format!("shutdown-{}-{cycle}", std::process::id()))
        .connect()
        .await
        .expect("broker connects");
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue(address))
        .await
        .expect("subscription opens");
    let publisher = connected.publisher();
    for _ in 0..IN_FLIGHT {
        publisher
            .publish(OutgoingMessage::new(address, b"one".as_slice()), None)
            .await
            .expect("publish succeeds");
    }
    {
        let mut stream = pin!(subscriber.stream());
        let delivery = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the delivery arrives")
            .expect("the stream is open")
            .expect("the delivery is ok");
        assert_eq!(delivery.payload(), b"one");
        delivery.ack().await.expect("ack succeeds");
    }
    (connected, subscriber)
}

/// The order the runtime stops in: the subscription is dropped, then the broker shuts down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shutdown_after_the_subscription_is_dropped_reports_no_error() {
    let Some(url) = test_url() else { return };
    let address = format!("it.shutdown.dropped.{}", std::process::id());

    for cycle in 0..CYCLES {
        let (connected, subscriber) = consume_one(&url, &address, cycle).await;
        drop(subscriber);
        if let Err(err) = connected.shutdown().await {
            panic!("cycle {cycle}: a clean shutdown reported {err}");
        }
    }
}

/// A subscription still open when the broker shuts down is ended by the shutdown, before the
/// connection closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shutdown_under_an_open_subscription_reports_no_error() {
    let Some(url) = test_url() else { return };
    let address = format!("it.shutdown.open.{}", std::process::id());

    for cycle in 0..CYCLES {
        let (connected, mut subscriber) = consume_one(&url, &address, cycle).await;
        if let Err(err) = connected.shutdown().await {
            panic!("cycle {cycle}: a clean shutdown reported {err}");
        }
        // The shutdown ended the subscription: the stream hands over what it had already
        // received and then ends, and none of those deliveries can be settled any more.
        let mut stream = pin!(subscriber.stream());
        while let Some(next) = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the ended subscription does not wait")
        {
            let delivery = next.expect("the delivery is ok");
            assert!(
                delivery.ack().await.is_err(),
                "cycle {cycle}: an ack after shutdown reported success"
            );
        }
    }
}

/// The in-process transport answers a settlement after shutdown the way a server connection
/// does: the session the disposition would travel on has ended, so the ack reports an error
/// rather than success.
#[cfg(feature = "testing")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_in_process_transport_refuses_a_settlement_after_shutdown() {
    let connected = AmqpBroker::new("amqp://broker.example.com:5672")
        .connect_in_process()
        .await
        .expect("the broker connects in process");
    let mut subscriber = connected
        .subscribe_address(AmqpAddress::queue("orders"))
        .await
        .expect("subscription opens");
    connected
        .publisher()
        .publish(OutgoingMessage::new("orders", b"one".as_slice()), None)
        .await
        .expect("publish succeeds");
    let mut stream = pin!(subscriber.stream());
    let delivery = tokio::time::timeout(RECV_TIMEOUT, stream.next())
        .await
        .expect("the delivery arrives")
        .expect("the stream is open")
        .expect("the delivery is ok");

    connected.shutdown().await.expect("shutdown succeeds");
    assert!(
        delivery.ack().await.is_err(),
        "an ack after shutdown reported success"
    );
}
