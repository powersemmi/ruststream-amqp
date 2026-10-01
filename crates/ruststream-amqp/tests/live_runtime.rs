//! A subscription opened from another runtime runs on the runtime the broker connected on, gated
//! behind `AMQP_TEST_URL`.
//!
//! A handler on a dedicated thread calls into the broker from that thread's own current-thread
//! runtime. The conformance `lifecycle` suite covers publishing, settling and requesting from
//! there; this suite covers opening a subscription, whose session and pump are tasks of their own.
//!
//! Start a broker with `just brokers-up` (`ActiveMQ` Artemis), then:
//! `AMQP_TEST_URL=amqp://artemis:artemis@127.0.0.1:5672 cargo test --all-features -- --test-threads=1`.

use std::pin::pin;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures::StreamExt;
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};
use ruststream_amqp::{AmqpAddress, AmqpBroker, AmqpSubscriber, ConnectedAmqpBroker};
use tokio::runtime::Builder;
use tokio::sync::oneshot;

mod live;

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

/// Opens a subscription from a current-thread runtime on a thread of its own, and stops that
/// runtime before handing the subscription back.
async fn subscribe_on_a_foreign_runtime(
    connected: Arc<ConnectedAmqpBroker>,
    address: String,
) -> AmqpSubscriber {
    let (done, opened) = oneshot::channel();
    thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime builds");
        let subscriber = runtime.block_on(connected.subscribe_address(AmqpAddress::queue(address)));
        // Stopped before the subscription is handed back, so anything the broker left on this
        // runtime is gone by the time the test reads from it.
        drop(runtime);
        let _ = done.send(subscriber);
    });
    opened
        .await
        .expect("the foreign runtime's thread finished")
        .expect("the subscription opens")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_subscription_opened_from_another_runtime_outlives_it() {
    let Some(url) = live::url("AMQP_TEST_URL") else {
        return;
    };
    let address = format!("it.runtime.subscribe.{}", std::process::id());
    let connected = Arc::new(
        AmqpBroker::new(url)
            .container_id(format!("runtime-{}", std::process::id()))
            .connect()
            .await
            .expect("broker connects"),
    );

    let mut subscriber =
        subscribe_on_a_foreign_runtime(Arc::clone(&connected), address.clone()).await;
    connected
        .publisher()
        .publish(OutgoingMessage::new(&address, b"one".as_slice()), None)
        .await
        .expect("publish succeeds");
    {
        let mut stream = pin!(subscriber.stream());
        let delivery = tokio::time::timeout(RECV_TIMEOUT, stream.next())
            .await
            .expect("the delivery arrives")
            .expect("the subscription is still open")
            .expect("the delivery is ok");
        assert_eq!(delivery.payload(), b"one");
        delivery.ack().await.expect("ack succeeds");
    }

    drop(subscriber);
    let connected = Arc::into_inner(connected).expect("the test holds the only handle");
    connected.shutdown().await.expect("shutdown succeeds");
}
