//! Partition keys on `AMQP` 1.0: the partition-key header of a published message becomes the
//! message's `group-id` property, and a broker that honours message groups keeps one group on one
//! consumer.
//!
//! ```text
//! cargo test --example amqp_publishing_keys --features testing
//! cargo run --example amqp_publishing_keys -- run
//! ```

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    customer: String,
}

// --8<-- [start:keys]
/// The header a shipment carries: `partition-key`, which becomes the `group-id` property.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct ShipmentKey {
    #[serde(rename = "partition-key")]
    customer: String,
}

/// Every shipment of one customer is one message group, so one packer takes all of them.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments", headers = ShipmentKey)]
struct Shipment {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

#[subscriber(AmqpAddress::queue("orders"))]
async fn ship(order: &Order, Out(shipments): Out<impl Publisher, Shipments>) -> HandlerOutcome {
    let key = ShipmentKey {
        customer: order.customer.clone(),
    };
    tracing::info!(order.id, customer = %key.customer, "shipping");
    let shipment = Shipment { order_id: order.id };
    if shipments
        .message(&shipment)
        .with_headers(&key)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:keys]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(ship).out(Shipments, Publish).build();
        },
    )
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::PARTITION_KEY_HEADER;
    use ruststream_amqp::prelude::*;
    use serde::{Deserialize, Serialize};

    use super::{Order, Shipment, ShipmentKey, Shipments, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_shipment_is_keyed_by_its_customer() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<AmqpBroker>()
            .message(&Order {
                id: 7,
                customer: "customer-42".to_owned(),
            })
            .publish()
            .await?;

        tb.out::<Shipments>().assert_called_once();
        tb.broker::<AmqpBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 7 })
            .with_header(PARTITION_KEY_HEADER, "customer-42");
        tb.shutdown().await?;
        Ok(())
    }

    // The round trip through `group-id` on a running broker: a consumer reads the key back as the
    // partition-key header. The address is per process, so a stand kept running between runs
    // hands this one none of another's messages.
    fn keyed_shipments() -> String {
        format!("keys.shipments.{}", std::process::id())
    }

    /// What the consumer read off the delivery, published so the test can see it in both modes.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(name = "keys.read")]
    struct KeyRead {
        key: String,
    }

    /// A shipment on the per-process address, keyed like the service keys it.
    #[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
    #[outgoing(headers = ShipmentKey)]
    struct Parcel {
        order_id: u64,
    }

    #[subscriber(AmqpAddress::queue(keyed_shipments()), reply)]
    async fn read_key(parcel: &Parcel, ctx: &mut Context<'_>) -> KeyRead {
        let _ = parcel.order_id;
        KeyRead {
            key: ctx
                .headers()
                .get_str(PARTITION_KEY_HEADER)
                .unwrap_or_default()
                .to_owned(),
        }
    }

    fn packer(url: &str) -> RustStream {
        RustStream::new(AppInfo::new("packing", "0.1.0")).with_broker(AmqpBroker::new(url), |b| {
            b.include(read_key).out_reply(Publish);
        })
    }

    async fn the_key_comes_back_to_the_consumer(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        let key = ShipmentKey {
            customer: "customer-42".to_owned(),
        };
        tb.broker::<AmqpBroker>()
            .message(&Parcel { order_id: 7 })
            .to(keyed_shipments())
            .with_headers(&key)
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .published::<KeyRead>("keys.read")
            .assert_called_once()
            .with(&KeyRead {
                key: "customer-42".to_owned(),
            });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_key_comes_back_in_process() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(packer("amqp://broker.example.com:5672")).await?;
        the_key_comes_back_to_the_consumer(tb).await
    }

    /// The broker under test: `AMQP_TEST_URL`. A run that sets `RUSTSTREAM_REQUIRE_LIVE` and
    /// leaves the address out fails instead of skipping.
    fn live_url() -> Option<String> {
        let url = env::var("AMQP_TEST_URL").ok().filter(|url| !url.is_empty());
        assert!(
            url.is_some() || env::var("RUSTSTREAM_REQUIRE_LIVE").is_err(),
            "RUSTSTREAM_REQUIRE_LIVE is set, so this test must run, but AMQP_TEST_URL is unset",
        );
        url
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_key_comes_back_live() -> Result<(), Box<dyn Error>> {
        let Some(url) = live_url() else {
            return Ok(());
        };
        let tb = TestApp::start_live(packer(&url)).await?;
        the_key_comes_back_to_the_consumer(tb).await
    }
}
