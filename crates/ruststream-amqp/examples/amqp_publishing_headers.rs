//! Headers on `AMQP` 1.0: the well-known ones ride the message's `properties` section, every other
//! one rides `application-properties`, and a delivery reads both back as one header map.
//!
//! ```text
//! cargo test --example amqp_publishing_headers --features testing
//! cargo run --example amqp_publishing_headers -- run
//! ```

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:headers]
/// The headers a shipment carries: `correlation-id` rides the `properties` section, `tenant` rides
/// `application-properties`.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Trace {
    #[serde(rename = "correlation-id")]
    correlation_id: String,
    tenant: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "shipments", headers = Trace)]
struct Shipment {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

#[subscriber(AmqpAddress::queue("orders"))]
async fn ship(
    order: &Order,
    ctx: &mut Context<'_>,
    Out(shipments): Out<impl Publisher, Shipments>,
) -> HandlerOutcome {
    // Both sections arrive as one header map: the property and the application property alike.
    let headers = ctx.headers();
    let trace = Trace {
        correlation_id: headers.correlation_id().unwrap_or_default().to_owned(),
        tenant: headers.get_str("tenant").unwrap_or_default().to_owned(),
    };
    tracing::info!(order.id, tenant = %trace.tenant, "shipping");
    let shipment = Shipment { order_id: order.id };
    if shipments
        .message(&shipment)
        .with_headers(&trace)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:headers]

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
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Order, Shipment, Shipments, app};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn both_sections_carry_over_to_the_shipment() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let mut headers = HeaderMap::new();
        headers.insert("correlation-id", "order-7");
        headers.insert("tenant", "acme");
        tb.broker::<AmqpBroker>()
            .message(&Order { id: 7 })
            .with_headers(headers)
            .publish()
            .await?;

        tb.out::<Shipments>().assert_called_once();
        tb.broker::<AmqpBroker>()
            .published::<Shipment>("shipments")
            .assert_called_once()
            .with(&Shipment { order_id: 7 })
            .with_header("correlation-id", "order-7")
            .with_header("tenant", "acme");
        tb.shutdown().await?;
        Ok(())
    }
}
