//! The service of `examples/amqp_publishing_headers.rs` written without `#[subscriber]`: the body
//! declares its publishing slot in the injections arena and copies the trace headers onto the
//! shipment it publishes there.
//!
//! ```text
//! cargo test --example manual_amqp_publishing_headers --features testing
//! cargo run --example manual_amqp_publishing_headers -- run
//! ```

use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

// --8<-- [start:headers]
/// The headers a shipment carries: `correlation-id` rides the `properties` section, `tenant` rides
/// `application-properties`.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Trace {
    #[serde(rename = "correlation-id")]
    correlation_id: String,
    tenant: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "shipments", headers = Trace)]
struct Shipment {
    order_id: u64,
}

#[derive(OutSlot)]
#[publishes(Shipment)]
struct Shipments;

struct Ship;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Ship
where
    Entry: OutEntry<Shipments, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        // Both sections arrive as one header map: the property and the application property alike.
        let headers = ctx.headers();
        let trace = Trace {
            correlation_id: headers.correlation_id().unwrap_or_default().to_owned(),
            tenant: headers.get_str("tenant").unwrap_or_default().to_owned(),
        };
        tracing::info!(order.id, tenant = %trace.tenant, "shipping");
        let shipment = Shipment { order_id: order.id };
        outs.get(Shipments)
            .message(&shipment)
            .with_headers(&trace)
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:headers]

fn app() -> impl App {
    RustStream::new(AppInfo::new("shipping", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            let ship = subscriber(AmqpAddress::queue("orders"), Ship);
            b.include(ship.build()).out(Shipments, Publish).build();
        },
    )
}

fn main() -> ExitCode {
    run_main(app)
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
