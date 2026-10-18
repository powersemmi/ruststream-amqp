//! `AmqpError` where a service meets it: a publish the broker refuses, and a startup that fails.
//!
//! ```text
//! cargo test --example amqp_broker_errors --features testing
//! ```

use std::error::Error;

use serde::{Deserialize, Serialize};

use ruststream_amqp::prelude::*;

// --8<-- [start:handler]
#[subscriber(AmqpAddress::queue("orders"))]
async fn ship(order: &Order, Out(warehouses): Out<impl Publisher>) -> HandlerOutcome {
    let shipment = Shipment { order_id: order.id };
    let sent = warehouses
        .message(&shipment)
        .to(order.warehouse.as_str())
        .publish()
        .await;
    match sent {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(subscription = "orders", order.id, error = %err, "shipment not sent");
            if refused_by_the_broker(&err) {
                HandlerOutcome::drop()
            } else {
                HandlerOutcome::retry()
            }
        }
    }
}

/// Whether the broker settled the message with an outcome other than accepted. Sending it again
/// meets the same answer, while a connection that failed under the publish may be back by the next
/// attempt.
fn refused_by_the_broker(err: &(dyn Error + 'static)) -> bool {
    let mut cause = Some(err);
    while let Some(err) = cause {
        if let Some(AmqpError::PublishNotAccepted { .. }) = err.downcast_ref() {
            return true;
        }
        cause = err.source();
    }
    false
}
// --8<-- [end:handler]

#[ruststream::app]
fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(ship).out(DefaultSlot, Publish).build();
        },
    )
}

/// An order names the warehouse address its shipment goes to.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    warehouse: String,
}

/// The derive names no address: the handler sends each shipment where its order says.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Shipment {
    order_id: u64,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Order, Shipment, app};

    #[tokio::test]
    async fn a_shipment_the_broker_accepts_acks_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 7,
            warehouse: "warehouse.north".to_owned(),
        };
        tb.broker::<AmqpBroker>().message(&order).publish().await?;

        tb.broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<AmqpBroker>()
            .published::<Shipment>("warehouse.north")
            .assert_called_once()
            .with(&Shipment { order_id: 7 });
        Ok(())
    }

    #[tokio::test]
    async fn a_shipment_the_broker_refuses_drops_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        // The empty address names no node, and the broker rejects a message sent to it.
        let order = Order {
            id: 8,
            warehouse: String::new(),
        };
        tb.broker::<AmqpBroker>().message(&order).publish().await?;

        tb.broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        Ok(())
    }

    #[subscriber(AmqpAddress::queue(""))]
    async fn nowhere(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "never reached");
        HandlerOutcome::ack()
    }

    #[tokio::test]
    async fn a_failed_startup_carries_the_amqp_error() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
            |b| {
                b.include(nowhere);
            },
        );

        let failed = TestApp::start(app).await.err();
        let mut cause: Option<&(dyn Error + 'static)> = failed.as_ref().map(|err| err as _);
        while let Some(err) = cause {
            if let Some(amqp) = err.downcast_ref::<AmqpError>() {
                assert!(matches!(amqp, AmqpError::InvalidAddress(_)), "{amqp}");
                return;
            }
            cause = err.source();
        }
        panic!("startup did not fail with an AmqpError: {failed:?}");
    }
}
