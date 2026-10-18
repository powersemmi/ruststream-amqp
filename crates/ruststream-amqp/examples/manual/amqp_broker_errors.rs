//! The service of `examples/amqp_broker_errors.rs` without the macros: the handler is a named type,
//! `subscriber` takes the address descriptor, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_amqp_broker_errors --features testing,asyncapi
//! ```

use std::error::Error;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_amqp::prelude::*;

// --8<-- [start:handler]
struct Ship;

impl<Entry> Handle<Order, (), Outs<(Entry,)>> for Ship
where
    Entry: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let shipment = Shipment { order_id: order.id };
        let sent = outs
            .get(DefaultSlot)
            .message(&shipment)
            .to(order.warehouse.as_str())
            .publish()
            .await;
        if let Err(err) = sent {
            tracing::warn!(subscription = "orders", order.id, error = %err, "shipment not sent");
            if refused_by_the_broker(&err) {
                return Err(HandlerOutcome::drop());
            }
            return Err(HandlerOutcome::retry());
        }
        Ok(())
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

fn app() -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            let ship = subscriber(AmqpAddress::queue("orders"), Ship);
            b.include(ship.build()).out(DefaultSlot, Publish).build();
        },
    )
}

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

/// An order names the warehouse address its shipment goes to.
#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
    warehouse: String,
}

// `#[outgoing(name = "orders")]` by hand: an order names the address it is published to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Shipment {
    order_id: u64,
}

// `#[derive(Outgoing)]` by hand: a shipment names no address, the handler does.
impl OutgoingDestination for Shipment {
    type Form = CallerName;
}

impl MessageHeaders for Shipment {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::future::{Future, ready};

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

    struct Nowhere;

    impl Handle<Order> for Nowhere {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "never reached");
            ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn a_failed_startup_carries_the_amqp_error() {
        let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
            AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
            |b| {
                let nowhere = subscriber(AmqpAddress::queue(""), Nowhere);
                b.include(nowhere.build());
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
