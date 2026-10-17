//! The service of `examples/amqp_broker_prelude.rs` without the macros: the handler is a named
//! type, `subscriber` takes the address descriptor, and `main` is written by hand.
//!
//! ```text
//! cargo test --example manual_amqp_broker_prelude --features testing,asyncapi
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_amqp::prelude::*;

struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation { id: order.id }))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://broker.internal:5672").sasl(Sasl::anonymous()),
        |b| {
            let confirm = subscriber(
                AmqpAddress::queue("orders")
                    .credit(nonzero!(64))
                    .settle(Settle::AtLeastOnce),
                Confirm,
            );
            b.include(confirm.reply().build()).out_reply(Publish);
        },
    )
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
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
struct Confirmation {
    id: u64,
}

// `#[outgoing(name = "confirmations")]` by hand: the reply names the address it goes to.
impl OutgoingDestination for Confirmation {
    type Form = FixedName;
    const DESTINATION: &'static str = "confirmations";
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Confirmation, Order, app};

    #[tokio::test]
    async fn an_order_is_confirmed_on_the_reply_address() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<AmqpBroker>()
            .message(&Order { id: 7 })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker::<AmqpBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { id: 7 });
        Ok(())
    }
}
