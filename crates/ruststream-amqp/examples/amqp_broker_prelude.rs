//! The prelude: one glob brings the core's prelude, the broker and its authentication, the
//! address descriptor with its settings, and the publish policy under the name every broker crate
//! uses.
//!
//! ```text
//! cargo test --example amqp_broker_prelude --features testing
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream_amqp::prelude::*;

#[subscriber(
    AmqpAddress::queue("orders")
        .credit(nonzero!(64))
        .settle(Settle::AtLeastOnce),
    reply
)]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://broker.internal:5672").sasl(Sasl::anonymous()),
        |b| {
            b.include(confirm).out_reply(Publish);
        },
    )
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
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
