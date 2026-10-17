//! A handler's outcome is the delivery's AMQP disposition: `ack` accepts it, `retry` sends
//! `modified` with `delivery-failed` and the broker delivers it again, `drop` rejects it.
//!
//! ```text
//! cargo test --example amqp_settlement_dispositions --features testing
//! cargo run --example amqp_settlement_dispositions -- run
//! ```

use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "payments")]
struct Payment {
    id: u64,
    amount_cents: i64,
}

/// A stand-in for a payment gateway client: it times out on the first charge of every payment
/// and succeeds on the next one.
#[derive(Clone, Default)]
struct PaymentGateway {
    attempted: Arc<Mutex<HashSet<u64>>>,
}

impl PaymentGateway {
    async fn charge(&self, payment: &Payment) -> Result<(), &'static str> {
        tokio::task::yield_now().await;
        let first = self
            .attempted
            .lock()
            .expect("the gateway's bookkeeping is never poisoned")
            .insert(payment.id);
        if first {
            Err("gateway timed out")
        } else {
            Ok(())
        }
    }
}

// `#[derive(FromRef)]` makes each state field injectable with `State<FieldType>`.
#[derive(FromRef)]
struct AppState {
    gateway: PaymentGateway,
}

// --8<-- [start:dispositions]
/// A charged payment is accepted, a timed-out one goes back to the broker for another try, and a
/// malformed one is rejected.
#[subscriber(AmqpAddress::queue("payments"))]
async fn charge(payment: &Payment, State(gateway): State<PaymentGateway>) -> HandlerOutcome {
    if payment.amount_cents <= 0 {
        // No retry can fix it: the broker's dead-letter policy decides where it goes.
        return HandlerOutcome::drop();
    }
    match gateway.charge(payment).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(err) => {
            tracing::warn!(payment.id, err, "charge failed, retrying");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:dispositions]

#[ruststream::app]
fn app() -> impl App<State = AppState> {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("payments", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppState {
                gateway: PaymentGateway::default(),
            })
        })
        .with_broker(broker, |b| {
            b.include(charge);
        })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::{Outcome, TestApp};
    use ruststream_amqp::prelude::*;

    use super::{Payment, app};

    #[tokio::test]
    async fn a_timed_out_charge_is_delivered_again_and_accepted() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let payment = Payment {
            id: 1,
            amount_cents: 1_000,
        };
        tb.broker::<AmqpBroker>()
            .message(&payment)
            .publish()
            .await?;

        assert_eq!(
            tb.broker::<AmqpBroker>()
                .subscriber("payments")
                .assert_called(2)
                .with(&payment)
                .outcomes(),
            [Outcome::Nack, Outcome::Ack],
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_malformed_payment_is_rejected_and_not_delivered_again() -> Result<(), Box<dyn Error>>
    {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&Payment {
                id: 2,
                amount_cents: 0,
            })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("payments")
            .assert_called_once()
            .settled(HandlerOutcome::drop());
        Ok(())
    }
}
