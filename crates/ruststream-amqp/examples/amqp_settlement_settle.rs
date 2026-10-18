//! At-most-once delivery: `Settle::AtMostOnce` settles each delivery on receipt, so the broker is
//! done with a message the moment it hands it over.
//!
//! ```text
//! cargo test --example amqp_settlement_settle --features testing
//! cargo run --example amqp_settlement_settle -- run
//! ```

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "readings")]
struct Reading {
    sensor: u64,
    celsius: f64,
}

/// A stand-in for a time-series store whose shard for sensor 0 is down.
async fn store(reading: &Reading) -> Result<(), &'static str> {
    tokio::task::yield_now().await;
    if reading.sensor == 0 {
        return Err("shard unavailable");
    }
    Ok(())
}

// --8<-- [start:settle]
/// A sensor reports every second, so a lost reading costs nothing. Each delivery is settled on
/// receipt: a reading the store refused is not delivered again.
#[subscriber(AmqpAddress::queue("readings").settle(Settle::AtMostOnce))]
async fn record(reading: &Reading) -> HandlerOutcome {
    if let Err(err) = store(reading).await {
        tracing::warn!(reading.sensor, err, "reading lost");
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:settle]

#[ruststream::app]
fn app() -> impl App {
    let broker = AmqpBroker::new("amqp://artemis:artemis@localhost:5672");
    RustStream::new(AppInfo::new("telemetry", "0.1.0")).with_broker(broker, |b| {
        b.include(record);
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Reading, app};

    #[tokio::test]
    async fn a_stored_reading_is_handled_once() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        let reading = Reading {
            sensor: 1,
            celsius: 21.5,
        };
        tb.broker::<AmqpBroker>()
            .message(&reading)
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("readings")
            .assert_called_once()
            .with(&reading);
        Ok(())
    }

    /// The delivery was settled before the handler ran, so the retry finds nothing to hand back.
    #[tokio::test]
    async fn a_refused_reading_is_not_delivered_again() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .message(&Reading {
                sensor: 0,
                celsius: 21.5,
            })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("readings")
            .assert_called_once()
            .settled(HandlerOutcome::retry());
        Ok(())
    }
}
