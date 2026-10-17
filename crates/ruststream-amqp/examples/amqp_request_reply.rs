//! Native request-reply on `AMQP` 1.0: a dynamic reply link, `reply-to` and `correlation-id`.
//!
//! Both sides live in this one app: the responder is a subscriber, a handler asks it through a
//! request slot, and the scope's `after_startup` hook asks it once the subscriptions are open.
//!
//! ```text
//! cargo test --example amqp_request_reply --features testing
//! cargo run --example amqp_request_reply -- run
//! ```

use std::io;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:responder]
/// The request payload: a name, not a serialized value, so it rides the byte lane and no codec
/// ever sees it.
#[derive(Deserialized)]
struct Who<'a>(&'a [u8]);

/// The reply the responder builds itself. It carries its own bytes and names no destination, so
/// the call site says where it goes, which is what a per-request reply address needs.
#[derive(Outgoing, Serialized)]
struct Greeting(Vec<u8>);

#[subscriber(AmqpAddress::queue("greeter"))]
async fn greet(
    who: &Who<'_>,
    ctx: &mut Context<'_>,
    Out(out): Out<impl Publisher>,
) -> HandlerOutcome {
    // The requester's private reply address; a message without one is no request.
    let Some(reply_to) = ctx.headers().reply_to().map(str::to_owned) else {
        return HandlerOutcome::drop();
    };
    let mut headers = HeaderMap::new();
    if let Some(correlation_id) = ctx.headers().correlation_id() {
        headers.insert("correlation-id", correlation_id.to_owned());
    }

    let name = String::from_utf8_lossy(who.0);
    tracing::info!(%name, %reply_to, "greeting");
    let greeting = Greeting(format!("hello, {name}").into_bytes());
    if out
        .message(&greeting)
        .to(reply_to)
        .with_headers(headers)
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:responder]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "visitors")]
struct Visitor {
    name: String,
}

// --8<-- [start:request]
#[subscriber(AmqpAddress::queue("visitors"))]
async fn welcome(visitor: &Visitor, Out(greeter): Out<impl RequestReply>) -> HandlerOutcome {
    let request = OutgoingMessage::new("greeter", visitor.name.as_bytes());
    match greeter.request(request, Duration::from_secs(5)).await {
        Ok(reply) => {
            let greeting = String::from_utf8_lossy(reply.payload());
            tracing::info!(%greeting, "visitor welcomed");
            HandlerOutcome::ack()
        }
        // `RequestTimeout` when nothing answered in time; the visitor comes back for another try.
        Err(error) => {
            tracing::warn!(%error, visitor = %visitor.name, "the greeter did not answer");
            HandlerOutcome::retry()
        }
    }
}
// --8<-- [end:request]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("request-reply", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(greet).out(DefaultSlot, Publish).build();
            b.include(welcome).out(DefaultSlot, Publish).build();
            // A request that opens a conversation has no delivery to answer, so it runs once the
            // subscriptions are open.
            b.after_startup(Publish, async move |publisher| -> io::Result<()> {
                let request = OutgoingMessage::new("greeter", b"world".as_slice());
                let reply = publisher
                    .request(request, Duration::from_secs(5))
                    .await
                    .map_err(io::Error::other)?;
                let greeting = String::from_utf8_lossy(reply.payload());
                tracing::info!(%greeting, "the greeter is up");
                Ok(())
            });
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_amqp::prelude::*;

    use super::{Visitor, app};

    /// A name on the byte lane, as a requester sends it.
    #[derive(Outgoing, Serialized)]
    struct Name(Vec<u8>);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_startup_request_is_answered() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<AmqpBroker>()
            .subscriber("greeter")
            .assert_called_once()
            .with_raw(b"world")
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_visitor_is_welcomed_through_a_request() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<AmqpBroker>()
            .message(&Visitor {
                name: "ada".to_owned(),
            })
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .subscriber("visitors")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        // The startup request, then the visitor's.
        tb.broker::<AmqpBroker>()
            .subscriber("greeter")
            .assert_called(2)
            .with_raw(b"ada");
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_responder_answers_where_the_request_asked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let mut headers = HeaderMap::new();
        headers.insert("reply-to", "replies.ada");
        headers.insert("correlation-id", "request-1");
        tb.broker::<AmqpBroker>()
            .message(&Name(b"ada".to_vec()))
            .to("greeter")
            .with_headers(headers)
            .publish()
            .await?;

        tb.broker::<AmqpBroker>()
            .published::<()>("replies.ada")
            .assert_called_once()
            .with_raw(b"hello, ada")
            .with_header("correlation-id", "request-1");
        tb.shutdown().await?;
        Ok(())
    }
}
