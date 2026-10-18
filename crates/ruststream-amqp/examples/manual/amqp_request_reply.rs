//! The service of `examples/amqp_request_reply.rs` written without `#[subscriber]`: the responder
//! and the requesting handler implement `Handle`, and each declares its slot in the injections
//! arena.
//!
//! ```text
//! cargo test --example manual_amqp_request_reply --features testing
//! cargo run --example manual_amqp_request_reply -- run
//! ```

use std::io;
use std::process::ExitCode;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
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

struct Greet;

impl<'p, Entry> Handle<Who<'p>, (), Outs<(Entry,)>> for Greet
where
    Entry: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        who: &Who<'p>,
        outs: &Outs<(Entry,)>,
        ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        // The requester's private reply address; a message without one is no request.
        let Some(reply_to) = ctx.headers().reply_to().map(str::to_owned) else {
            return Err(HandlerOutcome::drop());
        };
        let mut headers = HeaderMap::new();
        if let Some(correlation_id) = ctx.headers().correlation_id() {
            headers.insert("correlation-id", correlation_id.to_owned());
        }

        let name = String::from_utf8_lossy(who.0);
        tracing::info!(%name, %reply_to, "greeting");
        let greeting = Greeting(format!("hello, {name}").into_bytes());
        outs.get(DefaultSlot)
            .message(&greeting)
            .to(reply_to)
            .with_headers(headers)
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
}
// --8<-- [end:responder]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "visitors")]
struct Visitor {
    name: String,
}

// --8<-- [start:request]
struct Welcome;

impl<Entry> Handle<Visitor, (), Outs<(Entry,)>> for Welcome
where
    Entry: OutEntry<DefaultSlot, Wire: RequestReply>,
{
    async fn handle(
        &self,
        visitor: &Visitor,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let request = OutgoingMessage::new("greeter", visitor.name.as_bytes());
        match outs
            .get(DefaultSlot)
            .request(request, Duration::from_secs(5))
            .await
        {
            Ok(reply) => {
                let greeting = String::from_utf8_lossy(reply.payload());
                tracing::info!(%greeting, "visitor welcomed");
                Ok(())
            }
            // `RequestTimeout` when nothing answered in time; the visitor comes back for another
            // try.
            Err(error) => {
                tracing::warn!(%error, visitor = %visitor.name, "the greeter did not answer");
                Err(HandlerOutcome::retry())
            }
        }
    }
}
// --8<-- [end:request]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("request-reply", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            let greet = subscriber(AmqpAddress::queue("greeter"), Greet);
            b.include(greet.build()).out(DefaultSlot, Publish).build();
            let welcome = subscriber(AmqpAddress::queue("visitors"), Welcome);
            b.include(welcome.build()).out(DefaultSlot, Publish).build();
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

fn main() -> ExitCode {
    run_main(app)
}

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
        // The startup hook's request is a reaction the harness did not start: the greeter's
        // settlement is recorded after its reply reaches the hook, so the test waits for it.
        tb.settle().await?;

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
