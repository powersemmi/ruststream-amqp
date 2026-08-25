//! Native request/reply: a dynamic reply link, `reply-to`, and `correlation-id`.
//!
//! Both sides live in this one app: the responder is a subscriber, and the request runs from the
//! scope's `after_startup` hook, once the subscription is open.
//!
//! Run a broker (`just brokers-up`), then:
//! `cargo run --example amqp_request_reply -- run`

use std::io;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream_amqp::prelude::*;

// --8<-- [start:responder]
/// The responder. The reply goes to the address the requester named in `reply-to`, which the
/// broker mints per request, and echoes `correlation-id` back.
#[subscriber(AmqpAddress::queue("greeter"), raw)]
async fn greet(name: &[u8], ctx: &mut Context<'_>, Out(out): Out<impl Publisher>) -> HandlerResult {
    let Some(reply_to) = ctx.headers().reply_to().map(str::to_owned) else {
        return HandlerResult::drop();
    };
    let mut headers = Headers::new();
    if let Some(correlation_id) = ctx.headers().correlation_id() {
        headers.insert("correlation-id", correlation_id.to_owned());
    }

    let payload = format!("hello, {}", String::from_utf8_lossy(name));
    if out
        .raw(payload.as_bytes())
        .to(reply_to)
        .with_headers(headers)
        .publish()
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}
// --8<-- [end:responder]

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("request-reply", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672")
            .container_id("request-reply-example"),
        |b| {
            b.include(greet).publisher(Publish);
            // --8<-- [start:request]
            b.after_startup(Publish, async move |publisher| -> io::Result<()> {
                let reply = publisher
                    .request(
                        OutgoingMessage::new("greeter", b"world".as_slice()),
                        Duration::from_secs(5),
                    )
                    .await
                    .map_err(io::Error::other)?;
                println!("reply: {}", String::from_utf8_lossy(reply.payload()));
                Ok(())
            });
            // --8<-- [end:request]
        },
    )
}
