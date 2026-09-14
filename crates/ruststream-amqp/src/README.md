`AMQP` 1.0 broker for the [`RustStream`](https://docs.rs/ruststream) messaging framework.

`AMQP` 1.0 is an ISO standard rather than a product, so one crate serves the whole family:
`ActiveMQ` Artemis and Classic, `RabbitMQ` 4.x, Azure Service Bus and Event Hubs, Amazon MQ,
Solace, Apache Qpid, IBM MQ. The transport is [`fe2o3-amqp`](https://docs.rs/fe2o3-amqp);
handlers, routers, codecs and middleware stay the framework's. On `RabbitMQ` the 1.0 stack is a
protocol of its own, separate from the 0.9.1 that
[`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin) speaks.

The protocol standardises the wire and leaves the meaning of an address to the deployment, so
this crate makes the intent explicit: a subscription names a queue, a topic, or a verbatim
address, and the terminus capability tells the broker which. There is no envelope of the
framework's own, so a peer written against any other `AMQP` client sees a plain message.

# The first service

Add the crate, write a handler, mount it on the broker:

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-amqp = "0.7"
serde = { version = "1", features = ["derive"] }
```

```
# mod demo {
use ruststream_amqp::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(AmqpAddress::queue("orders"))]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(handle);
        },
    )
}
# }
# fn main() {}
```

`cargo run -- run` starts it. [`AmqpBroker::new`] only records the URL, so the application is
assembled synchronously and the runtime dials once at startup. The ladder is
`AmqpBroker::new(url)` to [`ConnectedAmqpBroker`] to the end of the run: `shutdown` consumes the
connected form, so publishing or subscribing after it does not compile. A publisher handed out
earlier shares the connection rather than owning it, so after `shutdown` it reports
[`AmqpError::NotConnected`] instead of succeeding against a closed connection.

Runnable versions of every topic below live in
[`examples/`](https://github.com/powersemmi/ruststream-amqp/tree/main/crates/ruststream-amqp/examples).

# Subscribing

[`AmqpAddress`] is the subscription descriptor: one type carrying the address, the terminus it
asks for, and every setting of the link. The constructor is the intent, and the terminus
capability it advertises is how Artemis and its peers tell the two apart:

| Constructor | Semantics | Terminus capability |
|---|---|---|
| [`AmqpAddress::queue`] | anycast: competing consumers, one delivery each | `queue` |
| [`AmqpAddress::topic`] | multicast: fan-out, a copy per subscriber | `topic` |
| [`AmqpAddress::raw`] | verbatim, for a deployment's own convention | none |

Two settings ride the descriptor. [`credit`](AmqpAddress::credit) is the protocol's own
back-pressure: how many unsettled deliveries the broker may have in flight, 256 by default. The
count is a `NonZeroU32`, because a subscription granted no credit receives nothing, so
`credit(nonzero!(0))` does not compile. [`settle`](AmqpAddress::settle) switches the subscription
to [`Settle::AtMostOnce`], where the receiver settles on receipt and the message is gone whether
the handler finished or not. An empty address is rejected before any I/O.

The plain `#[subscriber("orders")]` form works too, and maps to [`AmqpAddress::raw`]: the name is
sent verbatim, with no capability, so a server consults its own configuration for the topology.
Prefer the descriptor wherever the topology matters.

The protocol exposes no position a client could seek to, so this broker implements neither
`Seekable` nor `Positioned` and `.start_at(..)` does not compile on it.

## Acknowledgement

The handler's outcome is the protocol's disposition:

| Handler outcome | Disposition | Effect |
|---|---|---|
| `HandlerOutcome::ack()` | `accept` | the delivery is done, the broker drops it |
| `HandlerOutcome::retry()` | `modified` with `delivery-failed` | the attempt is counted, the delivery comes back |
| `HandlerOutcome::drop()` | `reject` | terminal; the broker's dead-letter policy decides |

A retry is `modified` rather than `released` on purpose. A release says the delivery was not acted
upon and leaves `delivery-count` where it was; a handler that asked for a retry did act on it and
failed, and a count that never moves lets a poison message circulate under a cap forever. On an
at-most-once subscription the deliveries arrive already settled, so `ack` and `nack` report
`AckError::Unsupported`.

How many times a message has been delivered is `delivery-count` in the `AMQP` `header` section,
and a delivery reports it as `redelivery_count`, counting the current one. A message this crate
published carries no `header` section at all, so nothing has counted an attempt for it and the
framework's own retry-count header decides instead. Both counts start the first delivery at one.

## Capping the retries

Nothing in `AMQP` 1.0 declares a node with a delivery limit and a dead-letter address, so the cap
is the runtime's and reads the same here as on every broker:

```
# mod demo {
use std::time::Duration;

use ruststream::runtime::{Outgoing, PublishContext};
use ruststream_amqp::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(AmqpAddress::queue("orders").credit(nonzero!(64)))]
async fn settle(order: &Order) -> HandlerOutcome {
    if order.id == 0 {
        // Not ready yet: ask for the delivery again in half a minute.
        return HandlerOutcome::retry_after(Duration::from_secs(30));
    }
    HandlerOutcome::ack()
}

#[subscriber(AmqpAddress::topic("events").settle(Settle::AtMostOnce))]
async fn audit(orders: &[Order]) -> HandlerOutcome {
    println!("auditing {} orders", orders.len());
    HandlerOutcome::ack()
}

/// Stamps a deferred copy with the address it came from, so a redelivery is recognisable.
struct Retried;

impl<C, Options> PublishTransform<ForReply<C>, Options> for Retried {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut()
            .insert("x-retried-from", cx.name().to_owned());
    }
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(settle)
                .max_attempts(nonzero!(5u32))
                .dead_letter("orders.dead")
                .out_retry(Publish)
                .transform(Retried);
            b.include(audit.batch(nonzero!(32)));
        },
    )
}
# }
# fn main() {}
```

`.max_attempts(n)` is how many deliveries one message gets, counting the first. `.dead_letter(name)`
is the address a spent delivery is published to, as it arrived; a cap with no destination rejects
the delivery instead and leaves the broker's own dead-letter policy in play. A destination with no
cap takes over every copy, so a handler asking for a retry has its delivery carried away rather
than sent back.

## Delayed redelivery

`AMQP` 1.0 has no delayed redelivery, so `HandlerOutcome::retry_after(delay)` is served by the
runtime's deferred re-publish: the delivery is dropped and a copy is published once the delay is
over. Both subscription forms answer
[`AddressedCopies`](https://docs.rs/ruststream/latest/ruststream/struct.AddressedCopies.html),
and the address they name is their own: one `AMQP` node is what a receiver attaches its source to
and what a sender attaches its target to, so a subscription here always knows where a publisher
reaches it again. The mount site is left nothing to name, and a transform declaring `Names` on
that position does not compile. On a `queue` address the copy competes for consumers like any
other message; on a `topic` address every subscriber sees it. The copy is at-most-once over the
delay window: if the process exits before the timer fires, it is lost.

Every registration already carries a publisher for its copies, taken from this broker's default
policy. `.out_retry(Publish)` above replaces it, and the steps after it are that slot's steps,
which is where a stamp on redeliveries belongs: the copy has no call site of its own and nothing
else on the chain sees it.

## Batches

A handler taking a slice is handed a batch, and `.batch(n)` at the mount site names the size.
`AMQP` 1.0 has no batch pull: a transfer delivers one message and credit is flow control, not a
batch size. The batches are therefore assembled on the client, and one holds at most the size the
mount site named, fewer whenever that is all that arrived in time.
[`batch_wait`](AmqpAddress::batch_wait) is the deadline that closes a partial batch, 10
milliseconds by default. Credit and the batch size stay separate: credit bounds what the broker
may have in flight, the size is how many messages one handler call sees.

## The per-delivery context

The well-known headers map onto the `AMQP` `properties` section: `content-type`,
`correlation-id`, `reply-to`, `message-id`, and the partition key as `group-id`. Every other
header travels in `application-properties`, so a message produced by another `AMQP` client arrives
with its headers intact. The partition key rides the [`PARTITION_KEY_HEADER`] header, and a
delivered message implements the `Partitioned` capability. That trait stays out of the prelude:
in scope it makes `msg.partition_key()` ambiguous with the method of the same name on
`IncomingMessage`, so a service that reads partition keys imports it itself.

# Publishing

[`AmqpPublish`] is the policy that constructs [`AmqpPublisher`]. It has no fields, so it is
written bare, and it is this broker's default policy: a `#[subscriber(.., publish)]` handler whose
mount site names no reply publisher replies through it. Name it where you want it explicitly with
`.out_reply(Publish)` for a reply, `.out_retry(Publish)` for the deferred copy, and
`.out(marker, Publish).build()` for a slot the body publishes through. `Publish` is the policy's
[prelude](#the-prelude) name.

Where a reply goes is a property of the reply type. A type deriving `Outgoing` with
`#[outgoing(name = "receipts")]` fixes its destination and the subscriber writes the bare
`publish` clause; a type that names nothing takes the mount site's `publish("dest")`, or the call
site's `.to(address)` when the body publishes it through a slot. The second form is what a
per-request reply address needs, since that address is new for every request:

```
# mod demo {
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u64,
}

/// Fixes its own destination, so the mount site adds nothing.
#[derive(Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

/// Names nothing, so the call site says where it goes.
#[derive(Serialize, Outgoing)]
struct Greeting {
    text: String,
}

#[subscriber(AmqpAddress::queue("orders"), publish)]
async fn issue_receipt(order: &Order) -> Receipt {
    Receipt { order_id: order.id }
}

#[subscriber(AmqpAddress::queue("greeter"))]
async fn greet(
    order: &Order,
    ctx: &mut Context<'_>,
    Out(out): Out<impl Publisher>,
) -> HandlerOutcome {
    // The requester named a private reply address; the answer echoes the correlation id back.
    let Some(reply_to) = ctx.headers().reply_to().map(str::to_owned) else {
        return HandlerOutcome::drop();
    };
    let mut headers = HeaderMap::new();
    if let Some(correlation_id) = ctx.headers().correlation_id() {
        headers.insert("correlation-id", correlation_id.to_owned());
    }
    let greeting = Greeting {
        text: format!("hello, order {}", order.id),
    };
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

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(issue_receipt).out_reply(Publish);
            b.include(greet).out(DefaultSlot, Publish).build();
        },
    )
}
# }
# fn main() {}
```

A sender link is attached on first use and kept per address. A publish the peer settles with
anything other than `accept` returns [`AmqpError::PublishNotAccepted`], so a broker-side refusal
cannot pass as a successful publish.

## Per-message settings

There are none: `Options` is `()` on every publisher this crate ships, and no publish-builder step
is added. `AMQP` 1.0 does define fields that would qualify - `durable`, `priority` and `ttl` - but
they live in the `header` section, and this crate sends no `header` section at all. A handler body
therefore keeps `Out<impl Publisher>` and imports nothing from this crate.

## Request/reply

Request/reply is in the protocol, so [`AmqpPublisher`] implements `RequestReply`.
`request(msg, timeout)` attaches a dynamic receiver link, so the broker assigns a private reply
address to that request alone; the message goes out with `reply-to` and `correlation-id` set, and
the call resolves with the first reply whose correlation id matches. A request with no reply inside
the timeout returns [`AmqpError::RequestTimeout`], and the reply link is detached either way.

A request that starts the conversation has no delivery to answer, so it runs from the scope's
`b.after_startup(Publish, hook)`, where the publisher is already live. Both sides are in
[`examples/amqp_request_reply.rs`](https://github.com/powersemmi/ruststream-amqp/blob/main/crates/ruststream-amqp/examples/amqp_request_reply.rs).

## Transactions

With the `transaction` feature, `AmqpTransactionalPublish` (`TransactionalPublish` in the prelude)
is the policy that constructs `AmqpTxnPublisher`, which publishes over the protocol's
transactional posting. `begin_transaction`, `commit` and `abort` live on that publisher alone, so
naming this policy instead of `Publish` is how a body gets them:

```
# #[cfg(feature = "transaction")]
# mod demo {
use std::io;

use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Outgoing)]
#[outgoing(name = "invoices")]
struct Invoice {
    id: u64,
}

#[subscriber(AmqpAddress::queue("invoices"))]
async fn handle(invoice: &Invoice) -> HandlerOutcome {
    println!("got invoice {}", invoice.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(handle);
            b.after_startup(
                TransactionalPublish,
                async move |publisher| -> io::Result<()> {
                    publisher
                        .begin_transaction()
                        .await
                        .map_err(io::Error::other)?;
                    for id in 1..=3_u64 {
                        if let Err(error) = publisher.message(&Invoice { id }).publish().await {
                            publisher.abort().await.map_err(io::Error::other)?;
                            return Err(io::Error::other(error));
                        }
                    }
                    publisher.commit().await.map_err(io::Error::other)
                },
            );
        },
    )
}
# }
# fn main() {}
```

This is the borrowed transaction kind: the handle carries at most one broker-side transaction, so
`OwnedTransactions` is not implemented and a transaction is not a value of its own. A second
`begin_transaction` while one is open returns an error and leaves the open transaction untouched;
`commit` or `abort` with nothing open returns an error too. A commit or abort that fails has still
consumed the transaction, so the next `begin_transaction` starts fresh. A publish outside a
transaction goes out immediately, on the same publisher. Transactions cover publishing:
transactional retirement (settling deliveries) and acquisition are out of scope, since the
client's tested path is transactional posting.

# The prelude

`use ruststream_amqp::prelude::*;` is the one import a service file writes. It re-exports the
framework's own prelude plus this crate's surface: [`AmqpBroker`] and [`Sasl`], [`AmqpAddress`]
and [`Settle`], [`AmqpError`], the publish policies, and the capability traits `RequestReply` and
(with the `transaction` feature) `TransactionalPublisher`.

A service is written in two vocabularies. A handler body names capabilities, so
`ruststream::prelude::*` alone is enough there: a slot is bound with the capability the body needs
(`Out<impl Publisher>`, `Out<impl TransactionalPublisher>`, `Out<impl RequestReply>`), and the
concrete publisher arrives from the mount site. A routes file names policies, and imports this
glob, where they arrive with the broker prefix stripped: [`AmqpPublish`] is `Publish`, and
`AmqpTransactionalPublish` is `TransactionalPublish`. So `b.include(handler).out_reply(Publish)`
reads the same on every broker, and moving a service between brokers is a change of one import
rather than of every mount site. The prefixed originals stay exported for a file that globs two
broker preludes and has to say which `Publish` it means.

The one exception other brokers make - a body importing the broker prelude to adjust a
per-message setting - does not arise here, because this crate has no per-message settings.

# The generated document

The `asyncapi` feature forwards the core's, so a service on this broker publishes an
[`AsyncAPI` document](https://docs.rs/ruststream/latest/ruststream/asyncapi/index.html) of its
own. The specification does define an `amqp1` binding, and it reserves all four of its objects:
each must carry no properties. What this crate knows therefore travels in one extension object,
`x-ruststream-amqp1`, at the level the binding would have sat at.

The server says which protocol it is. `amqp` is the specification's key for `AMQP` 0.9.1, and the
two protocols share the scheme and the port, so the key `amqp1` and the `protocolVersion` beside
it are what tell a reader, or a tool generating a client, which one the service speaks. The
extension carries the container id the service presents on the connection.

A subscription's channel carries the node address, the terminus capability it asked for (absent on
a `raw` address), the link credit and the delivery guarantee. A channel a publish reaches carries
the address of the node its sender attaches its target to: a reply destination, the name of an
`Out` slot, a dead-letter address. A send operation says how it posts, `confirmed` or
`transactional`. A reply has no send operation of its own; what a reply policy does contribute is
where a client reads the address of an answer, `$message.header#/reply-to`.

Nothing here is read from a connection, because the document is built before anything connects, so
a value only the live connection knows has no place in it. Neither has a credential:
`DescribeServer` reports the host and port from the URL and drops any userinfo it carries, which
the conformance suite checks by configuring the broker with a password and scanning what it
produces.

# Testing

The `testing` feature ships `AmqpTestBroker`, an in-process transport that reproduces this crate's
behaviour with no server and no `AMQP` wire. A test file imports it by its own path,
`use ruststream_amqp::testing::AmqpTestBroker;`, alongside the prelude glob. It follows the same
ladder as the real broker and drives the core's
[`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/index.html) harness:

```
# #[cfg(feature = "testing")]
# mod demo {
use ruststream::testing::TestApp;
use ruststream_amqp::prelude::*;
use ruststream_amqp::testing::AmqpTestBroker;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber(AmqpAddress::queue("orders"))]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

pub async fn an_order_reaches_the_handler() {
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(AmqpTestBroker::new(), |b| {
            b.include(handle);
        });
    let app = TestApp::start(app).await.expect("startup failed");

    app.broker::<AmqpTestBroker>()
        .message(&Order { id: 7 })
        .to("orders")
        .publish()
        .await
        .expect("publish failed");
    app.settle().await.expect("the run settles");

    app.broker::<AmqpTestBroker>()
        .subscriber("orders")
        .assert_called_once();
}
# }
# fn main() {}
```

The whole production declaration resolves against it, so the test runs the wiring the service
ships rather than a rewritten copy: `#[subscriber(AmqpAddress::queue("orders"))]` mounts
unchanged, `.out_reply(Publish)` mounts the production policy, and `AmqpTransactionalPublish`
pairs into an in-process publisher that buffers until the commit. There is no test-only policy to
swap in, and no capability that exists on one broker and not the other.

Behaviour crosses over with them. The terminus decides delivery here as it does on a server, so a
work-queue service cannot pass in process what a broker would fail; an `AmqpAddress::raw` address
declares no capability and the stand-in, having no configuration to consult, delivers each message
once, so say `topic` where the broadcast is the thing being asserted. An at-most-once delivery
arrives settled and its `ack` reports `AckError::Unsupported`. Batches come from the same
client-side buffer with the descriptor's own `batch_wait`. A transaction publishes nothing before
its commit and discards its buffer on an abort. A request carries `reply-to` and `correlation-id`
and fails with `AmqpError::RequestTimeout` when nothing answers.

What is left out is what a broker holds and a process cannot, and each one makes an assertion
unsound rather than merely imprecise, so it belongs in the live suite instead: no storage (a
message published where nothing subscribes is dropped, so open the subscriptions first), no
broker-side redelivery (a requeued delivery returns to the subscription that had it, and there is
no dead-letter policy behind `nack(requeue = false)`), no durability, no flow control (`credit`
has no counterpart and subscriptions here are unbounded), and no refusal (a request nothing
consumes times out rather than being rejected). `just test-brokers` starts `ActiveMQ` Artemis from
`docker-compose.test.yml` and runs the integration tests and every conformance suite against it.

# Operations

Authentication is [`Sasl`]: `Sasl::anonymous()`, `Sasl::plain(user, password)`, or
`Sasl::external()` for a credential established outside SASL, typically a TLS client certificate.
A URL with userinfo (`amqp://user:pass@host`) selects PLAIN on its own, and an explicit profile
passed to [`AmqpBroker::sasl`] wins over it.

An `amqps://` endpoint needs the `rustls` or the `native-tls` feature, which is forwarded to the
client; the crate adds no TLS configuration of its own.

[`AmqpBroker::container_id`] names the service to the broker, `"ruststream"` by default, which is
how an operator finds its links in the broker's own console.

Each subscription runs on its own `AMQP` session and the publishers share one session of their own,
because flow-control windows are per session: a slow consumer cannot starve the publishers or
another subscription. Link names are unique per connection, derived from the container id.

Shutdown runs inwards, links then session then connection, because each layer has to still route
the peer's answer to the one inside it. Every step runs even after an earlier one fails, and the
error reported is the innermost one.

Known gaps: no per-message settings, no positions and no seeking, transactions cover publishing
only, and a batch is assembled on the client rather than pulled. A delivery whose body is an
`AMQP` value section that is neither binary nor a string is rejected as
[`AmqpError::UnsupportedBody`], since a handler can never see bytes for it.
