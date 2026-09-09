# AMQP 1.0

`ruststream-amqp` is the AMQP 1.0 broker, built on [`fe2o3-amqp`](https://docs.rs/fe2o3-amqp). The
protocol is an ISO standard, so the same crate talks to ActiveMQ Artemis, RabbitMQ 4.x, Azure
Service Bus, Amazon MQ, Solace, Apache Qpid, and IBM MQ. For framework concepts (writing
subscribers, routing, codecs, middleware), see the
[RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-amqp = "0.7"
serde = { version = "1", features = ["derive"] }
```

## The prelude

`use ruststream_amqp::prelude::*;` is the one import a service file writes. It carries the broker,
the address descriptor, the publish policies, the framework capability traits this broker
implements, and the framework's own prelude.

The imports follow the two vocabularies a service is written in. A handler body names
capabilities, so it imports `ruststream::prelude::*` alone and bounds its slots with the broker
capability traits (`Out<impl Publisher>`, `Out<impl TransactionalPublisher>`,
`Out<impl RequestReply>`); the concrete publisher arrives from the mount site. A routes file names
policies, so it imports this glob, where they arrive with the broker prefix stripped:

| Crate root | In the prelude |
|---|---|
| `AmqpPublish` | `Publish` |
| `AmqpTransactionalPublish` (feature `transaction`) | `TransactionalPublish` |

A mount site therefore reads `b.include(handler).out(Reply, Publish)` on every broker, and moving a
service between brokers is a change of one import rather than of every include site. The two
vocabularies never share a name: a policy ends in `Publish`, and the capability trait of its live
form ends in `Publisher`. The prefixed originals stay exported too, for a file that globs two
broker preludes and has to say which `Publish` it means.

## Capabilities

The framework's optional capability traits, and what this broker implements natively:

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | yes | subscribe by name; the name is sent verbatim, as `AmqpAddress::raw` does |
| `BatchSubscriber` | yes, on the client | [a transfer carries one message, so the framework's buffer assembles the batches](#batches) |
| `TransactionalPublisher` | yes, with the `transaction` feature | [transactional posting](#transactions), one broker-side transaction per handle |
| `OwnedTransactions` | no | only the borrowed form is implemented; the client's transactional path covers posting |
| `RequestReply` | yes | [`reply-to`, `correlation-id`, and a dynamic reply link](#requestreply) |
| `Partitioned` | yes | [the partition key rides the `group-id` property](#headers-and-the-partition-key) |
| `Seekable` and `Positioned` | no | the queue position belongs to the broker; the protocol exposes no client-addressable offset to seek to |
| `DescribeServer` | yes | reports the connection host and the `amqp` protocol for the framework's server description |

A delivery carries no broker metadata beyond its own sections, so the per-delivery context stays
the framework's `()` default and this crate publishes no `Ctx` keys; a batch inherits that default,
having no subscription-scoped handle to offer either. What an AMQP message says about itself lives
in the `properties` and `application-properties` sections, which arrive as headers and are read
with `ctx.headers()` or the framework's `Headers<T>` extractor.

## The lifecycle

The broker is a ladder of consuming transitions, so each state is a distinct type:

```text
AmqpBroker::new(url)      configuration only, synchronous, no I/O
  .connect()   ->  ConnectedAmqpBroker      the live connection; subscriptions and publishers
  .shutdown()  ->  ()                       ends the sessions and closes the connection
```

`new` performs no I/O, so an AMQP service is assembled with the same `#[ruststream::app]` macro as
any other broker: the runtime connects once at startup, before opening subscriptions, and closes
the connection at the end. Because `shutdown` consumes the connected broker, publishing or
subscribing after it does not compile. A publisher handed out earlier still aliases the connection,
and reports `AmqpError::NotConnected` once it is gone rather than succeeding against a dead
connection.

Authentication and identity are builder options on the synchronous form: `sasl` takes a
`Sasl::anonymous()`, `Sasl::plain(user, pass)`, or `Sasl::external()` profile, and `container_id`
names the service to the broker (the default is `"ruststream"`). A URL with userinfo
(`amqp://user:pass@host`) selects PLAIN implicitly, and an explicit profile wins over it. TLS
endpoints (`amqps://`) need the `rustls` or `native-tls` feature.

Each subscription runs on its own AMQP session, and publishers share one session of their own.
Flow-control windows are per session, so a slow consumer cannot starve the publishers or another
subscription.

## Addressing

AMQP 1.0 standardises the wire but not the meaning of an address, so `AmqpAddress` makes the
intent explicit. Each constructor advertises the matching terminus capability, which is how
Artemis and other products disambiguate:

| Constructor | Semantics | Terminus capability |
| --- | --- | --- |
| `AmqpAddress::queue(name)` | anycast: competing consumers, one delivery each | `queue` |
| `AmqpAddress::topic(name)` | multicast: fan-out to every subscriber | `topic` |
| `AmqpAddress::raw(address)` | verbatim, for a deployment's own convention | none |

`AmqpAddress` implements `SubscriptionSource`, so the descriptor sits inline in the decorator:

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_service.rs:handler"
```

Wiring it onto the broker is the framework's `with_broker` / `include` pair, identical to every
other broker:

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_service.rs:app"
```

Two options ride the descriptor:

- `credit(nonzero!(n))` sets the protocol-level credit (prefetch): how many unsettled deliveries
  the broker may have in flight to this subscription. The default is 256. Credit is the protocol's
  own back-pressure, so a lower value bounds work in flight without an extra layer. A subscription
  granted no credit receives nothing, so the count is a `NonZeroU32` and `credit(0)` does not
  compile.
- `settle(Settle::AtMostOnce)` switches the subscription to at-most-once delivery, where the
  receiver settles on receipt.

A descriptor with an empty address is rejected with `AmqpError::InvalidAddress` before any I/O.

The plain string form `#[subscriber("orders")]` also works: a by-name source resolves to
`AmqpAddress::raw`, so the address goes to the broker verbatim with no capability attached.

## Batches

A handler taking a slice is handed a batch of messages rather than one, and the mount site names
the batch size:

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_batches.rs:handler"
```

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_batches.rs:app"
```

AMQP 1.0 has no batch pull: a transfer carries one message, and credit is flow control rather than
a batch size. The batches are therefore assembled on the client, by the framework's own buffer, and
a batch holds at most the size the mount site named - fewer whenever that is all that arrived in
time, never more. Nothing at the mount site says which of the two a broker does, which is the point:
the size is the one word either way.

What belongs to this crate is the deadline that closes a partial batch: `batch_wait` on the
descriptor, 10 milliseconds by default. It trades latency for fuller batches under a trickle of
traffic; under a steady flow the size closes the batch first and the deadline never fires. Credit is
a separate dial: it bounds what the broker may have in flight, while the batch size is how many
messages one handler call sees.

## Acknowledgement and dispositions

Settlement maps onto the protocol's dispositions, with no invented middle layer:

| Handler outcome | Disposition | Effect |
| --- | --- | --- |
| `HandlerOutcome::ack()` | `accept` | the delivery is done, the broker drops it |
| `HandlerOutcome::retry()` | `release` | the delivery returns to the broker for redelivery |
| `HandlerOutcome::drop()` | `reject` | terminal; the broker's dead-letter policy decides |

On an at-most-once subscription the deliveries arrive already settled, so `ack` and `nack` report
`AckError::Unsupported` instead of a settlement that never reaches the wire.

AMQP 1.0 has no protocol-level delayed redelivery, so `HandlerOutcome::retry_after(delay)` falls
back to the runtime's broker-agnostic deferred re-publish rather than a broker-side timer.

## Publishing

A publisher is a policy plus the live connection. `AmqpPublish` holds no connection, so it is
constructed anywhere (in a router, in configuration, at a mount site) and the runtime pairs it with
the broker at startup to produce an `AmqpPublisher`. It is also the broker's default publish
policy, so a `#[subscriber(.., publish("dest"))]` handler whose mount site names no reply publisher
replies through it. A mount site that does name one writes `.out(Reply, Publish)` for the reply and
`.out(<marker>, Publish).build()` for an injected slot, `Publish` being the policy's
[prelude](#the-prelude) name. The policy carries no options of its own, so it is written bare;
this crate ships no mount-site settings trait over it.

Sender links are attached on first use and cached per address. A message the peer settles with
anything other than `accept` (rejected, released, modified) is reported as
`AmqpError::PublishNotAccepted`, so a broker-side refusal cannot pass as a successful publish.

Every publish surface is entered with `message(&value)`, and the wire follows the value's type: a
`serde::Serialize` value encodes with the resolved codec, a `#[derive(Serialized)]` newtype carries
bytes the service already holds and they leave as they are. Bytes therefore travel under a name of
their own rather than as an anonymous payload, which is also what puts them in the generated
document. A bare `AmqpPublisher` reaches that entry point through the framework's blanket
`PublishExt`.

A per-message argument of a broker's own goes on the publisher, ahead of the builder entry point:
a step like `publisher.with_x(v)` returns an adapter that implements `Publisher`, captures the
argument, and stamps it onto the `OutgoingMessage` inside its own `publish` before delegating, so
the argument rides the ordinary chain (`publisher.with_x(v).message(&order).publish()`). This crate
ships no such step; its per-message vocabulary is the AMQP properties section, which the framework's
well-known headers already cover.

## Request/reply

AMQP 1.0 carries request/reply natively, so `AmqpPublisher` implements the `RequestReply`
capability. `request(msg, timeout)` attaches a dynamic receiver link (the broker mints a private
reply address), sends the message with `reply-to` and `correlation-id` set, and resolves with the
first reply carrying the matching correlation id. Nothing answering within the timeout is an
`AmqpError::RequestTimeout`, and the reply link is detached either way.

The requester side is a first publish, so it belongs in the scope's `after_startup` hook, where the
publisher arrives live, already paired with the connected broker:

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_request_reply.rs:request"
```

The responder end reads the reply address the requester named and publishes the answer there,
echoing `correlation-id` back. The address is minted per request, so the reply rides an injected
publisher rather than the fixed-destination `publish(..)` form. The slot names the capability it
needs (`Out<impl Publisher>`); `AmqpPublisher` is inferred from the policy the include site binds
to the slot's marker, `b.include(greet).out(DefaultSlot, Publish).build()` for the unnamed slot
this handler declares.

Both ends of the exchange are byte-shaped here, and the payload types say so: the request arrives
as a `#[derive(Deserialized)]` view of the delivery's bytes, so no codec runs on it, and the
greeting the handler builds is a `#[derive(Outgoing, Serialized)]` newtype, so it leaves
byte-for-byte. The derive carries no `name`, which is what opens the `to(..)` position the
per-request reply address fills.

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_request_reply.rs:responder"
```

Both sides of the exchange are in
[`examples/amqp_request_reply.rs`](https://github.com/powersemmi/ruststream-amqp/blob/main/crates/ruststream-amqp/examples/amqp_request_reply.rs).

## Transactions

With the `transaction` feature, `AmqpTransactionalPublish` pairs into an `AmqpTxnPublisher`, which
implements `TransactionalPublisher` over the protocol's transactional posting. The transactional
mode is a separate policy type, so the plain publisher carries no transactional surface at all.

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_transaction.rs:transaction"
```

The handle carries at most one broker-side transaction: a second `begin_transaction` while one is
open is an error that leaves the open transaction untouched, and `commit` or `abort` with nothing
open is an error rather than a silent no-op. A failed discharge still closes the transaction, so
the next `begin_transaction` starts fresh and the handle never wedges. Publishing outside a
transaction goes out immediately, on the same publisher.

The scope is transactional posting only. Transactional retirement (settling deliveries inside a
transaction) and transactional acquisition are not implemented.

## Headers and the partition key

Well-known headers ride the AMQP `properties` section: `content-type`, `correlation-id`,
`reply-to`, `message-id`, and the partition key as `group-id`. Every other header rides
`application-properties`. No envelope format is invented, so a non-Rust peer sees a plain AMQP
message and a message produced by another AMQP client arrives with its headers intact.

The partition key is read and written through the `partition-key` header (exported as
`PARTITION_KEY_HEADER`), which is the same convention the framework's in-memory broker uses, and
delivered messages implement the `Partitioned` capability.

## Testing

The `testing` feature ships `AmqpTestBroker`: an in-process transport that reproduces the crate's
core routing with no server and no AMQP wire. It follows the same ladder as the real broker, and
its connected form implements `ruststream::testing::TestableBroker`, so the same broker drives the
`TestApp` harness and the framework's conformance suite. Inject traffic with
`broker.inject(OutgoingMessage::new(..))` and assert on published output with the free
`ruststream::testing::expect_published`. See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

The test broker routes by exact address match and does not simulate broker-specific behaviour
(dead-letter policies, credit, redelivery timing). Those are verified end to end against a real
broker: `just test-brokers` starts ActiveMQ Artemis from `docker-compose.test.yml` and runs the
integration tests plus the conformance lifecycle, batching, request/reply, and transactions suites
against it, gated behind `AMQP_TEST_URL`.

Batches are the one behaviour the two brokers share verbatim: both assemble them with the
framework's buffer, so a batch handler runs against the test broker exactly as it does against a
server.
