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

The policies arrive under their concept name, with the broker prefix stripped:

| Crate root | In the prelude |
|---|---|
| `AmqpPublish` | `Publish` |
| `AmqpTransactionalPublish` (feature `transaction`) | `TransactionalPublish` |

A mount site then reads `b.include(handler).publisher(Publish)`. `Publish` is the publish policy,
not the framework's publish builder of the same name that a handler enters with `message(..)` or
`raw(..)`; a policy ends in `Publish` and the capability trait of its live form ends in `Publisher`.

## Capabilities

The framework's optional capability traits, and what this broker implements natively:

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | yes | subscribe by name; the name is sent verbatim, as `AmqpAddress::raw` does |
| `BatchSubscriber` | no | the protocol delivers one message per transfer, and batching is credit, not a batch pull |
| `TransactionalPublisher` | yes, with the `transaction` feature | [transactional posting](#transactions), one broker-side transaction per handle |
| `OwnedTransactions` | no | only the borrowed form is implemented; the client's transactional path covers posting |
| `RequestReply` | yes | [`reply-to`, `correlation-id`, and a dynamic reply link](#requestreply) |
| `Partitioned` | yes | [the partition key rides the `group-id` property](#headers-and-the-partition-key) |
| `Seekable` and `Positioned` | no | the queue position belongs to the broker; the protocol exposes no client-addressable offset to seek to |
| `DescribeServer` | yes | reports the connection host and the `amqp` protocol for the framework's server description |

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

- `credit(n)` sets the protocol-level credit (prefetch): how many unsettled deliveries the broker
  may have in flight to this subscription. The default is 256. Credit is the protocol's own
  back-pressure, so a lower value bounds work in flight without an extra layer.
- `settle(Settle::AtMostOnce)` switches the subscription to at-most-once delivery, where the
  receiver settles on receipt.

A descriptor that cannot form a subscription (an empty address, zero credit) is rejected with
`AmqpError::InvalidAddress` before any I/O.

The plain string form `#[subscriber("orders")]` also works: a by-name source resolves to
`AmqpAddress::raw`, so the address goes to the broker verbatim with no capability attached.

## Acknowledgement and dispositions

Settlement maps onto the protocol's dispositions, with no invented middle layer:

| Handler result | Disposition | Effect |
| --- | --- | --- |
| `HandlerResult::Ack` | `accept` | the delivery is done, the broker drops it |
| `HandlerResult::retry()` | `release` | the delivery returns to the broker for redelivery |
| `HandlerResult::drop()` | `reject` | terminal; the broker's dead-letter policy decides |

On an at-most-once subscription the deliveries arrive already settled, so `ack` and `nack` report
`AckError::Unsupported` instead of a settlement that never reaches the wire.

AMQP 1.0 has no protocol-level delayed redelivery, so `HandlerResult::retry_after(delay)` falls
back to the runtime's broker-agnostic deferred re-publish rather than a broker-side timer.

## Publishing

A publisher is a policy plus the live connection. `AmqpPublish` holds no connection, so it is
constructed anywhere (in a router, in configuration, at a mount site) and the runtime pairs it with
the broker at startup to produce an `AmqpPublisher`. It is also the broker's default publish
policy, so a `#[subscriber(.., publish("dest"))]` handler mounted without an explicit publisher
replies through it.

The mount sites below write it as `Publish`, its [prelude](#the-prelude) name.

Sender links are attached on first use and cached per address. A message the peer settles with
anything other than `accept` (rejected, released, modified) is reported as
`AmqpError::PublishNotAccepted`, so a broker-side refusal cannot pass as a successful publish.

Every publish surface is entered with `message(&value)` for a value or `raw(&bytes)` for a payload
the service already holds encoded, and a bare `AmqpPublisher` reaches those entry points through
the framework's blanket `PublishExt`.

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
needs (`Out<impl Publisher>`); `AmqpPublisher` is inferred from the policy attached at the include
site.

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
integration tests plus the conformance lifecycle, request/reply, and transactions suites against
it, gated behind `AMQP_TEST_URL`.
