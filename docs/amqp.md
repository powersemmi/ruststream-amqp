# AMQP 1.0

`ruststream-amqp` is the AMQP 1.0 broker, built on [`fe2o3-amqp`](https://docs.rs/fe2o3-amqp). The
protocol is an ISO standard, so one crate serves ActiveMQ Artemis, RabbitMQ 4.x, Azure Service Bus,
Amazon MQ, Solace, Apache Qpid, and IBM MQ. For framework concepts (writing subscribers, routing,
codecs, middleware), see the [RustStream documentation](https://powersemmi.github.io/ruststream/).

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-amqp = "0.7"
serde = { version = "1", features = ["derive"] }
```

## The prelude

`use ruststream_amqp::prelude::*;` is the one import a service file writes. It carries the broker,
the address descriptor, the publish policies, the capability traits a handler bounds its slots
with, and the framework's own prelude.

A service is written in two vocabularies. A handler body names capabilities, so
`ruststream::prelude::*` alone is enough there: each slot is bound with the capability that body
needs (`Out<impl Publisher>`, `Out<impl TransactionalPublisher>`, `Out<impl RequestReply>`), and the
concrete publisher comes from the mount site. A routes file names policies: it imports this glob,
where they arrive with the broker prefix stripped:

| Crate root | In the prelude |
|---|---|
| `AmqpPublish` | `Publish` |
| `AmqpTransactionalPublish` (feature `transaction`) | `TransactionalPublish` |

A mount site therefore reads `b.include(handler).out(Reply, Publish)` on every broker, and moving a
service between brokers is a change of one import rather than of every include site. A policy ends
in `Publish` and the capability trait of its live form ends in `Publisher`, so the two vocabularies
never share a name. The prefixed originals stay exported too, for a file that globs two broker
preludes and has to say which `Publish` it means.

## Capabilities

The framework's optional capability traits, and what this broker implements natively:

| Capability | Native | Notes |
| --- | --- | --- |
| `Subscribe` | yes | subscribe by name; the name is the address, sent verbatim |
| `BatchSubscriber` | yes, on the client | [a transfer delivers one message, so the framework's buffer assembles the batches](#batches) |
| `TransactionalPublisher` | yes, with the `transaction` feature | [transactional posting](#transactions), one broker-side transaction per handle |
| `OwnedTransactions` | no | the transaction belongs to the publisher handle, not to a value of its own |
| `RequestReply` | yes | [`reply-to`, `correlation-id`, and a dynamic reply link](#requestreply) |
| `Partitioned` | yes | [the partition key is the `group-id` property](#headers-and-the-partition-key) |
| `Seekable` and `Positioned` | no | the protocol exposes no position a client could seek to |
| `DescribeServer` | yes | reports the host and port from the connection URL, without the credentials it may carry |

## The lifecycle

The broker is a ladder of consuming transitions, so each state is a distinct type:

```text
AmqpBroker::new(url)      configuration only, synchronous, no I/O
  .connect()   ->  ConnectedAmqpBroker      the live connection; subscriptions and publishers
  .shutdown()  ->  ()                       ends the sessions and closes the connection
```

`new` only records the URL, so the service is assembled synchronously and the runtime connects once
at startup. Because `shutdown` consumes the connected broker, publishing or subscribing after it
does not compile. A publisher handed out earlier shares the connection rather than owning it: after
`shutdown` it returns an error rather than succeeding against a closed connection.

You can set authentication and identity on the synchronous form. `sasl` takes a `Sasl::anonymous()`,
`Sasl::plain(user, pass)`, or `Sasl::external()` profile, and `container_id` names the service to
the broker, `"ruststream"` by default. A URL with userinfo (`amqp://user:pass@host`) selects PLAIN
on its own, and an explicit profile wins over it. An `amqps://` endpoint needs the `rustls` or
`native-tls` feature.

Each subscription runs on its own AMQP session, and the publishers share one session of their own.
Flow-control windows are per session, so a slow consumer cannot starve the publishers or another
subscription.

## Addressing

AMQP 1.0 standardises the wire but not the meaning of an address, so `AmqpAddress` names the
intent. Each constructor advertises the matching terminus capability, which is how Artemis and
other products tell them apart:

| Constructor | Semantics | Terminus capability |
| --- | --- | --- |
| `AmqpAddress::queue(name)` | anycast: competing consumers, one delivery each | `queue` |
| `AmqpAddress::topic(name)` | multicast: fan-out to every subscriber | `topic` |
| `AmqpAddress::raw(address)` | verbatim, for a deployment's own convention | none |

Write the descriptor inline in the subscriber attribute:

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_service.rs:handler"
```

The service names the broker once and mounts the handler on it:

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_service.rs:app"
```

Two options sit on the descriptor:

- `credit(nonzero!(n))` sets how many unsettled deliveries the broker may have in flight to this
  subscription. The default is 256. Credit is the protocol's own back-pressure, so a lower value
  bounds the work in flight. A subscription granted no credit receives nothing, so the count is a
  `NonZeroU32` and `credit(0)` does not compile.
- `settle(Settle::AtMostOnce)` switches the subscription to at-most-once delivery, where the
  receiver settles each delivery on receipt.

A descriptor with an empty address is rejected before any I/O.

The plain string form `#[subscriber("orders")]` also works: the name becomes `AmqpAddress::raw`.

## Batches

A handler taking a slice is handed a batch of messages rather than one, and the mount site names
the batch size:

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_batches.rs:handler"
```

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_batches.rs:app"
```

AMQP 1.0 has no batch pull: a transfer delivers one message, and credit is flow control rather than
a batch size. The batches are therefore assembled on the client, and a batch holds at most the size
the mount site named, fewer whenever that is all that arrived in time.

`batch_wait` on the descriptor is the deadline that closes a partial batch, 10 milliseconds by
default. A longer deadline gives fuller batches under a trickle of traffic; under a steady flow the
size closes the batch first and the deadline never fires. Credit and the batch size are separate:
credit bounds what the broker may have in flight, the size is how many messages one handler call
sees.

## Acknowledgement and dispositions

The handler's outcome is the protocol's disposition:

| Handler outcome | Disposition | Effect |
| --- | --- | --- |
| `HandlerOutcome::ack()` | `accept` | the delivery is done, the broker drops it |
| `HandlerOutcome::retry()` | `release` | the delivery returns to the broker for redelivery |
| `HandlerOutcome::drop()` | `reject` | terminal; the broker's dead-letter policy decides |

On an at-most-once subscription the deliveries arrive already settled, so `ack` and `nack` report
`AckError::Unsupported`.

AMQP 1.0 has no delayed redelivery, so `HandlerOutcome::retry_after(delay)` is served by the
runtime's deferred re-publish. That path needs a publisher of its own, wired on the scope with
`retry_via`; without one the delay is dropped and the delivery is released at once.

## Publishing

`AmqpPublish` is the policy that constructs the publisher `AmqpPublisher`. You name the policy
where the handler is mounted, and at startup it instantiates the publisher on the connected broker.
It is also this broker's default policy, so a `#[subscriber(.., publish)]` handler whose mount site
names no reply publisher replies through it.

Write `.out(Reply, Publish)` for the reply, and `.out(<marker>, Publish).build()` for an injected
slot; `Publish` is the policy's [prelude](#the-prelude) name. The policy has no options, so you
write it bare.

A sender link is attached on first use and kept per address. A publish the broker settles with
anything other than `accept` (rejected, released, modified) returns an error, so a broker-side
refusal cannot pass as a successful publish.

## Request/reply

Request/reply is in the protocol, so `AmqpPublisher` implements the `RequestReply` capability.
`request(msg, timeout)` attaches a dynamic receiver link, so the broker assigns a private reply
address to that request alone. It sends the message with `reply-to` and `correlation-id` set, and
resolves with the first reply whose correlation id matches. A request with no reply inside the
timeout returns an error, and the reply link is detached either way.

A request that starts the conversation has no delivery to answer, so it runs from the scope's
`after_startup` hook, where the publisher is already live:

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_request_reply.rs:request"
```

The responder reads the reply address the requester named and publishes the answer there, echoing
`correlation-id` back. That address is new for every request, so the answer goes out through an
injected publisher rather than a returned reply, whose destination is fixed when the service is
written. The slot names the capability the body needs (`Out<impl Publisher>`), and
`b.include(greet).out(DefaultSlot, Publish).build()` binds the policy to the unnamed slot this
handler declares.

The greeting type carries no `#[outgoing(name = ..)]`, so its destination is the one the call site
supplies: here the per-request reply address in `to(..)`.

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_request_reply.rs:responder"
```

Both sides are in
[`examples/amqp_request_reply.rs`](https://github.com/powersemmi/ruststream-amqp/blob/main/crates/ruststream-amqp/examples/amqp_request_reply.rs).

## Transactions

With the `transaction` feature, `AmqpTransactionalPublish` is the policy that constructs the
transactional publisher `AmqpTxnPublisher`, which publishes over the protocol's transactional
posting. `begin_transaction`, `commit` and `abort` live on that publisher alone, so you get them by
naming this policy instead of `Publish`.

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_transaction.rs:transaction"
```

The handle carries at most one broker-side transaction. A second `begin_transaction` while one is
open returns an error and leaves the open transaction untouched, and `commit` or `abort` with
nothing open returns an error too. A commit or abort that returns an error still closes the
transaction, so the next `begin_transaction` starts fresh. A publish outside a transaction goes out
immediately, on the same publisher.

Transactions cover publishing: receiving and settling stay outside them.

## Headers and the partition key

The well-known headers map onto the AMQP `properties` section: `content-type`, `correlation-id`,
`reply-to`, `message-id`, and the partition key as `group-id`. Every other header goes into
`application-properties`. There is no envelope of the framework's own, so a peer on another stack
sees a plain AMQP message, and a message produced by another AMQP client arrives with its headers
intact.

The partition key is the `partition-key` header (exported as `PARTITION_KEY_HEADER`), and a
delivered message implements the `Partitioned` capability.

## Testing

The `testing` feature ships `AmqpTestBroker`: an in-process transport that reproduces the crate's
core routing with no server and no AMQP wire. A test file imports it by its own path,
`use ruststream_amqp::testing::AmqpTestBroker;`, alongside the prelude glob. It follows the same
ladder as the real broker, and the framework's `TestApp` harness runs a service's handlers on it.
See
[Unit-testing a service with TestApp](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp).

The test broker routes by exact address match. Dead-letter policies, credit and redelivery timing
are the server's behaviour: exercise them against a real broker. The crate's own suites run that
way, gated behind `AMQP_TEST_URL`: `just test-brokers` starts ActiveMQ Artemis from
`docker-compose.test.yml` and runs the integration tests plus the conformance lifecycle, batching,
request/reply, and transactions suites against it.

What carries over is the batch size the mount site names: both brokers assemble batches with the
same client-side buffer. The test broker closes a partial batch on the default `batch_wait`.
