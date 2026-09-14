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

`use ruststream_amqp::prelude::*;` is the one import a service file writes. It carries the broker
and its `Sasl` profile, the address descriptor and its `Settle` guarantee, the publish policies,
`AmqpError`, the capability traits this broker implements (`RequestReply`, and
`TransactionalPublisher` with the `transaction` feature), and the framework's own prelude.
`Partitioned` stays out: in scope it makes `msg.partition_key()` ambiguous with the method of the
same name on `IncomingMessage`. A service that reads partition keys imports `Partitioned` itself.

A service is written in two vocabularies. A handler body names capabilities, so
`ruststream::prelude::*` alone is enough there: each slot is bound with the capability that body
needs (`Out<impl Publisher>`, `Out<impl TransactionalPublisher>`, `Out<impl RequestReply>`), and the
concrete publisher comes from the mount site. A routes file names policies: it imports this glob,
where they arrive with the broker prefix stripped:

| Crate root | In the prelude |
|---|---|
| `AmqpPublish` | `Publish` |
| `AmqpTransactionalPublish` (feature `transaction`) | `TransactionalPublish` |

A mount site therefore reads `b.include(handler).out_reply(Publish)` on every broker, and moving a
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
| `DescribeServer` | yes | [the host and port from the connection URL under the `amqp1` protocol key, without the credentials the URL may carry](#the-generated-document) |
| Per-message publish settings | none | [a publish carries no `header` section, so there is nothing for a call site to adjust](#publishing) |

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
| `HandlerOutcome::retry()` | `modified` with `delivery-failed` | the broker counts the attempt and redelivers |
| `HandlerOutcome::drop()` | `reject` | terminal; the broker's dead-letter policy decides |

A retry is `modified` rather than `released` because the two say different things about the
attempt. `released` means the delivery was not acted upon, and the peer leaves `delivery-count`
where it was; a handler that asked for a retry did act on it and failed. Counting the attempt is
what lets a cap end a message that never settles.

On an at-most-once subscription the deliveries arrive already settled, so `ack` and `nack` report
`AckError::Unsupported`.

How many times a message has been delivered is the `delivery-count` field of the AMQP `header`
section, and the framework reads it to apply a cap. A message this crate published carries no
`header` section at all, so nothing has counted an attempt for it and the framework's own
`x-ruststream-retry-count` header decides instead. Both counts start the first delivery at one.

### Capping the retries

A handler that keeps asking for another try circulates its message until an operator intervenes.
Two steps right after `include` end that, and they read the same on every broker:

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_retry.rs:declaration"
```

`max_attempts(n)` is how many deliveries one message gets, counting the first. `dead_letter(name)`
is the address a spent delivery is published to, as it arrived. A cap with no destination rejects
the delivery instead, which leaves the broker's own dead-letter policy in play where the deployment
configured one. A destination with no cap takes over every copy: a handler that asks for a retry
has its delivery carried away rather than sent back.

### Delayed redelivery

AMQP 1.0 has no delayed redelivery, so `HandlerOutcome::retry_after(delay)` is served by the
runtime's deferred re-publish: the delivery is dropped and a copy of it is published once the delay
is over.

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_retry.rs:handler"
```

The copy goes to the subscription's own address. One AMQP node is both what a receiver attaches to
and what a sender publishes to, so a subscription here always knows where a publisher reaches it
again - the descriptor form and the plain `#[subscriber("orders")]` form alike. The mount site is
left nothing to name, and a transform that names a destination per delivery does not compile on
this broker. On a `queue` address the copy competes for consumers like any other message; on a
`topic` address every subscriber sees it. The copy is at-most-once over the delay window: if the
process exits before the timer fires, it is lost.

The publisher that copy leaves through is on every registration already, taken from this broker's
default policy. Naming one replaces it, once per registration:

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_retry.rs:customised"
```

That position is an `Out` slot, so the steps after it are the slot steps. The copy has no call site
of its own and nothing else on the chain sees it, so a stamp that marks a redelivery goes here:

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_retry.rs:transform"
```

## Publishing

`AmqpPublish` is the policy that constructs the publisher `AmqpPublisher`. You name the policy
where the handler is mounted, and at startup it instantiates the publisher on the connected broker.
It is also this broker's default policy, so a `#[subscriber(.., publish)]` handler whose mount site
names no reply publisher replies through it.

Write `.out_reply(Publish)` for the reply, `.out_retry(Publish)` for the deferred copy, and
`.out(<marker>, Publish).build()` for an injected slot; `Publish` is the policy's
[prelude](#the-prelude) name. The policy has no fields, so you write it bare.

A single publish has no settings of its own either: `AmqpPublisher::Options` is `()`. Some brokers
let a call site adjust one message - a priority, a time to live - with a step on the publish
builder. This one ships no such step, because it sends no AMQP `header` section, and that section
is where `durable`, `priority` and `ttl` live. A handler body therefore keeps `Out<impl Publisher>`
and imports nothing from this crate.

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

## The generated document

The `asyncapi` feature forwards the core's, so a service on this broker publishes an AsyncAPI
document of its own. The specification does have an `amqp1` binding, and it reserves all four of
its objects: each one must carry no properties. What this crate knows therefore travels in one
extension object, `x-ruststream-amqp1`, at the level the binding would have sat at.

```json
--8<-- "crates/ruststream-amqp/tests/asyncapi_excerpt.json"
```

The server says which protocol it is. `amqp` is the specification's key for AMQP 0.9.1, and the two
protocols share the scheme and the port, so the key `amqp1` and the version beside it are what tell
a reader - or a tool generating a client - which one the service speaks. The extension carries the
container id the service presents on the connection.

A subscription's channel carries the AMQP node's address, the terminus capability it asks for
(absent on a `raw` address, which asks for none), the link credit, and the delivery guarantee. A
channel a publish reaches carries the address of the AMQP node its sender attaches its target to:
the reply destination, the name of an `Out` slot, the dead-letter address. A
publisher's send operation says how it posts: `confirmed` waits for the peer's disposition on every
transfer, `transactional` posts under a broker-side transaction. A reply has no send operation of
its own, so a reply policy contributes nothing there; what it does contribute is where a client
reads the address of an answer, `$message.header#/reply-to`, which the document reports wherever
the mount site names the reply destination per delivery.

None of this is read from a connection: the document is built before anything connects, so a value
only the live connection knows has no place in it. Neither has a credential, which the conformance
suite checks by configuring the broker with a password and scanning what it produces.

## Testing

The `testing` feature ships `AmqpTestBroker`: an in-process transport that reproduces this crate's
behaviour with no server and no AMQP wire. A test file imports it by its own path,
`use ruststream_amqp::testing::AmqpTestBroker;`, alongside the prelude glob. It follows the same
ladder as the real broker, and it drives the `TestApp` harness. See
the [`testing` module overview](https://docs.rs/ruststream/latest/ruststream/testing/index.html#examples).

The whole production declaration resolves against the test broker, so the test runs the wiring the
service ships rather than a rewritten copy of it. `#[subscriber(AmqpAddress::queue("orders"))]`
mounts on `AmqpTestBroker` unchanged, `.out_reply(Publish)` mounts the production policy, and
`AmqpTransactionalPublish` pairs into an in-process publisher that buffers until the commit. There
is no test-only policy to swap in at the mount site, and no capability that exists on one broker
and not the other: `RequestReply` and `TransactionalPublisher` are carried over, so a handler that
binds `Out<impl RequestReply>` or `Out<impl TransactionalPublisher>` mounts in process too.

Behaviour crosses over with them, because a test that cannot fail is worth nothing. The terminus
decides delivery here as it does on a server: `AmqpAddress::queue` subscriptions on one address
compete for each message, `AmqpAddress::topic` subscriptions each get a copy, so a work-queue
service cannot pass in process what a broker would fail. An `AmqpAddress::raw` address declares no
capability, so a server consults its own configuration and the stand-in, having none, delivers each
message once; say `topic` where the broadcast is the thing being asserted. An at-most-once delivery
arrives settled
and its `ack` reports `AckError::Unsupported`. Batches come from the same client-side buffer, with
the descriptor's own `batch_wait`. A transaction publishes nothing before its commit and discards
its buffer on an abort, and misuse (a second `begin_transaction`, a commit with nothing open) is an
error rather than a silent success. A request carries `reply-to` and `correlation-id`, resolves
with the correlated reply, and fails with `AmqpError::RequestTimeout` when nothing answers.

What is left out is what a broker holds and a process cannot, and each one makes an assertion
unsound rather than merely imprecise:

- **No storage.** A message published to an address with no live subscription is logged and
  dropped, where a server would hold it for a consumer that attaches later. Open the subscriptions
  first.
- **No broker-side redelivery.** A requeued delivery returns to the subscription that had it, never
  to a competing consumer, and there is no dead-letter policy behind `nack(requeue = false)`. The
  attempt is still counted, so a cap ends a message here as it does on a server.
- **No durability.** A committed transaction is atomic as far as a handler can observe, but the
  buffer lives in this process: nothing survives a crash, and there is no broker-side transaction
  timeout or fencing.
- **No flow control.** `credit` has no counterpart. Holding messages back the way a link does would
  make the router a broker-side queue, and no handler would observe the difference, so in-process
  subscriptions are unbounded and a prefetch window cannot be asserted here.
- **No refusal.** A request sent where nothing consumes it times out instead of being rejected or
  dead-lettered.

Those belong to the live suite: `just test-brokers` starts ActiveMQ Artemis from
`docker-compose.test.yml` and runs the integration tests plus every conformance suite against it,
gated behind `AMQP_TEST_URL`.
