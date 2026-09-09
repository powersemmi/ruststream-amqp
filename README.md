<h1 align="center">ruststream-amqp</h1>

<p align="center">
  <i>The AMQP 1.0 broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: one protocol crate for ActiveMQ Artemis, RabbitMQ 4.x, Azure Service Bus, and the rest of the AMQP 1.0 family.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-amqp/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-amqp/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/ruststream-amqp"><img src="https://img.shields.io/crates/v/ruststream-amqp.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream-amqp"><img src="https://img.shields.io/crates/dr/ruststream-amqp" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream-amqp"><img src="https://img.shields.io/docsrs/ruststream-amqp" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.88-blue.svg" alt="MSRV 1.88">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream-amqp/">Documentation</a></b>
</p>

---

`ruststream-amqp` implements the RustStream broker contract over [`fe2o3-amqp`](https://crates.io/crates/fe2o3-amqp). Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

AMQP 1.0 is an ISO-standard protocol spoken by ActiveMQ Artemis and Classic, RabbitMQ 4.x (a separate protocol stack from the 0.9.1 that [`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin) speaks), Azure Service Bus and Event Hubs, Amazon MQ, Solace, Apache Qpid, and IBM MQ - one crate serves the whole family.

## Features

- **Lazy startup contract.** `AmqpBroker::new(url)` is synchronous and does no I/O; the runtime connects once at startup, so the broker composes with `#[ruststream::app]`. SASL (ANONYMOUS, PLAIN, EXTERNAL) and the container id are builder options.
- **Acknowledgement as dispositions.** `ack` maps to `accept`, `nack(requeue = true)` to `release`, `nack(requeue = false)` to `reject` - the broker's own dead-letter policy applies. At-most-once subscriptions report `AckError::Unsupported` instead of a settlement that never reaches the wire.
- **Explicit addressing.** The protocol standardises the wire, not the meaning of an address: `AmqpAddress::queue` (anycast), `AmqpAddress::topic` (multicast), `AmqpAddress::raw` (verbatim, for deployments with their own convention), plus `credit` (prefetch as protocol-level flow control) and the `settle` guarantee.
- **Batches.** A handler taking a slice gets batches of the size its mount site names (`.batch(nonzero!(32))`). A transfer carries one message, so the batches are assembled on the client and `batch_wait` caps how long a partial one waits - the mount site reads the same as on a broker that batches on the wire.
- **Publishers pair at startup.** `AmqpPublish` is declaration only, so it is written anywhere; the runtime pairs it against the connected broker, and a handler slot never holds an unconnected publisher. The crate prelude aliases it to `Publish` (and `AmqpTransactionalPublish` to `TransactionalPublish`), so a mount site reads `.out(Reply, Publish)` here exactly as on every other broker in the family.
- **Native request/reply.** `AmqpPublisher` implements the `RequestReply` capability over `reply-to`, `correlation-id`, and a dynamic receiver link.
- **Transactions** (feature `transaction`). A distinct `AmqpTransactionalPublish` policy pairs into a `TransactionalPublisher` built on the protocol's transactional posting; the plain publisher carries no transactional surface.
- **Headers without an envelope.** Well-known headers ride the `properties` section (`content-type`, `correlation-id`, `reply-to`, `message-id`, the partition key as `group-id`); everything else rides `application-properties`, so non-Rust peers see plain AMQP messages.
- **In-process test broker** (feature `testing`). `AmqpTestBroker` reproduces this crate's routing with no server, a service mounts on it and runs under the framework's `TestApp` harness, and it answers the way a real broker does, which the crate's own tests hold it to.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-amqp = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-amqp = { version = "0.7", features = ["testing"] }
```

Everything else is off by default: `amqps://` endpoints need `rustls` or `native-tls`, and transactional publishing needs `transaction`.

## Write a service

```rust
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    order_id: u64,
}

#[subscriber(AmqpAddress::queue("orders"))]
async fn handle(order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    let confirmation = Confirmation { order_id: order.id };
    if out.message(&confirmation).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://localhost:5672"),
        |b| {
            b.include(handle).out(DefaultSlot, Publish).build();
        },
    )
}
```

The handler names a capability, never a broker: `Out<impl Publisher>` is filled by whatever policy the mount site binds to the slot's marker, `DefaultSlot` being the unnamed one and `Reply` the reply slot. `Publish` is the prelude's name for `AmqpPublish`, so an include site reads the same on every broker in the family - what changes when a service moves is the prelude it globs and the subscription descriptor, not every `.out(..)`.

The descriptor carries the AMQP-specific options inline in the decorator - `credit` is the protocol's own flow control, so a lower value bounds work in flight with no extra layer:

```rust
#[subscriber(AmqpAddress::queue("audit").credit(64))]
async fn audit(order: &Order) -> HandlerOutcome {
    println!("auditing order {}", order.id);
    HandlerOutcome::ack()
}
```

## Test it

The `testing` feature ships an in-process AMQP stand-in: no server, the same routing, the same ladder. It plugs into the framework's `TestApp` harness, which drives the built application through the dispatch path the production runtime uses, so the assertions read the handler's own behaviour rather than the transport's:

```rust
use ruststream::testing::TestApp;
use ruststream_amqp::prelude::*;
use ruststream_amqp::testing::{AmqpTestBroker, AmqpTestPublish};

// The handler is the production one; only the mount changes. The stand-in routes by name, so
// the address descriptor is mapped down to the name it carries.
let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
    AmqpTestBroker::new(),
    |b| {
        b.include(handle.map_source(|address| Name::new(address.address().to_owned())))
            .out(DefaultSlot, AmqpTestPublish)
            .build();
    },
);
let tb = TestApp::start(app).await?;

// The publish drives the handler to a standstill before it returns.
tb.publish("orders", &Order { id: 42 }).await?;

tb.broker::<AmqpTestBroker>()
    .subscriber("orders")
    .assert_called_once()
    .with(&Order { id: 42 })
    .settled(HandlerOutcome::ack());

tb.broker::<AmqpTestBroker>()
    .published::<Confirmation>("confirmations")
    .assert_called_once()
    .with(&Confirmation { order_id: 42 });
```

The stand-in matches addresses exactly and models none of the broker-specific behaviour (dispositions, credit, dead-lettering). That is covered by the env-gated live suite instead: `just test-brokers` spins up ActiveMQ Artemis and runs the integration tests plus the framework conformance suites against it.

## Layout

```
ruststream-amqp/
├── crates/
│   └── ruststream-amqp/        the published crate
│       └── examples/           runnable amqp_* examples (docs-site snippet sources)
├── docs/                       the documentation site (properdocs + Material)
├── docker-compose.test.yml     ActiveMQ Artemis for the live suite
├── properdocs.yml              docs site config
└── Cargo.toml                  workspace
```

The AMQP guide, including the request/reply, transaction, and capability coverage, lives at [powersemmi.github.io/ruststream-amqp](https://powersemmi.github.io/ruststream-amqp/). Framework concepts (subscribers, routing, codecs, middleware, the CLI) live in the [RustStream docs](https://powersemmi.github.io/ruststream/).

## Contributing

```bash
just check          # fmt, clippy, feature checks
just test           # in-process tests; the live suites skip without AMQP_TEST_URL
just test-brokers   # live integration + conformance against ActiveMQ Artemis
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
