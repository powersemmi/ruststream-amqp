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
- **Native request/reply.** `AmqpPublisher` implements the `RequestReply` capability over `reply-to`, `correlation-id`, and a dynamic receiver link.
- **Transactions** (feature `transaction`). A distinct `AmqpTransactionalPublish` policy pairs into a `TransactionalPublisher` built on the protocol's transactional posting; the plain publisher carries no transactional surface.
- **Headers without an envelope.** Well-known headers ride the `properties` section (`content-type`, `correlation-id`, `reply-to`, `message-id`, the partition key as `group-id`); everything else rides `application-properties`, so non-Rust peers see plain AMQP messages.
- **In-process test broker** (feature `testing`). `AmqpTestBroker` reproduces core routing with no server, implements `ruststream::testing::TestableBroker`, and passes the framework's conformance suite in process.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-amqp = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-amqp = { version = "0.7", features = ["testing"] }
```

## Write a service

```rust
use ruststream_amqp::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(AmqpAddress::queue("orders"))]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(AmqpBroker::new("amqp://localhost:5672"), |b| b.include(handle))
}
```

The descriptor carries the AMQP-specific options inline in the decorator:

```rust
#[subscriber(AmqpAddress::queue("orders").credit(64))]
async fn handle(order: &Order) -> HandlerResult { /* ... */ }
```

## Test it

The `testing` feature runs handlers against an in-process AMQP stand-in - no server, same routing, same ladder. Inject a message as an external producer would with `TestableBroker::inject`, then assert on what a handler published with the free `expect_published`:

```rust
use ruststream::{Broker, OutgoingMessage};
use ruststream::testing::{TestableBroker, expect_published};
use ruststream_amqp::testing::AmqpTestBroker;

let broker = AmqpTestBroker::new().connect().await?;
broker.inject(OutgoingMessage::new("orders", br#"{"id":1}"#));
let confirmations =
    expect_published(&broker, "confirmations", 1, std::time::Duration::from_secs(1)).await;
```

Broker-specific behaviour (dispositions, credit, dead-lettering) is covered by the env-gated live suite instead: `just test-brokers` spins up ActiveMQ Artemis and runs the integration tests plus the framework conformance suites against it.

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
just test           # handler-stub tests, no server
just test-brokers   # live integration + conformance against ActiveMQ Artemis
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
