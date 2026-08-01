<h1 align="center">ruststream-amqp</h1>

<p align="center">
  <i>The AMQP 1.0 broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: one protocol crate for ActiveMQ Artemis, RabbitMQ 4.x, Azure Service Bus, and the rest of the AMQP 1.0 family.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-amqp/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-amqp/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/MSRV-1.85-blue.svg" alt="MSRV 1.85">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

---

`ruststream-amqp` implements the RustStream broker contract over [`fe2o3-amqp`](https://crates.io/crates/fe2o3-amqp). Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

AMQP 1.0 is an ISO-standard protocol spoken by ActiveMQ Artemis and Classic, RabbitMQ 4.x (a separate protocol stack from the 0.9.1 that [`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin) speaks), Azure Service Bus and Event Hubs, Amazon MQ, Solace, Apache Qpid, and IBM MQ - one crate serves the whole family.

## Features

- **Lazy startup contract.** `AmqpBroker::new(url)` is synchronous and does no I/O; the runtime connects once at startup, so the broker composes with `#[ruststream::app]`. SASL (ANONYMOUS, PLAIN, EXTERNAL) and the container id are builder options.
- **Acknowledgement as dispositions.** `ack` maps to `accept`, `nack(requeue = true)` to `release`, `nack(requeue = false)` to `reject` - the broker's own dead-letter policy applies. At-most-once subscriptions report `AckError::Unsupported` instead of pretending.
- **Explicit addressing.** The protocol standardises the wire, not the meaning of an address: `AmqpAddress::queue` (anycast), `AmqpAddress::topic` (multicast), `AmqpAddress::raw` (verbatim, for deployments with their own convention), plus `credit` (prefetch as protocol-level flow control) and the `settle` guarantee.
- **Native request/reply.** `AmqpPublisher` implements the `RequestReply` capability over `reply-to`, `correlation-id`, and a dynamic receiver link.
- **Transactions** (feature `transaction`). A distinct `AmqpTransactionalPublish` policy pairs into a `TransactionalPublisher` built on the protocol's transactional posting; the plain publisher carries no transactional surface.
- **Headers without an envelope.** Well-known headers ride the `properties` section (`content-type`, `correlation-id`, `reply-to`, `message-id`, the partition key as `group-id`); everything else rides `application-properties`, so non-Rust peers see plain AMQP messages.
- **In-process test broker** (feature `testing`). `AmqpTestBroker` reproduces core routing with no server, implements `ruststream::testing::TestableBroker`, and passes the framework's conformance suite in process.

## Status

Implemented and verified against ActiveMQ Artemis (the framework's conformance lifecycle, request/reply, and transactions suites run in CI against a live broker). Built on `ruststream` 0.6 from crates.io; the crate itself is not published yet. Design and scope are tracked in [powersemmi/ruststream#187](https://github.com/powersemmi/ruststream/issues/187).

## Write a service

```rust
use ruststream::runtime::{App, AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_amqp::{AmqpAddress, AmqpBroker};
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

## Request/reply

```rust
use std::time::Duration;
use ruststream::{IncomingMessage, OutgoingMessage, RequestReply};

let reply = publisher
    .request(OutgoingMessage::new("greeter", b"hello".as_slice()), Duration::from_secs(5))
    .await?;
println!("{}", String::from_utf8_lossy(reply.payload()));
```

## Test it

The `testing` feature runs handlers against an in-process AMQP stand-in - no server, same routing. Broker-specific behaviour (dispositions, credit, dead-lettering) is covered by the env-gated live suite instead: `just test-brokers` spins up ActiveMQ Artemis and runs the integration tests plus the framework conformance suites against it.

## Layout

```
ruststream-amqp/
├── crates/
│   └── ruststream-amqp/        the published crate
│       └── examples/           runnable amqp_* examples
├── docker-compose.test.yml     ActiveMQ Artemis for the live suite
└── Cargo.toml                  workspace
```

## Contributing

```bash
just check          # fmt, clippy, feature checks
just test           # handler-stub tests, no server
just test-brokers   # live integration + conformance against ActiveMQ Artemis
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
