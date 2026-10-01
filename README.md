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

`ruststream-amqp` connects a RustStream service to an AMQP 1.0 broker over
[`fe2o3-amqp`](https://crates.io/crates/fe2o3-amqp). One crate serves the whole family: ActiveMQ
Artemis and Classic, RabbitMQ 4.x over AMQP 1.0, Azure Service Bus and Event Hubs, Amazon MQ,
Solace, Apache Qpid and IBM MQ. For RabbitMQ over AMQP 0.9.1, use
[`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin). Handlers, routing, codecs and
middleware come from the framework; this crate is the transport.

## Features

- **Settlement as dispositions:** `ack` is `accepted`, a retry is `modified`, a drop is `rejected`,
  so the broker's own dead-letter policy applies.
- **Explicit addressing:** queues (anycast), topics (multicast) or a raw address, with credit as
  flow control.
- **Retry caps and dead letters** declared where the handler is mounted.
- **Batches** assembled on the client.
- **Request/reply** over `reply-to` and `correlation-id`, and **transactions** behind the
  `transaction` feature.
- **Plain AMQP messages** for non-Rust peers: well-known headers ride the `properties` section.
- **AsyncAPI** that names the protocol `amqp1`, behind the `asyncapi` feature.
- **Tests without a server:** handlers run against an in-process AMQP broker.

## Install

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-amqp = "0.7"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream-amqp = { version = "0.7", features = ["testing"] }
```

Optional features: TLS for `amqps://` (`rustls` or `native-tls`), `transaction` and `asyncapi`.

## Write a service

```rust
use ruststream_amqp::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
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

`#[ruststream::app]` generates `main`, so the binary understands `run` and `asyncapi gen`.

## Test it

`TestApp` runs the service's own app with `AmqpBroker` in process, with no server.

```rust
use ruststream::testing::TestApp;

let tb = TestApp::start(app()).await?;

tb.broker::<AmqpBroker>()
    .message(&Order { id: 42 })
    .to("orders")
    .publish()
    .await?;

tb.broker::<AmqpBroker>()
    .subscriber("orders")
    .assert_called_once()
    .with(&Order { id: 42 })
    .settled(HandlerOutcome::ack());

tb.broker::<AmqpBroker>()
    .published::<Confirmation>("confirmations")
    .assert_called_once()
    .with(&Confirmation { order_id: 42 });
```

## Documentation

- This crate: <https://docs.rs/ruststream-amqp>
- The framework: <https://powersemmi.github.io/ruststream/latest>

## Minimum supported Rust version

The MSRV is **1.88**, edition 2024.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

Licensed under the [Apache-2.0](./LICENSE) license.
