---
sidebar_position: 1
description: "Install ruststream-amqp, start a local ActiveMQ Artemis, run a first subscriber against it and test it without a broker."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Getting started

This guide installs `ruststream-amqp`, starts a local AMQP 1.0 broker and runs a first service against it.

## Installation

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-amqp = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-amqp = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` is the framework: `macros` gives the attributes, `json` the default codec, and `logging` prints the service's log to the terminal.
`ruststream-amqp` is the AMQP 1.0 broker.
Its `testing` feature adds the in-process broker that tests run on.

The broker crate has these features, all off by default:

| Feature | What it gives |
|---|---|
| `rustls` | `amqps://` connections over TLS with rustls |
| `native-tls` | `amqps://` connections over the platform's TLS |
| `transaction` | [transactional publishing](transactions.md) |
| `asyncapi` | the AMQP 1.0 details in the generated AsyncAPI document |
| `testing` | the in-process broker behind `TestApp` |

The client underneath is [fe2o3-amqp](https://docs.rs/fe2o3-amqp), written in Rust, so the build needs no system libraries.

## A local broker

One ActiveMQ Artemis is enough to develop against:

```yaml title="compose.yaml"
services:
  artemis:
    image: apache/activemq-artemis:latest-alpine
    environment:
      ARTEMIS_USER: artemis
      ARTEMIS_PASSWORD: artemis
    ports:
      - "5672:5672"
```

```bash
docker compose up -d
```

The broker listens for AMQP on `localhost:5672`, with the user `artemis` and the password `artemis`.
Artemis creates a queue the first time a service reads it, so there is nothing to declare.

## The first service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` reads JSON orders from the queue `orders`.
It accepts an order with an amount and rejects one without.
[`AmqpAddress::queue`](api:ruststream_amqp::AmqpAddress::queue) names the queue, and [Queues and topics](queues-and-topics.md) shows the other kinds of address.
[`AmqpBroker::new`](api:ruststream_amqp::AmqpBroker::new) takes the broker's URL and only records it.
The connection opens when the service starts.
The user and password in the URL log the service in.
`#[ruststream::app]` turns `app` into `main`; in the manual form, [`run_main`](api:ruststream::runtime::cli::run_main) does the same.

Run the service:

```bash
cargo run -- run
```

Publish an order from another terminal with the command-line client that ships in the image:

```bash
docker compose exec artemis /opt/activemq-artemis/bin/artemis producer \
  --protocol amqp --user artemis --password artemis \
  --destination queue://orders --message-count 1 --message '{"id":1,"amount_cents":1250}'
```

The service logs `order taken` with the order's id.
<kbd>Ctrl</kbd>+<kbd>C</kbd> stops it.

## The first test

The example file also holds two tests.
Each starts the same `app` with `TestApp::start`, publishes an order and checks how `take_order` settled it.
`AmqpBroker` connects to an in-process broker there, so the tests need no Artemis:

```bash
cargo test
```

[Testing an AMQP service](testing.md) walks through such tests and what the in-process broker models.

## Coming from fe2o3-amqp

A service on fe2o3-amqp opens its connection and sessions, attaches its links and writes a receive loop.
Here is where each part goes in RustStream:

| fe2o3-amqp | RustStream |
|---|---|
| `Connection::open` | [`AmqpBroker::new`](api:ruststream_amqp::AmqpBroker::new), with [`sasl`](api:ruststream_amqp::AmqpBroker::sasl) and [`container_id`](api:ruststream_amqp::AmqpBroker::container_id) |
| `Session::begin` | nothing: each subscription gets a session of its own, and the publishers share one |
| `Receiver::attach` and a `recv` loop | a `#[subscriber(AmqpAddress::..)]` handler |
| `CreditMode::Auto(n)` | [`AmqpAddress::credit`](api:ruststream_amqp::AmqpAddress::credit) |
| `accept`, `modify`, `reject` | the handler's outcome: `ack`, `retry`, `drop` ([Acknowledgement and delivery guarantees](acknowledgement.md)) |
| `auto_accept(true)` | [`Settle::AtMostOnce`](api:ruststream_amqp::Settle::AtMostOnce) |
| `Sender::attach` and `send` | a reply or an `Out` parameter, sent through the [`Publish`](api:ruststream_amqp::AmqpPublish) policy |
| a dynamic receiver for replies | [`request`](api:ruststream::RequestReply::request) ([Request-reply](request-reply.md)) |
| `Controller` and `Transaction::declare` | `TransactionalPublish` ([Transactions](transactions.md)) |

A service that speaks AMQP 0.9.1 to RabbitMQ uses [ruststream-lapin](https://powersemmi.github.io/ruststream-lapin/) instead: the two protocols share the port and the `amqp://` scheme, and they are different protocols.
