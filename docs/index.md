# ruststream-amqp

**`ruststream-amqp`** is the AMQP 1.0 broker for the
[RustStream](https://powersemmi.github.io/ruststream/) messaging framework. AMQP 1.0 is an
ISO-standard protocol, so one crate serves the whole family: ActiveMQ Artemis and Classic,
RabbitMQ 4.x (a separate protocol stack from the 0.9.1 that
[`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin) speaks), Azure Service Bus and
Event Hubs, Amazon MQ, Solace, Apache Qpid, and IBM MQ.

Handlers, routers, codecs, and middleware come from the framework; this crate supplies the
transport, and nothing broker-specific leaks back into the framework.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-amqp = "0.7"
serde = { version = "1", features = ["derive"] }
```

`ruststream_amqp::prelude::*` is the one import a service file writes: it carries the broker, the
address descriptor, and the publish policies, and re-exports the framework's own prelude.

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_service.rs:app"
```

## Where to go next

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[AMQP guide](amqp.md)** - addressing, dispositions, request/reply, transactions, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, routing, codecs, middleware, the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-amqp)** - the crate's rustdoc on docs.rs.

</div>

## How this site relates to the RustStream docs

This site documents the AMQP 1.0 broker only. Framework concepts that apply to every broker
(writing subscribers, publishing, routing, codecs, middleware, observability, the CLI) live in the
[RustStream documentation](https://powersemmi.github.io/ruststream/). The pages here cover what is
specific to AMQP and link back to the framework docs where the two meet.
