# ruststream-amqp

**`ruststream-amqp`** runs a [RustStream](https://powersemmi.github.io/ruststream/) service on
AMQP 1.0. The protocol is an ISO standard, so one crate serves the whole family: ActiveMQ Artemis
and Classic, RabbitMQ 4.x, Azure Service Bus and Event Hubs, Amazon MQ, Solace, Apache Qpid, and
IBM MQ.

On RabbitMQ, AMQP 1.0 is a separate protocol stack from the 0.9.1 that
[`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin) speaks.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-amqp = "0.7"
serde = { version = "1", features = ["derive"] }
```

`ruststream_amqp::prelude::*` is the one import a service file writes. It re-exports the broker and
its authentication profile, the address descriptor and its delivery guarantee, the publish
policies, the crate's error, and the framework's own prelude.

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_service.rs:app"
```

## Where to go next

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[AMQP guide](amqp.md)** - addressing, batches, dispositions, request/reply, transactions, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, publishing, routing, codecs, middleware, observability, and the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-amqp)** - every type and method the crate exports.

</div>

## How this site relates to the RustStream docs

This site covers AMQP 1.0 and this crate. What the framework does on every broker is in the
[RustStream documentation](https://powersemmi.github.io/ruststream/). The pages here link to it
where the two meet.
