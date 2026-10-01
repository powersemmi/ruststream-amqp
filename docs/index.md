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

## What the crate offers

Addressing is explicit, because AMQP 1.0 standardises the wire and leaves the meaning of an address
to the deployment: a subscription names a queue (anycast), a topic (multicast), or a verbatim
address, and the terminus capability tells the broker which.
[Subscribing](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#subscribing) has the
descriptor and its settings, credit as the protocol's own back-pressure, at-most-once delivery, the
disposition each handler outcome maps to, the retry cap and the deferred re-publish behind
`retry_after`, and batches assembled on the client.
[Publishing](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#publishing) has the
publish policies, request/reply over a dynamic reply link, and transactional posting. Then
[the generated document](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#the-generated-document),
[testing](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#testing) of the
production app in process or against a live broker, and
[operations](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#operations):
authentication, TLS, sessions, and the known gaps.

## Where to go next

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[AMQP reference](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html)** - addressing, batches, dispositions, retries, request/reply, transactions, the generated document, and testing.
- :material-book-open-variant: **[RustStream docs](https://powersemmi.github.io/ruststream/)** - the framework itself: subscribers, publishing, routing, codecs, middleware, observability, and the CLI.
- :material-language-rust: **[API reference](https://docs.rs/ruststream-amqp)** - every type and method the crate exports.

</div>

## How this site relates to the RustStream docs

This page is the entry point: what the crate is, how to install it, and the first service. Every
topic of the crate is documented beside the code it describes, on
[docs.rs](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html). What the framework
does on every broker is in the
[RustStream documentation](https://powersemmi.github.io/ruststream/).
