<h1 align="center">ruststream-amqp</h1>

<p align="center">
  <i>The AMQP 1.0 broker for the <a href="https://github.com/powersemmi/ruststream">RustStream</a> messaging framework: one protocol crate for ActiveMQ Artemis, Azure Service Bus, RabbitMQ 4.x, and the rest of the AMQP 1.0 family.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream-amqp/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream-amqp/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/MSRV-1.85-blue.svg" alt="MSRV 1.85">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
</p>

---

`ruststream-amqp` will implement the [RustStream](https://github.com/powersemmi/ruststream) broker contract over [`fe2o3-amqp`](https://crates.io/crates/fe2o3-amqp) (AMQP 1.0). Handlers, routers, codecs, and middleware come from the framework; this crate supplies the transport - and nothing broker-specific leaks back into the framework.

## Status

**Not implemented yet.** This repository is a scaffold: the workspace, CI, and release plumbing are in place, and the crate is an empty stub. The implementation will target the `ruststream` 0.6 line; the design and scope are tracked in [powersemmi/ruststream#187](https://github.com/powersemmi/ruststream/issues/187).

## Planned surface

- Acknowledgement as protocol dispositions: accept for ack, release for requeue, reject for drop, and modify targeting the broker's own dead-letter address.
- `RequestReply` over reply-to, correlation-id, and a dynamic receiver link.
- `TransactionalPublisher` over the protocol's transactional state.
- `Partitioned` over the message group id; headers over application-properties, no invented envelope.
- Per-product addressing helpers (Artemis, Azure Service Bus, RabbitMQ 4.x) behind cargo features, over one shared protocol core.

The broker contract (lazy startup, the typed connect/shutdown lifecycle, and the optional capability traits) is defined by [`ruststream`](https://crates.io/crates/ruststream) and verified by `ruststream::conformance`, with the suite run against a real broker before release.

## Contributing

```bash
just check   # fmt, clippy, feature checks
just test    # tests
just ci      # the full local gate
```

## License

Licensed under the [Apache-2.0](./LICENSE) license.
