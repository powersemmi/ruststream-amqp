`AMQP` 1.0 broker for the [RustStream](https://github.com/powersemmi/ruststream) messaging
framework, backed by [`fe2o3-amqp`](https://docs.rs/fe2o3-amqp).

`AMQP` 1.0 is an ISO standard, so one crate serves the whole family: `ActiveMQ` Artemis and
Classic, `RabbitMQ` 4.x, Azure Service Bus and Event Hubs, Amazon MQ, Solace, Apache Qpid and IBM
MQ. A subscription names a queue, a topic or a verbatim address, and a handler's outcome becomes
the delivery's disposition. On `RabbitMQ` the 1.0 stack is a protocol of its own, separate from the
0.9.1 that [`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin) speaks.

# A service

A handler is an `async fn` over the decoded payload, mounted on a broker inside the application
object, and the attribute writes the `main`:

```
# mod demo {
use ruststream_amqp::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[subscriber(AmqpAddress::queue("orders"))]
async fn handle(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "accepted");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        AmqpBroker::new("amqp://artemis:artemis@localhost:5672"),
        |b| {
            b.include(handle);
        },
    )
}
# }
# fn main() {}
```

`cargo run -- run` starts it. [`AmqpBroker::new`] only records the URL; the runtime connects the
broker at startup and closes the connection when the service stops.

# Where things are

The [guides][guides] take an `AMQP` 1.0 service from installation to production, in this order:
[getting started][g-start], [queues and topics][g-queues], [acknowledgement and delivery
guarantees][g-ack], [retries and dead letters][g-retries], [request-reply][g-request-reply],
[transactions][g-transactions], [testing][g-testing] and [connecting to production][g-production].
The [architecture][architecture] pages show how `AMQP` 1.0 works and where the crate sits between
the broker and the runtime, and the [benchmarks][benchmarks] what it costs.

Each topic below links to its component page on the site, which shows it in a service; the
item's own documentation keeps the detail.

## Broker

- [AMQP broker][c-broker]: [`AmqpBroker`], connected into [`ConnectedAmqpBroker`].
- [`container_id`][c-container-id]: [`AmqpBroker::container_id`], the name an operator finds the
  service's links by.
- [Security][c-security]: [`Sasl`] through [`AmqpBroker::sasl`], and TLS for `amqps://` with the
  `rustls` or `native-tls` feature: `Tls` through `AmqpBroker::tls` names the trusted authorities
  and the client certificate SASL EXTERNAL logs in with.
- [Startup][c-startup]: the connect that opens the socket, authenticates and begins the
  publishers' session.
- [Shutdown][c-shutdown]: the subscriptions, the publisher links and the connection closed in turn,
  and [`AmqpError::NotConnected`] for a publisher used afterwards.
- [Prelude][c-prelude]: [`prelude`], the one glob an `AMQP` service imports.
- [Errors][c-errors]: [`AmqpError`].

## Subscriptions

- [Subscriptions][c-subscriptions]: [`AmqpAddress`], the address descriptor, and a bare name in
  `#[subscriber]`.
- [queue][c-queue]: [`AmqpAddress::queue`], competing consumers, one delivery each.
- [topic][c-topic]: [`AmqpAddress::topic`], a copy for every subscription.
- [raw][c-raw]: [`AmqpAddress::raw`], an address sent as written.
- [credit][c-credit]: [`AmqpAddress::credit`], the window of deliveries the broker may send ahead.
- [Batches][c-batches]: a slice handler over batches assembled on the client, closed by
  [`AmqpAddress::batch_wait`].

## Dispositions

- [Dispositions][c-settlement]: what `ack`, `retry` and `drop` send for an [`AmqpMessage`].
- [Settle][c-settle]: [`Settle`], at least once or at most once.
- [Retries][c-retries]: `max_attempts`, counted by the broker's `delivery-count`.
- [Dead letters][c-dead-letters]: `dead_letter`, the address a spent delivery goes to.
- [`retry_after`][c-retry-after]: a copy published to the subscription's own address after the
  delay.
- [`out_retry`][c-out-retry]: the publisher of those copies, and the steps that apply to them.

## Publishing

- [Publish][c-publishing]: [`AmqpPublish`], the publish policy, and its live [`AmqpPublisher`].
- [Headers][c-headers]: the well-known headers in the `properties` section, the rest in
  `application-properties`.
- [Keys][c-keys]: [`PARTITION_KEY_HEADER`] rides the `group-id` property.
- [Request-reply][c-request-reply]: `request` over a dynamic reply address, and
  [responders][c-responders] that answer on it.
- [Transactions][c-transactions]: `AmqpTransactionalPublish` and its live `AmqpTxnPublisher`,
  and an `AmqpTransaction` of its own per delivery under `workers(n)` (feature `transaction`).

## Tooling

- [AMQP bindings][c-asyncapi]: what the generated `AsyncAPI` document says about the
  [server][c-asyncapi-server], the [channels][c-asyncapi-channels], the
  [operations][c-asyncapi-operations] and the [replies][c-asyncapi-replies] (feature `asyncapi`).
- [Testing][c-testing]: the in-process transport
  [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html) connects
  the broker to (feature `testing`), and [Live][c-live] runs against a running broker.

# Cargo features

`default` is empty. `rustls` and `native-tls` select the TLS backend for `amqps://` and add `Tls`,
the connection's TLS settings.
`transaction` adds the transactional publisher. `asyncapi` contributes the `amqp1` server and the
crate's extension to the generated document, and `testing` adds the in-process mode
`TestApp::start` connects the broker through.

[guides]: https://powersemmi.github.io/ruststream-amqp/latest/
[g-start]: https://powersemmi.github.io/ruststream-amqp/latest/guides/getting-started/
[g-queues]: https://powersemmi.github.io/ruststream-amqp/latest/guides/queues-and-topics/
[g-ack]: https://powersemmi.github.io/ruststream-amqp/latest/guides/acknowledgement/
[g-retries]: https://powersemmi.github.io/ruststream-amqp/latest/guides/retries/
[g-request-reply]: https://powersemmi.github.io/ruststream-amqp/latest/guides/request-reply/
[g-transactions]: https://powersemmi.github.io/ruststream-amqp/latest/guides/transactions/
[g-testing]: https://powersemmi.github.io/ruststream-amqp/latest/guides/testing/
[g-production]: https://powersemmi.github.io/ruststream-amqp/latest/guides/production/
[architecture]: https://powersemmi.github.io/ruststream-amqp/latest/architecture/
[benchmarks]: https://powersemmi.github.io/ruststream-amqp/latest/architecture/benchmarks/
[c-broker]: https://powersemmi.github.io/ruststream-amqp/latest/components/broker/
[c-container-id]: https://powersemmi.github.io/ruststream-amqp/latest/components/broker/container-id/
[c-security]: https://powersemmi.github.io/ruststream-amqp/latest/components/broker/security/
[c-startup]: https://powersemmi.github.io/ruststream-amqp/latest/components/broker/startup/
[c-shutdown]: https://powersemmi.github.io/ruststream-amqp/latest/components/broker/shutdown/
[c-prelude]: https://powersemmi.github.io/ruststream-amqp/latest/components/broker/prelude/
[c-errors]: https://powersemmi.github.io/ruststream-amqp/latest/components/broker/errors/
[c-subscriptions]: https://powersemmi.github.io/ruststream-amqp/latest/components/subscriptions/
[c-queue]: https://powersemmi.github.io/ruststream-amqp/latest/components/subscriptions/queue/
[c-topic]: https://powersemmi.github.io/ruststream-amqp/latest/components/subscriptions/topic/
[c-raw]: https://powersemmi.github.io/ruststream-amqp/latest/components/subscriptions/raw/
[c-credit]: https://powersemmi.github.io/ruststream-amqp/latest/components/subscriptions/credit/
[c-batches]: https://powersemmi.github.io/ruststream-amqp/latest/components/subscriptions/batches/
[c-settlement]: https://powersemmi.github.io/ruststream-amqp/latest/components/settlement/
[c-settle]: https://powersemmi.github.io/ruststream-amqp/latest/components/settlement/settle/
[c-retries]: https://powersemmi.github.io/ruststream-amqp/latest/components/settlement/retries/
[c-dead-letters]: https://powersemmi.github.io/ruststream-amqp/latest/components/settlement/dead-letters/
[c-retry-after]: https://powersemmi.github.io/ruststream-amqp/latest/components/settlement/retry-after/
[c-out-retry]: https://powersemmi.github.io/ruststream-amqp/latest/components/settlement/out-retry/
[c-publishing]: https://powersemmi.github.io/ruststream-amqp/latest/components/publishing/
[c-headers]: https://powersemmi.github.io/ruststream-amqp/latest/components/publishing/headers/
[c-keys]: https://powersemmi.github.io/ruststream-amqp/latest/components/publishing/keys/
[c-request-reply]: https://powersemmi.github.io/ruststream-amqp/latest/components/request-reply/
[c-responders]: https://powersemmi.github.io/ruststream-amqp/latest/components/request-reply/responders/
[c-transactions]: https://powersemmi.github.io/ruststream-amqp/latest/components/transactions/
[c-asyncapi]: https://powersemmi.github.io/ruststream-amqp/latest/components/asyncapi/
[c-asyncapi-server]: https://powersemmi.github.io/ruststream-amqp/latest/components/asyncapi/server/
[c-asyncapi-channels]: https://powersemmi.github.io/ruststream-amqp/latest/components/asyncapi/channels/
[c-asyncapi-operations]: https://powersemmi.github.io/ruststream-amqp/latest/components/asyncapi/operations/
[c-asyncapi-replies]: https://powersemmi.github.io/ruststream-amqp/latest/components/asyncapi/replies/
[c-testing]: https://powersemmi.github.io/ruststream-amqp/latest/components/testing/
[c-live]: https://powersemmi.github.io/ruststream-amqp/latest/components/testing/live/
