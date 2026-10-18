---
title: AMQP broker
description: "AmqpBroker connects a service to an AMQP 1.0 broker; new only records the URL, and the runtime connects it once at startup."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# AMQP broker

[`AmqpBroker`](api:ruststream_amqp::AmqpBroker) connects a service to an AMQP 1.0 broker; `new` only records the URL, and the runtime connects it once at startup.

AMQP 1.0 is an ISO standard, so one broker type serves ActiveMQ Artemis and Classic, RabbitMQ 4.x, Azure Service Bus, Amazon MQ, Solace, Apache Qpid and IBM MQ.
[`AmqpBroker::new`](api:ruststream_amqp::AmqpBroker::new) takes the broker's URL: `amqp://host:port`, or `amqps://host:port` for TLS.
A URL without a port uses 5672 for `amqp` and 5671 for `amqps`.
The constructor does no I/O, so the whole service fits the synchronous `#[ruststream::app]` builder.
The settings that follow it are builder steps too: the [authentication](./security.md) and the [container id](./container-id.md).

At startup the runtime connects the broker and receives a [`ConnectedAmqpBroker`](api:ruststream_amqp::ConnectedAmqpBroker).
The [connect](./startup.md) opens the socket, authenticates and opens the AMQP connection, so a broker that is unreachable or refuses the service fails startup instead of the first publish.
Every subscription and every publisher of the service runs on that one connection.
At [shutdown](./shutdown.md) the runtime closes the connection, and the connected form is consumed with it.

`#[subscriber("orders")]` opens the address `orders` as it is written, without a terminus capability.
The broker's own configuration then decides whether the subscriptions on the address compete or each get a copy.
[`AmqpAddress::queue`](../subscriptions/queue.md) and [`AmqpAddress::topic`](../subscriptions/topic.md) state that choice in the service; the [Subscriptions](../subscriptions/index.md) pages describe them.

[Getting started](../../guides/getting-started.md) runs a first service against a local ActiveMQ Artemis.

## Example

The service reads the queue address `orders`.
`cargo run --example amqp_service --features testing -- run` starts it against a broker on `localhost:5672` with the user `artemis`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_service.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_service.rs#app"
```

</TabItem>
</Tabs>
