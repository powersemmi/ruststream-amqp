---
sidebar_position: 2
description: "Read a work queue, broadcast on a topic, keep a broadcast for a reader that is away, and publish to an address over AMQP 1.0."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Queues and topics

This guide reads a work queue, broadcasts on a topic, keeps a broadcast for a reader that is away, and publishes to an address.

AMQP 1.0 fixes the wire and leaves the meaning of an address to the broker.
A subscription therefore says what it wants: [`AmqpAddress`](../components/subscriptions/index.md) has a constructor for each intent, and the link tells the broker which one it is.
What this page states is proved against ActiveMQ Artemis, the broker the crate's tests run on.

The example is an orders service.
It takes orders from a queue and announces each taken order on a topic, which billing, an audit log and a ledger follow.

## A work queue

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/queues_and_topics.rs#queue"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/queues_and_topics.rs#queue"
```

</TabItem>
</Tabs>

[`AmqpAddress::queue`](../components/subscriptions/queue.md) asks for a queue: the link carries the terminus capability `queue`.
Artemis then creates an anycast address `orders` with one queue behind it.
Consumers on a queue compete, and each order goes to one of them.
To take orders faster, run more replicas of the service: each replica is one more consumer of `orders`.

`take_order` answers each order with an `OrderTaken`.
`OrderTaken` names its address with `#[outgoing(name = "order-events")]`, so the bare `reply` clause sends it there.

## A broadcast

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/queues_and_topics.rs#topic"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/queues_and_topics.rs#topic"
```

</TabItem>
</Tabs>

[`AmqpAddress::topic`](../components/subscriptions/topic.md) asks for a topic: the link carries the capability `topic`.
Artemis makes `order-events` a multicast address and gives each subscription a queue of its own.
So `bill` and `audit` each get a copy of every `OrderTaken`.

A topic subscription lives as long as its link.
What is published to `order-events` while the service is down is not kept for it.
A reader that must not miss a message reads a queue.

## A broadcast that waits for its reader

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/queues_and_topics.rs#durable"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/queues_and_topics.rs#durable"
```

</TabItem>
</Tabs>

The ledger must see every order, also the ones taken while it was stopped.
The operator creates a durable queue `ledger` on the multicast address `order-events`:

```bash
docker compose exec artemis /opt/activemq-artemis/bin/artemis queue create \
  --user artemis --password artemis --silent \
  --name ledger --address order-events --auto-create-address \
  --multicast --durable --preserve-on-no-consumers
```

Every message published to `order-events` now lands in `ledger` too, whether `post_to_ledger` runs or not.
`order-events::ledger` is the queue's fully qualified name in Artemis: the address, two colons, the queue.
[`AmqpAddress::raw`](../components/subscriptions/raw.md) sends an address as written, with no capability, so the link attaches to that queue.
A bare `#[subscriber("order-events::ledger")]` does the same.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/queues_and_topics.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/queues_and_topics.rs#app"
```

</TabItem>
</Tabs>

`.out_reply(Publish)` names the publisher of the replies, [`AmqpPublish`](../components/publishing/index.md) under its [prelude](../components/broker/prelude.md) name.
A publish attaches a sender link to the reply's address on first use and keeps it.
A message the broker refuses fails the publish with [`AmqpError::PublishNotAccepted`](../components/broker/errors.md).

## Headers and other clients

The well-known [headers](../components/publishing/headers.md) travel in the AMQP `properties` section: `content-type`, `correlation-id`, `reply-to`, `message-id`.
Every other header travels in `application-properties`.
A message from this crate is a plain AMQP message, so a client in another language reads it without RustStream.

The partition key travels as the [`PARTITION_KEY_HEADER`](../components/publishing/keys.md) header and becomes the message's `group-id`.
Artemis uses `group-id` for [message grouping](https://artemis.apache.org/components/artemis/documentation/latest/message-grouping.html): every message of one group goes to the same consumer of a queue.
So orders keyed by customer keep their order across replicas.

## Addresses on other brokers

Other brokers name their nodes their own way, and `raw` passes their spelling as it is.
Their own documentation describes it:
[RabbitMQ 4.x](https://www.rabbitmq.com/docs/amqp),
[Azure Service Bus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-protocol-guide),
[ActiveMQ Classic](https://activemq.apache.org/components/classic/documentation/amqp), which [Amazon MQ](https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/working-with-activemq.html) runs.

## Testing the routing

The tests in the example file run the app in process.
One order publishes one `OrderTaken`, `orders` is called once, and `order-events` is called twice, once for each topic subscription.
The in-process broker delivers a `raw` address exactly as written, so its test publishes to `order-events::ledger` directly.
[Testing an AMQP service](testing.md) covers the harness.
