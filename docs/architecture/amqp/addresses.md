---
title: Addresses
sidebar_position: 1
description: "An address names a node in the broker, and the capability a receiver asks for decides whether its receivers share a queue or each get a copy."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Three ways receivers meet an address: competing for one queue, a subscription queue each, and a node the broker creates",
  "text": "The diagram as text",
  "anycast": "Address orders: anycast",
  "queue": ["Queue orders", "each message once"],
  "worker1": ["Receiver 1", "capability queue"],
  "worker2": ["Receiver 2", "capability queue"],
  "first": "message 1",
  "second": "message 2",
  "multicast": "Address events: multicast",
  "copy1": ["Subscription queue", "of receiver 1"],
  "copy2": ["Subscription queue", "of receiver 2"],
  "audit": ["Receiver 1", "capability topic"],
  "billing": ["Receiver 2", "capability topic"],
  "every": "every message",
  "dynamic": "Dynamic source",
  "requester": ["Receiver", "dynamic = true"],
  "temporary": ["Temporary queue", "named by the broker"],
  "attach": "attach",
  "named": "address"
};

# Addresses

An address names a node in the broker, and the capability a receiver asks for decides whether its
receivers share a queue or each get a copy.

<Diagram name="addresses" words={words} />

A receiver attaches with a source terminus: the address of a node and a list of capabilities.
[`AmqpAddress::queue`](api:ruststream_amqp::AmqpAddress::queue) asks for the capability `queue`,
[`AmqpAddress::topic`](api:ruststream_amqp::AmqpAddress::topic) for `topic`, and
[`AmqpAddress::raw`](api:ruststream_amqp::AmqpAddress::raw) for none.

ActiveMQ Artemis reads the capability. With `queue`, the receivers of an address attach to one
anycast queue and compete: each message goes to one of them. With `topic`, the address is
multicast and Artemis gives each receiver a subscription queue of its own, so each receives every
message. Artemis creates an address and its queues on the first attach unless its settings forbid
it, and `orders::billing` names one queue of an address directly.

A topic subscription keeps the terminus defaults of the specification: not durable, and expiring
with its session. Its subscription queue lives as long as the link, so what is published to the
topic while the service is stopped is not kept for it.

A `raw` address reaches the broker exactly as written, and the broker's own configuration decides
what it is. Other brokers address their nodes in their own way:
[RabbitMQ 4](https://www.rabbitmq.com/docs/amqp) names a queue `/queues/orders` and an exchange
`/exchanges/events/key`, and the queue must exist beforehand.
[Azure Service Bus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-protocol-guide)
names a queue by its name and a topic subscription `events/subscriptions/billing`.

A receiver may ask for a dynamic source instead of naming one. The broker then creates a node,
Artemis a temporary queue, and returns its address in its answer to the attach.
[`request`](api:ruststream_amqp::AmqpPublisher) attaches one such receiver for every request, sends
the address in `reply-to`, and detaches the receiver when the reply has arrived or the timeout has
passed.

The terminus fields are the specification's
([messaging, section 3.5](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html)),
and the address model is Artemis's
([AMQP](https://artemis.apache.org/components/artemis/documentation/latest/amqp.html),
[address model](https://artemis.apache.org/components/artemis/documentation/latest/address-model.html)).
