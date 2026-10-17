---
title: Connection
sidebar_position: 0
description: "AMQP 1.0 joins two containers by one connection, which carries sessions, and each session carries links that move messages one way."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One connection between the service and the broker: a session for the publishers, a session for each subscription, and links inside them",
  "text": "The diagram as text",
  "service": ["Service container", "container id ruststream"],
  "connection": "One TCP connection",
  "opens": "TLS, SASL, open",
  "publishers": "Session: publish",
  "publisherWindow": ["Session window", "shared by publishers"],
  "sender": ["Sender link", "target receipts", "one per address"],
  "subscription": "Session: orders",
  "subscriptionWindow": ["Session window", "of one subscription"],
  "receiver": ["Receiver link", "source orders", "credit 256"],
  "broker": "Broker container",
  "receipts": ["Node receipts", "a queue or a topic"],
  "orders": ["Node orders", "a queue or a topic"],
  "names": ["Link names are unique per connection; the crate derives", "them from the container id, as in ruststream-receiver-3"],
  "transfers": "transfers"
};

# Connection

AMQP 1.0 joins two containers by one connection, which carries sessions, and each session carries
links that move messages one way.

<Diagram name="connection" words={words} />

A container is a peer of the protocol: the service is one and the broker is another. The service
presents the container id `ruststream` unless
[`container_id`](../../components/broker/container-id.md) names another, and a broker's
console lists the service's connection and links under it.

The connection is one TCP socket. On an `amqps://` URL it starts with TLS. SASL then
authenticates the service with PLAIN, ANONYMOUS or EXTERNAL, and the two containers exchange
`open`.

A session is a pair of channels inside the connection, each with a window that bounds the
transfers in flight on it. The crate begins one session that every publisher of the connection
shares, and one more for each subscription. A slow subscription fills its own window, never the
publishers' one or another subscription's.

A link is named and carries messages in one direction. A sender link goes from the service to a
node, and a receiver link from a node to the service. Each end of a link is a terminus, and the
terminus on the broker's side names a node by its address. A link also has a credit of its own,
which [Transfers](transfers.md) draws.

The standard fixes the frames on the wire, not what an address means. Whether `orders` is a queue
or a topic is the broker's decision, which [Addresses](addresses.md) draws.

The model is the specification's
([transport](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html),
[security](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html)).
