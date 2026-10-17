---
title: Path
sidebar_position: 4
description: "The way of one AMQP 1.0 delivery through a RustStream service: the pump, the channel, the handler, the reply and the disposition."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of one delivery that is answered, from the broker's transfer to its disposition",
  "text": "The diagram as text",
  "transfer": ["transfer", "on the subscription's receiver link"],
  "pump": ["Pump task: Receiver::recv", "headers from the properties sections,", "bytes from the data section"],
  "channel": ["Bounded channel", "as deep as the link credit"],
  "dispatch": ["Dispatch", "one loop, or a lane per key"],
  "decode": ["Decode", "from the delivery's bytes"],
  "body": ["Handler", "async fn under #[subscriber]"],
  "encode": ["Reply", "encoded into the loop's buffer"],
  "publisher": ["AmqpPublisher", "holds the address's sender link", "until the broker's disposition"],
  "settle": ["Settlement", "a command back to the pump task"],
  "disposition": ["disposition", "accepted, modified or rejected"]
};

# Path

The way of one AMQP 1.0 delivery through a RustStream service: the pump, the channel, the handler,
the reply and the disposition.

<Diagram name="path" words={words} />

A subscription's pump task owns its receiver link. Receiving needs the link to itself, while a
settlement only reads it, and the delivery a handler holds has to travel between tasks. So the pump
receives, and every settlement comes back to it as a command.

The pump turns a transfer into an [`AmqpMessage`](api:ruststream_amqp::AmqpMessage). The headers
come from the `properties` and `application-properties` sections, the bytes from the `data`
section, and `delivery-count` from the `header` section. A string or binary value section is read
as bytes too, and any other body is rejected and reported to the subscription. The pump then puts
the delivery in a channel as deep as the link credit, and settles commands while it waits for room
there.

A delivery the client cannot read, such as one larger than the link allows, is reported and the
link stays attached. A broker that transfers past the credit it was given breaks the link, and the
subscription ends.

The handler's reply is encoded into a buffer the dispatch loop reuses. The
[`AmqpPublisher`](api:ruststream_amqp::AmqpPublisher) takes that buffer as the message's `data`
section, without a copy. It sends the message unsettled on the address's sender link and holds the
link until the broker's disposition arrives. So the publishes to one address from one connection
go out one at a time, each one round trip. A disposition other than `accepted` is an error, and a
reply that fails to publish settles the delivery with a retry.

The delivery settles after the reply. Its outcome becomes a disposition, as
[Settlement](settlement.md) draws.
