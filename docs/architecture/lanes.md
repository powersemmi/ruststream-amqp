---
title: Lanes
sidebar_position: 5
description: "One subscription reads its queue through one receiver link and one pump task; workers(n, by_key) spreads the deliveries over lanes keyed by group-id."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "One subscription's dispatch: the queue, the receiver link, the pump task, one stream and two lanes keyed by group-id",
  "text": "The diagram as text",
  "queue": ["Queue orders", "on the broker"],
  "link": ["Receiver link", "credit 256: how far the broker sends ahead"],
  "pump": ["Pump task", "settles while a delivery waits for room"],
  "stream": ["AmqpSubscriber stream", "a channel as deep as the credit"],
  "keyed": ["workers(2, by_key)", "lane = hash of the group-id"],
  "lane0": ["Lane 1", "groups a and c,", "each in order"],
  "lane1": ["Lane 2", "group b, in order"],
  "replicas": ["More replicas compete for the queue, and Artemis", "message groups keep one group-id on one receiver"],
  "transfers": "transfers",
  "received": "received",
  "bounded": "handed over",
  "oneAtATime": "one at a time"
};

# Lanes

One subscription reads its queue through one receiver link and one pump task;
`workers(n, by_key)` spreads the deliveries over lanes keyed by `group-id`.

<Diagram name="lanes" words={words} />

The broker sends against the link's credit, 256 transfers by default, and the client grants more as
the service settles. The pump task hands each delivery to a channel as deep as the credit, so the
service holds no more than one credit's worth of deliveries waiting for a handler. While a delivery
waits for room in that channel, the pump keeps applying settlements, so a stream nobody polls
cannot hold the acknowledgements of the deliveries already handed out.

The stream yields one delivery at a time. Without `workers`, one loop runs the handler for each in
turn, and the queue's order is kept.

`workers(n, by_key)` runs `n` lanes. A delivery goes to the lane its partition key hashes to, and a
lane runs its deliveries in order. The partition key rides the message's `group-id` property, so
the messages of one group keep their order while different groups run side by side. A delivery
without a key has no lane key, and such deliveries rotate over the lanes.

A service scales past one process by running more replicas on the same `queue` address: their
receivers compete, and each message goes to one of them. Artemis message groups send every message
of one `group-id` to the same receiver, so a group keeps its order across replicas too. On Azure
Service Bus, `group-id` is the session id.

A [batch handler](api:ruststream_amqp::AmqpAddress::batch_wait) takes a batch the client assembles
from the stream: up to the size the mount site names, closed early once `batch_wait` (10
milliseconds by default) passes. Credit and the batch size stay separate: credit bounds what the
broker sends ahead, and the size is how many messages one handler call sees.
