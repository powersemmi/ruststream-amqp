---
title: Transfers
sidebar_position: 2
description: "A sender transfers a message only against the receiver's link credit, and the receiver settles each one with a disposition that carries its outcome."
---

import {Diagram} from '@site/src/components/Diagrams';

export const credit = {
  "title": "Link credit over time: the receiver grants it, the sender spends it, and the client grants a new window once half has been settled",
  "text": "The diagram as text",
  "receiverLane": "Receiver: the service",
  "senderLane": "Sender: the broker",
  "grant": ["flow", "link-credit 256"],
  "send": ["transfers", "up to the credit"],
  "settle": ["dispositions", "for 128 deliveries"],
  "refill": ["flow", "link-credit 256 again"],
  "resume": ["transfers", "continue"],
  "flow": "flow",
  "transfer": "transfer",
  "half": "half settled",
  "rule": ["Credit counts transfers from the last flow:", "a delivery the service still holds does not return it"]
};

export const outcomes = {
  "title": "The four outcomes of a disposition and what each leaves the message as on the broker",
  "text": "The diagram as text",
  "outcomeLane": "Disposition",
  "brokerLane": "Broker",
  "accepted": ["accepted"],
  "acceptedEffect": ["done: the message", "leaves the queue"],
  "rejected": ["rejected"],
  "rejectedEffect": ["invalid: the broker's", "dead-letter policy decides"],
  "released": ["released"],
  "releasedEffect": ["not processed: back on", "the queue, count unchanged"],
  "modified": ["modified", "delivery-failed"],
  "modifiedEffect": ["failed: back on the", "queue, delivery-count + 1"],
  "modes": ["A sender sends unsettled, settled or mixed (the default);", "a transfer settled on sending is delivered at most once"]
};

# Transfers

A sender transfers a message only against the receiver's link credit, and the receiver settles
each one with a disposition that carries its outcome.

<Diagram name="transfers" words={credit} />

Credit is the receiver's flow control. The receiver sends a `flow` frame with a credit, and the
sender may make that many transfers before it waits for the next one. A subscription grants 256 by
default, and [`credit`](../../components/subscriptions/credit.md) sets another count.

The `fe2o3-amqp` client manages the credit itself. Each time half of the credit has been settled,
it grants the full credit again, counted from the transfers received so far. So the sender never
runs dry while the service works, and it never runs more than one window ahead of the last grant.

<Diagram name="outcomes" words={outcomes} />

A transfer the sender did not settle stays open until the receiver answers with a disposition. Its
outcome is one of four. `accepted` finishes the message. `rejected` calls it invalid, and the
broker's dead-letter policy decides what happens to it. `released` returns it untouched, and the
next delivery carries the same `delivery-count`. `modified` with `delivery-failed` returns it as
a failed attempt, and the next delivery counts one more. `undeliverable-here` on `modified` asks
the broker not to deliver it to this receiver again.

`delivery-count` lives in the message's `header` section and counts the earlier attempts that
failed. Brokers differ on `modified`: Artemis counts it, RabbitMQ 4 supports it on quorum queues
only, and Azure Service Bus does not use it.

The flow control and the settle modes are the specification's
([transport, sections 2.6.7 and 2.8](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html)),
and so are the outcomes
([messaging, section 3.4](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html)).
