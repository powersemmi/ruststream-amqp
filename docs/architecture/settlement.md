---
title: Settlement
sidebar_position: 6
description: "What ack, drop, retry and retry_after become on AMQP 1.0: a disposition the broker acts on, and copies the runtime publishes to the same address."
---

import {Diagram} from '@site/src/components/Diagrams';

export const table = {
  "title": "Each outcome of a handler on an at-least-once subscription: the disposition the subscription sends and what the broker does",
  "text": "The diagram as text",
  "outcomeLane": "Handler",
  "wireLane": "Disposition",
  "brokerLane": "Broker",
  "ack": ["ack()"],
  "ackWire": ["accepted"],
  "ackEffect": ["done: the", "message leaves", "the queue"],
  "retry": ["retry()"],
  "retryWire": ["modified", "delivery-failed"],
  "retryEffect": ["back on the", "queue, counted", "one more"],
  "retryAfter": ["retry_after(d)"],
  "retryAfterWire": ["rejected,", "a copy after d"],
  "retryAfterEffect": ["the copy is a", "new message on", "the address"],
  "drop": ["drop()"],
  "dropWire": ["rejected"],
  "dropEffect": ["the broker's", "dead-letter", "policy decides"],
  "unsettled": ["no outcome", "before the", "link goes"],
  "unsettledWire": ["nothing"],
  "unsettledEffect": ["back on the", "queue when the", "link detaches"],
  "atMostOnce": ["At most once, deliveries arrive settled: ack, retry", "and drop report AckError::Unsupported"]
};

export const retries = {
  "title": "A retry on AMQP 1.0: the attempt cap decides between bringing the message back, the dead-letter address and a rejection",
  "text": "The diagram as text",
  "delivery": ["A delivery: its attempt is delivery-count + 1,", "or x-ruststream-retry-count on a copy"],
  "outcome": ["The handler returns", "retry() or retry_after(delay)"],
  "cap": ["max_attempts: is this attempt below the cap?"],
  "requeue": ["retry():", "modified,", "back on", "the queue"],
  "copy": ["retry_after:", "rejected,", "a copy after", "the delay"],
  "deadLetter": ["At the cap:", "a copy to", "the dead-", "letter", "address"],
  "rejected": ["At the cap,", "no dead", "letter:", "rejected"],
  "returns": ["Both come back to", "the subscription's address"],
  "crash": ["A copy waits for its delay inside the process:", "a stop before the timer fires loses it"],
  "handled": "handled",
  "retries": "a retry",
  "below": "below",
  "atCap": "at the cap"
};

# Settlement

What `ack`, `drop`, `retry` and `retry_after` become on AMQP 1.0: a disposition the broker acts on,
and copies the runtime publishes to the same address.

<Diagram name="settlement" words={table} />

A subscription is at least once by default. A delivery stays unsettled until the handler's outcome
reaches the pump task as a disposition. A delivery that never gets one stays on the link, and the
broker returns it to the queue when the link detaches or the connection drops.

A `retry()` sends `modified` with `delivery-failed`, never `released`. A release tells the broker
the delivery was not acted on, and it leaves `delivery-count` as it was. A handler that asks for a
retry did act on it and failed, and an attempt cap reads that count: without the increment a
message that always fails would come back forever. `undeliverable-here` stays false, so the
message may come back to this same subscription.

[`Settle::AtMostOnce`](api:ruststream_amqp::Settle::AtMostOnce) settles each delivery as it
arrives.

| Outcome | At most once |
|---|---|
| `ack`, `drop` | `AckError::Unsupported`, logged: the delivery was settled on arrival |
| `retry()` | `AckError::Unsupported`; under a declared cap, a copy at once where the delivery carries no `delivery-count` |
| `retry_after(d)` | a copy published to the address after `d` |
| no outcome | nothing: the broker already let the message go |

How many times a message has been delivered is `delivery-count` + 1, which
[`redelivery_count`](api:ruststream_amqp::AmqpMessage) reports. A message this crate published has
no `header` section, so it carries no count, and the framework's retry-count header counts for it.

## Retries

<Diagram name="retries" words={retries} />

The cap is declared at the mount site with
[`max_attempts`](api:ruststream::runtime::Mounting::max_attempts), and the first delivery counts
as one. Below the cap, a `retry()` goes back to the broker with `modified`, and the broker counts
it. A copy carries no count of the broker's, so under a cap a `retry()` of a copy publishes the
next copy at once.

AMQP 1.0 holds no message back, so a `retry_after` is the runtime's own. It rejects the delivery,
waits for the delay, and publishes a copy with the retry-count header raised by one. The copy goes
to the subscription's own address, because one node is where a receiver attaches its source and a
sender its target. On a `queue` address the copy competes for receivers like any other message,
and on a `topic` address every subscription receives it.

At the cap, the delivery is published as it arrived to the
[dead-letter address](api:ruststream::runtime::Mounting::dead_letter). With no dead-letter address
declared it is rejected, and the broker's own dead-letter policy decides. A dead-letter address
declared without a cap takes every retry.
