---
sidebar_position: 4
description: "Retry an AMQP 1.0 delivery now or later, cap the attempts, move what keeps failing to a dead-letter address and mark the copies."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries and dead letters

This guide retries a delivery now or later, caps the attempts, moves what keeps failing to a dead-letter address and marks the copies.

AMQP 1.0 has no delayed redelivery, and a link cannot declare a delivery limit or a dead-letter address.
The framework covers both: the mount site declares the cap and the dead-letter address, and a delayed retry travels as a copy of the message with a retry count in a header.

The example is a refunds service that sends refunds through a card processor.

## The subscriber

The processor refuses a refund in three ways, and each needs a different outcome.
`Busy` means it takes no more large refunds for now: a retry right now would be refused again, so [`retry_after`](../components/settlement/retry-after.md) waits 30 seconds.
`Unavailable` means the card's bank did not answer: [`retry`](api:ruststream::runtime::HandlerOutcome::retry) tries again at once.
`Declined` means the card account is closed, and another try gets the same answer, so the refund is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

`retry_after` settles the delivery and publishes a copy to `refunds` once the delay has passed.
The copy carries the retry-count header ([`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)), raised by one.
The delayed copy is at most once: a process that stops before the delay ends loses it.
On a queue the copy goes to one consumer like any other message.

## The cap and the dead-letter address

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](../components/settlement/retries.md) counts every delivery of one refund, the first included.
The fifth delivery is not retried: [`dead_letter`](../components/settlement/dead-letters.md) publishes it to `refunds.dead`, payload and headers as they arrived.
A dropped refund is rejected and goes to the broker's own dead-letter policy, not to `refunds.dead`.

Under a cap the framework reads the count from the delivery.
It is the broker's `delivery-count` when the message carries an AMQP `header` section.
A message published by this crate carries none, so `retry` then publishes a copy at once, and the retry-count header counts the attempts.

A cap without `dead_letter` rejects the last delivery instead, and the broker's dead-letter policy decides.
The broker also has a limit of its own: Artemis stops after [`max-delivery-attempts`](https://artemis.apache.org/components/artemis/documentation/latest/undelivered-messages.html), 10 by default.
Choose one owner for the limit: a cap below the broker's limit makes the framework's count the one that applies.

## Reading the dead letters

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`refunds.dead` is an ordinary queue, and `review` reads it like any other.
The retry-count header of a dead letter holds the number of deliveries it went through, 5 here.
`review` reads it with `ctx.headers()` and logs the refund for a person to look at.

## Marking the copies

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#stamp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#stamp"
```

</TabItem>
</Tabs>

A copy has no call site in the handler, so a [publish transform](api:ruststream::runtime::PublishTransform) is the one place that reaches it.
The app mounts `process_refund` with `.out_retry(Publish).transform(Retried)`.
[`.out_retry(Publish)`](../components/settlement/out-retry.md) names the publisher of the copies, and the steps after it apply to them.
`Retried` writes the address the delivery came from into `x-retried-from`.
Every copy goes back to the address of its subscription, so a transform on this position may not change the destination.

## Retries on a topic

A copy is published to the subscription's own address.
On a [topic address](queues-and-topics.md#a-broadcast) that address is shared, so every subscription on the topic gets the copy, not only the one that asked for it.
Retry on a queue when one reader owns the message.

## Testing the delay

The tests in the example file run the app in process with a paused clock, so `tb.advance(BACKOFF)` lets the 30 seconds pass at once.
[Testing an AMQP service](testing.md) covers the harness.
