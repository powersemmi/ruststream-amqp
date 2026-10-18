---
sidebar_position: 4
title: retry_after
description: "AMQP 1.0 has no delayed redelivery, so retry_after drops the delivery and publishes a copy to the subscription's own address after the delay."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry_after

AMQP 1.0 has no delayed redelivery, so [`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) drops the delivery and publishes a copy to the subscription's own address after the delay.

The runtime rejects the delivery, waits out the delay and publishes a copy of the message, payload and headers, with [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) incremented.
The copy goes to the subscription's address, because one AMQP node is both what a receiver attaches to and what a sender publishes to, so the mount site names no destination.
A bare name and every descriptor answer the same way.
On a [queue](../subscriptions/queue.md) the copy competes for the receivers like any other message.
On a [topic](../subscriptions/topic.md) every subscription on the address receives the copy, not only the one that asked for it.
The copy waits in the service's memory, so the retry is at-most-once over the delay: a process that stops before the timer fires loses it.
The rejected original is the broker's to handle: ActiveMQ Artemis moves it to its `DLQ` address by default, beside the copy that comes back.
A [cap](./retries.md) reads the header of the copy, so it holds across delays.
In a [test](../testing/index.md) the delay runs on the test's clock: `tb.advance(delay)` brings the copy back.

## Example

A rate-limited charge comes back after 30 seconds.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_retry.rs#retry_after"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_retry.rs#retry_after"
```

</TabItem>
</Tabs>
