---
sidebar_position: 3
description: "Settle AMQP 1.0 deliveries with accept, modified and reject, cap the attempts, read at most once, size the link credit and handle batches."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Acknowledgement and delivery guarantees

This guide settles deliveries, caps the attempts, reads a queue at most once, sizes the link credit and handles batches.

In AMQP 1.0 a receiver settles each delivery with an outcome, and the broker acts on it.
A handler's outcome is that disposition.

The example is a warehouse.
It picks shipments, logs every pick, and restocks its shelves from deliveries that come in batches.

## The three outcomes

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#pick"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#pick"
```

</TabItem>
</Tabs>

| Handler outcome | Disposition | What the broker does |
|---|---|---|
| `HandlerOutcome::ack()` | `accepted` | removes the message |
| `HandlerOutcome::retry()` | `modified` with `delivery-failed` | counts the attempt in `delivery-count` and delivers the message again |
| `HandlerOutcome::drop()` | `rejected` | applies its dead-letter policy |

`pick` accepts a shipment it picked.
An empty shipment can never be picked, so `pick` rejects it.
Artemis moves a rejected message to its dead-letter address, `DLQ` by default.
A shipment of goods that are out of stock may be picked later, so `pick` asks for it again.
Artemis stops delivering a message after [`max-delivery-attempts`](https://artemis.apache.org/components/artemis/documentation/latest/undelivered-messages.html), 10 by default, and moves it to the dead-letter address.

## Capping the attempts

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) caps the deliveries of one shipment at five, the first included.
The fifth is rejected instead of asked for again.
The count is the broker's `delivery-count` when the message carries an AMQP `header` section.
A message this crate publishes carries none.
For such a message the framework counts the attempts itself: `retry` publishes a copy back to `shipments` with a retry count in a header.
[Retries and dead letters](retries.md) adds a dead-letter address and a delay.

## A crash in the middle

The subscriptions are at least once by default.
A delivery the service has not settled when it stops goes back to the queue, and the next consumer gets it.
So a handler may see one message twice, and its work has to tolerate that.

## At most once

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#audit"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#audit"
```

</TabItem>
</Tabs>

[`Settle::AtMostOnce`](api:ruststream_amqp::Settle::AtMostOnce) settles each delivery on receipt.
The broker forgets the message the moment it hands it over, so a crash before the handler finishes loses it.
That suits the pick log, where a missing line costs less than a second pass over the broker.
The handler's outcome has nothing left to settle: `ack` and `nack` report `AckError::Unsupported`, and the runtime logs a warning for each.

## Credit

`.credit(nonzero!(32))` on `shipments` is the link credit: how many deliveries the broker may send ahead of the service's settlements.
The default is [`DEFAULT_CREDIT`](api:ruststream_amqp::DEFAULT_CREDIT), 256.
Once half of the credit is settled, the client grants the full credit again.
A slow handler therefore holds the broker back instead of filling the service's memory, and the rest of the queue stays on the broker for the other replicas.
The credit is a `NonZeroU32`, because a link with no credit receives nothing: `credit(nonzero!(0))` does not compile.

## Batches

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#restock"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#restock"
```

</TabItem>
</Tabs>

A handler that takes a slice gets a batch, and `.batch(nonzero!(20usize))` at the mount site sets its largest size.
AMQP 1.0 transfers one message at a time, so the batch is put together on the client.
A batch holds at most 20 deliveries, and fewer when no more arrived in time.
[`batch_wait`](api:ruststream_amqp::AmqpAddress::batch_wait) is how long a partial batch waits for more, 50 milliseconds here and 10 by default.
The outcome of `restock` settles every delivery of the batch.

## Testing the outcomes

The tests in the example file run the warehouse in process.
They check each outcome, the five deliveries of a shipment out of stock, the pick log and the batches.
Credit, the broker's dead-letter policy and its delivery limit belong to Artemis, so the in-process broker leaves them out.
[Testing an AMQP service](testing.md) covers the harness.
