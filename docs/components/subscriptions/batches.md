---
sidebar_position: 5
title: Batches
description: "A slice handler gets the deliveries assembled on the client, up to the size the mount site names, closed early by batch_wait."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A slice handler gets the deliveries assembled on the client, up to the size the mount site names, closed early by `batch_wait`.

A handler over `&[T]` is a batch handler, and [`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch) at the mount site is its largest batch.
AMQP 1.0 transfers one message at a time, and [credit](./credit.md) is flow control rather than a batch size, so the subscription assembles the batches itself.
A batch closes when it holds `n` deliveries, or when [`batch_wait`](api:ruststream_amqp::AmqpAddress::batch_wait) has passed since its first delivery.
The default, [`DEFAULT_BATCH_WAIT`](api:ruststream_amqp::DEFAULT_BATCH_WAIT), is 10 ms.
A longer wait fills batches under a trickle of traffic, at the cost of latency for the first delivery of each batch.
`batch_wait` applies to batch handlers only: a single-message subscription hands every delivery over as it arrives.
One [outcome](../settlement/index.md) settles every delivery of the batch, and a `Vec<HandlerOutcome>` settles delivery `i` with outcome `i`.
In the manual form the body is an `impl Handle<[T]>`, where `Ok(())` acks the batch and `Err(outcomes)` settles it delivery by delivery.

[Acknowledgement and delivery guarantees](../../guides/acknowledgement.md) restocks a warehouse from batches.

## Example

Three orders waiting on the subscription arrive in one call.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_batches.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_batches.rs#handler"
```

</TabItem>
</Tabs>

The mount site names the size:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_batches.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_batches.rs#app"
```

</TabItem>
</Tabs>
