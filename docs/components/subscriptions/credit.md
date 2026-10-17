---
sidebar_position: 4
title: credit
description: "credit is the link credit: how many deliveries the broker may send ahead of the handler's settlements, 256 by default and never zero."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# credit

[`AmqpAddress::credit`](api:ruststream_amqp::AmqpAddress::credit) sets the link credit: how many deliveries the broker may send ahead of the handler's settlements, 256 by default and never zero.

AMQP 1.0 has flow control instead of a prefetch count: a receiver grants credit, and the broker sends a delivery only while it holds some.
The subscription grants the full credit when it attaches.
Each time half of the credit has been settled, it grants the full credit again, counted from the last delivery the broker sent.
So the broker never runs more than `credit` deliveries ahead of the last grant, but the deliveries held unsettled can outnumber the credit.
With a credit of 4, the second settlement brings four more deliveries while two are still open: six unsettled on a credit of four.
The default, [`DEFAULT_CREDIT`](api:ruststream_amqp::DEFAULT_CREDIT), is 256, and its first top-up comes after 128 settlements.
On a [queue](./queue.md), a lower credit spreads a backlog across more replicas, and a higher one keeps a fast handler busy.
Under [at-most-once](../settlement/settle.md) every delivery is settled as it arrives, so the credit tops itself up without waiting for the handler.
The credit is a `NonZeroU32`: `credit(nonzero!(0))` does not compile, because a receiver with no credit receives nothing.
Credit is a property of the link, which only a broker has: in process, `TestApp` hands every delivery over at once, and the example's live test holds the window to what a broker does.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_subscriptions_credit.rs#credit"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_subscriptions_credit.rs#credit"
```

</TabItem>
</Tabs>
