---
title: Subscriptions
description: "AmqpAddress describes a subscription: the address, the terminus it asks for and its link settings; a bare name sends the address as written."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Subscriptions

[`AmqpAddress`](api:ruststream_amqp::AmqpAddress) describes a subscription: the address, the terminus it asks for and its link settings; a bare name sends the address as written.

`#[subscriber("orders")]` opens a receiver on the address `orders`.
The name reaches the broker as written, with no terminus capability, which makes it the same subscription as [`AmqpAddress::raw`](./raw.md).
AMQP 1.0 standardises the wire, not what an address means, so whether the subscriptions on one address compete or each get a copy is a question the broker answers.
A descriptor asks the question explicitly: [`AmqpAddress::queue`](./queue.md) asks for competing consumers, [`AmqpAddress::topic`](./topic.md) for a copy each.
A descriptor goes where the name goes, as the attribute's first argument.
It also carries the link settings: the [credit](./credit.md), the [settle mode](../settlement/settle.md) and the deadline of a [batch](./batches.md).
The manual form passes the name to [`subscriber(name, body)`](api:ruststream::runtime::subscriber), and a descriptor joins the chain through [`map_source`](api:ruststream::runtime::SubscriberSettings::map_source).
The subscription's name is its address: a test reads its deliveries under that name, and a [deferred retry](../settlement/retry-after.md) is published back to it.
An empty address stops the service at startup with [`AmqpError::InvalidAddress`](api:ruststream_amqp::AmqpError::InvalidAddress).

[Queues and topics](../../guides/queues-and-topics.md) builds a service on a work queue, a broadcast and a raw address.

## Example

The address `orders`, read as written.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_subscriptions_name.rs#name"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_subscriptions_name.rs#name"
```

</TabItem>
</Tabs>
