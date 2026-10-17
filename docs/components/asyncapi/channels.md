---
sidebar_position: 2
title: Channels
description: "A subscription's channel reports its address, terminus, credit and settle mode; a publish channel reports the address its sender targets."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Channels

A subscription's channel reports its address, terminus, credit and settle mode; a publish channel reports the address its sender targets.

A subscription's extension has four fields, read from its [`AmqpAddress`](api:ruststream_amqp::AmqpAddress).
`address` is the node the receiver attaches to.
`capability` is the terminus it asks for, `queue` or `topic`.
An `AmqpAddress::raw` address asks for none, so its channel has no `capability`.
`credit` is the link credit, 256 unless the descriptor sets [`credit`](api:ruststream_amqp::AmqpAddress::credit).
`settleMode` is `at-least-once`, or `at-most-once` for a descriptor with [`Settle::AtMostOnce`](api:ruststream_amqp::Settle::AtMostOnce).

A channel the service publishes to carries one field, `address`: the node the sender attaches its target to.
That is a reply destination, the name of an `Out` slot, or a dead-letter address.
A reply [routed per request](./replies.md) has no fixed address on its channel, and the extension still names the destination its mount site declared.

## Example

The channel `orders` reports a queue with a credit of 64, `events` a topic settled at most once, and `/queues/audit` no terminus.
The channels `receipts`, `orders.dead` and `ledger` report the address each sender targets.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_asyncapi.rs#channels"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_asyncapi.rs#channels"
```

</TabItem>
</Tabs>
