---
sidebar_position: 2
title: topic
description: "AmqpAddress::topic asks for the topic terminus: every subscription on the address gets its own copy of each message."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# topic

[`AmqpAddress::topic`](api:ruststream_amqp::AmqpAddress::topic) asks for the topic terminus: every subscription on the address gets its own copy of each message.

The descriptor attaches its receiver with the terminus capability `topic` on the link's source.
ActiveMQ Artemis and the other brokers that read the capability treat the address as multicast.
That is a broadcast: an audit trail and a notifier read the same events, each at its own pace.
A [deferred retry](../settlement/retry-after.md) publishes its copy to the address, so every topic subscription on it receives the copy, not only the one that asked.
In process, [`TestApp`](../testing/index.md) copies each message to every topic subscription of the address.

## Example

Two subscriptions of one service on the address `orders.placed`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_subscriptions_topic.rs#topic"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_subscriptions_topic.rs#topic"
```

</TabItem>
</Tabs>
