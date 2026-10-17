---
sidebar_position: 1
title: queue
description: "AmqpAddress::queue asks for the queue terminus: subscriptions on the address compete, and each message goes to one of them."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# queue

[`AmqpAddress::queue`](api:ruststream_amqp::AmqpAddress::queue) asks for the queue terminus: subscriptions on the address compete, and each message goes to one of them.

The descriptor attaches its receiver with the terminus capability `queue` on the link's source.
ActiveMQ Artemis and the other brokers that read the capability treat the address as anycast.
That is a work queue: two replicas of a service share the orders, and no order is handled twice.
A delivery handed back with a `retry` goes to whichever receiver the broker picks next.
Once a queue subscription has attached, the queue keeps what is published while no receiver is attached, so a replica that restarts finds its backlog.
A delivery the process held unsettled when it stopped returns to the queue as well.
In process, `TestApp` hands the messages of a queue address to its subscriptions in turn, as the example's test shows with two replicas.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_subscriptions_queue.rs#queue"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_subscriptions_queue.rs#queue"
```

</TabItem>
</Tabs>
