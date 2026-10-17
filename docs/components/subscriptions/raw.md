---
sidebar_position: 3
title: raw
description: "AmqpAddress::raw sends the address as written with no capability, for a deployment's own convention such as /queues/orders on RabbitMQ."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# raw

[`AmqpAddress::raw`](api:ruststream_amqp::AmqpAddress::raw) sends the address as written with no capability, for a deployment's own convention such as `/queues/orders` on RabbitMQ.

RabbitMQ 4.x addresses a queue by its v2 path, `/queues/<name>`, and ActiveMQ Artemis accepts a fully qualified queue name.
A bare name in `#[subscriber("orders")]` is a raw address too.
With no capability on the link, the broker's own configuration decides whether the subscriptions on the address compete or each get a copy.
The address is matched exactly: `orders` and `/queues/orders` are two different nodes.
In process, `TestApp` delivers each message of a raw address once, as on a queue, because there is no broker configuration to consult.
A service whose subscriptions must each get a copy says [`topic`](./topic.md), which is also what the brokers that need the capability have to be told.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_subscriptions_raw.rs#raw"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_subscriptions_raw.rs#raw"
```

</TabItem>
</Tabs>
