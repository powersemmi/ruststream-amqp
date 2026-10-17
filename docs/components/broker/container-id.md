---
sidebar_position: 1
title: container_id
description: "container_id names the service to the broker, which is how an operator finds its links in the broker's console; the default is ruststream."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# container_id

[`container_id`](api:ruststream_amqp::AmqpBroker::container_id) names the service to the broker, which is how an operator finds its links in the broker's console; the default is `ruststream`.

Every AMQP 1.0 connection opens with a container id, the name of the process on each end.
Each link the service attaches takes its name from it, with a role and a number that keeps the name unique on the connection:

| Link | Name |
|---|---|
| a subscription's receiver | `<container>-receiver-<n>` |
| a publisher's sender, one per address | `<container>-sender-<n>` |
| the reply link of a [request](../request-reply/index.md) | `<container>-reply-<n>` |
| the controller of a [transaction](../transactions/index.md) | `<container>-txn-<n>` |

The generated [AsyncAPI document](../asyncapi/server.md) reports the container id beside the server's host and port, in the `x-ruststream-amqp1` extension.

## Example

The service presents itself to the broker as `orders-svc`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_broker_container.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_broker_container.rs#broker"
```

</TabItem>
</Tabs>
