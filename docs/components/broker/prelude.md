---
sidebar_position: 5
title: Prelude
description: "One glob, ruststream_amqp::prelude, brings the core prelude, the broker, the descriptor and the policies under Publish and TransactionalPublish."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Prelude

One glob, [`ruststream_amqp::prelude`](api:ruststream_amqp::prelude), brings the core prelude, the broker, the descriptor and the policies under `Publish` and `TransactionalPublish`.

`use ruststream_amqp::prelude::*;` is the one import an AMQP 1.0 service writes.
It carries the core's own prelude, so `#[subscriber]`, `RustStream`, `HandlerOutcome`, `Out` and `nonzero!` come with it.
It adds [`AmqpBroker`](./index.md) with its authentication [`Sasl`](./security.md), the address descriptor [`AmqpAddress`](../subscriptions/index.md) with its delivery guarantee [`Settle`](../settlement/settle.md), the crate's error [`AmqpError`](./errors.md), and the capability trait `RequestReply`.

The publish policies come under the names every broker crate uses: `Publish` is [`AmqpPublish`](../publishing/index.md), and with the `transaction` feature `TransactionalPublish` is [`AmqpTransactionalPublish`](../transactions/index.md), next to the capability trait `TransactionalPublisher`.
So a mount site reads the same on every broker.
Two broker preludes in one file collide on these names; the prefixed originals, exported here too, tell them apart.

A handler body bounds its slots with capability traits, such as `Out<impl Publisher>` or `Out<impl RequestReply>`, and names no broker type.
A service that reads a delivery's [partition key](../publishing/keys.md) through the `Partitioned` capability imports `ruststream::Partitioned` itself.
In the glob it would make `partition_key()` ambiguous with the method of the same name that the core prelude brings.

## Example

The broker, its authentication, the descriptor with its settings and the reply policy all come from the one glob.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_broker_prelude.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_broker_prelude.rs#app"
```

</TabItem>
</Tabs>
