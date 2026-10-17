---
sidebar_position: 3
title: Operations
description: "A send operation says how it posts: confirmed for Publish, transactional for TransactionalPublish."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Operations

A send operation says how it posts: `confirmed` for `Publish`, `transactional` for `TransactionalPublish`.

The extension of a send operation has one field, `posting`, and the publish policy at its mount site decides it.
[`Publish`](../publishing/index.md) posts `confirmed`: each transfer waits for the broker's outcome, and anything but `accepted` fails the publish.
[`TransactionalPublish`](../transactions/index.md) posts `transactional`: the transfers carry a transaction and become visible at its commit.
A dead-letter destination has a send operation of its own, and it posts `confirmed`.

A reply has no send operation.
What a reply policy adds to the document is on its [reply](./replies.md).

## Example

The operation `send_events_orders_dead` posts `confirmed`, and `send_payments_ledger` posts `transactional`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_asyncapi.rs#operations"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_asyncapi.rs#operations"
```

</TabItem>
</Tabs>
