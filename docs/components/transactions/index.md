---
title: Transactions
description: "TransactionalPublish posts under an AMQP transaction: what a publisher sends between begin and commit becomes visible together, or not at all."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Transactions

[`AmqpTransactionalPublish`](api:ruststream_amqp::AmqpTransactionalPublish), `TransactionalPublish` in the prelude, posts under an `AMQP` transaction: what a publisher sends between begin and commit becomes visible together, or not at all.

The policy needs the `transaction` feature.
The runtime pairs it into an [`AmqpTxnPublisher`](api:ruststream_amqp::AmqpTxnPublisher), which implements [`TransactionalPublisher`](api:ruststream::TransactionalPublisher).
A handler binds its slot as `Out<impl TransactionalPublisher>`, and the mount site names the policy with `.out(DefaultSlot, TransactionalPublish).build()`.
`begin_transaction` declares a transaction on the broker.
Each publish after it is posted under that transaction and waits for the broker's outcome, as a plain publish does.
`commit` makes every posted message visible at once; `abort` discards them.
A publish with no transaction open goes out at once.

The handle holds one transaction at a time.
A second `begin_transaction` while one is open fails and leaves the open one untouched.
`commit` or `abort` with nothing open fails too.
A commit or an abort that fails has still ended the transaction, so the next `begin_transaction` starts a new one.
Each failure is an [`AmqpError::Transaction`](api:ruststream_amqp::AmqpError::Transaction).

The slot's publisher is paired once, at startup, so every delivery of the registration uses the same handle.
A registration that runs deliveries side by side with `workers(n)` has them compete for the one transaction, and a second `begin_transaction` fails until the first ends.

A transaction covers what the publisher posts.
The delivery the handler answers settles on its own after the handler returns, so a delivery redelivered after a commit posts its messages again.

## Example

The dependency line:

```toml
ruststream-amqp = { version = "0.7", features = ["transaction"] }
```

The handler posts one invoice line per order line and commits them together; an empty line aborts the order.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_transaction.rs#transaction"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_transaction.rs#transaction"
```

</TabItem>
</Tabs>

The mount site gives the slot the transactional policy.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_transaction.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_transaction.rs#app"
```

</TabItem>
</Tabs>
