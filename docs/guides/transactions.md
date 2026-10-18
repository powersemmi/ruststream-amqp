---
sidebar_position: 6
description: "Publish several AMQP 1.0 messages from one handler together or not at all, with transactional posting and an abort that publishes nothing."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Publishing all or nothing

This guide publishes several messages from one handler together or not at all.

AMQP 1.0 has [transactions](../components/transactions/index.md) in the protocol: a sender posts messages inside a transaction, and the broker makes them visible on the commit.
The `transaction` feature turns this on:

```toml title="Cargo.toml"
[dependencies]
ruststream-amqp = { version = "0.7", features = ["transaction"] }
```

The example is a billing service.
It turns each order into ledger entries and an invoice, and either all of them reach the broker or none does.

## The handler

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/transactions.rs#bill"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/transactions.rs#bill"
```

</TabItem>
</Tabs>

The `Billing` slot is bounded `Out<impl TransactionalPublisher, _>`: the body says it needs transactions and names no broker.
[`begin_transaction`](api:ruststream::TransactionalPublisher::begin_transaction) opens a transaction on the slot's publisher.
Every publish after it is posted inside the transaction, and the broker holds it back.
[`commit`](api:ruststream::TransactionalPublisher::commit) makes the ledger entries and the invoice visible together.
A line without a price makes the order unbillable, and [`abort`](api:ruststream::TransactionalPublisher::abort) discards what was posted, so nothing reaches `ledger` or `invoices`.

A commit or an abort that fails has still ended the transaction, so the next `begin_transaction` starts a fresh one.
`bill` retries the order when the commit fails.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/transactions.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/transactions.rs#app"
```

</TabItem>
</Tabs>

`TransactionalPublish` is the policy that carries transactions, [`AmqpTransactionalPublish`](api:ruststream_amqp::AmqpTransactionalPublish) under its [prelude](../components/broker/prelude.md) name.
Its publisher holds one transaction at a time.
A second `begin_transaction` while one is open returns an error and leaves the open one untouched.
Each registration gets a publisher of its own, so two handlers can each hold a transaction at the same time.
A publish on this publisher outside a transaction goes out at once.

A transaction that starts the service, with no delivery to handle, runs from a startup hook: `b.after_startup(TransactionalPublish, ..)` hands the hook the same publisher.

## The order's own delivery

The transaction covers what the handler publishes.
The order's delivery is settled after the handler returns, outside the transaction.
A crash between the commit and the settlement delivers the order again, and its ledger entries and invoice are published a second time.
Give the messages an id the readers can use to drop a repeat, such as the order's id here.

## Testing the transaction

The tests in the example file run the service [in process](../components/testing/index.md).
The in-process broker publishes nothing before the commit and discards the messages of an abort, so the tests read the outcome from what was published.
[Testing an AMQP service](testing.md) covers the harness.
