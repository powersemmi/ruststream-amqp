---
title: Testing
description: "TestApp::start connects AmqpBroker in process: queues, topics, dispositions, request-reply and transactions, with no broker running."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`AmqpBroker`](../broker/index.md) in process: queues, topics, dispositions, request-reply and transactions, with no broker running.

A test hands the harness the app `main` runs, unchanged, and addresses the broker by its production type, `tb.broker::<AmqpBroker>()`.
The `testing` feature in `[dev-dependencies]` adds the in-process mode, and turns on the core's harness with it.
The connected broker then carries an in-process transport in place of the connection, and so do its subscribers, its publishers and its deliveries.
The descriptors and publish policies of the service are the production ones.

The transport has no settings of its own.
It reads the broker's URL as the [connect](../broker/startup.md) reads it, and frames a publish with the same AMQP sections a live publish goes through, so a handler reads the headers a live delivery carries.
It routes by the exact address, as a broker does.
Subscriptions on a [queue](../subscriptions/queue.md) address compete, one delivery each, and every [topic](../subscriptions/topic.md) subscription gets a copy.
A [raw](../subscriptions/raw.md) address is delivered once, like a queue; a test that asserts a broadcast subscribes with `topic`.
A queue subscription creates its address's queue, and a message published while no consumer is attached waits there for the next one.
The [disposition](../settlement/index.md) of a handler's outcome behaves as on a broker: a retry comes back with its `delivery-count` one higher, a drop rejects, and an [at-most-once](../settlement/settle.md) delivery arrives settled.
[Batches](../subscriptions/batches.md), [requests](../request-reply/index.md) with their reply address and timeout, and [transactions](../transactions/index.md) work as they do live.

The transport refuses what a broker refuses.
A message to the empty address is rejected, and a publisher, a subscription, a settlement or a transaction used after shutdown fails with the error a live connection returns.
A publish through the harness returns once every handler it woke has settled, so the assertions that follow read finished state.
What only a broker has, such as link credit, the dead-letter policy behind a rejection, message groups and the login, is for a [live test](./live.md).

## Example

The dependency line:

```toml
[dev-dependencies]
ruststream-amqp = { version = "0.7", features = ["testing"] }
```

The first test publishes a payment and reads how the handler settled it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_testing.rs#testapp"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_testing.rs#testapp"
```

</TabItem>
</Tabs>

An order is answered with a receipt, and the test reads it from the reply address.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_testing.rs#reply"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_testing.rs#reply"
```

</TabItem>
</Tabs>
