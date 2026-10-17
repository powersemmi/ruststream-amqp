---
sidebar_position: 4
title: Shutdown
description: "Shutdown ends the subscriptions, the publisher links and the connection in turn; a publisher used afterwards reports NotConnected."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shutdown

Shutdown ends the subscriptions, the publisher links and the connection in turn; a publisher used afterwards reports [`AmqpError::NotConnected`](api:ruststream_amqp::AmqpError::NotConnected).

When shutdown begins, the runtime stops reading and waits for the handlers still running.
The app's [`shutdown_timeout`](api:ruststream::runtime::RustStream::shutdown_timeout) bounds that wait; without it the runtime waits until every handler has finished.
A publish returns once the broker has settled the message, so what the handlers published is on the broker by then, and nothing is left to send.

The broker then closes from the inside out: each subscription's session ends, the publishers' sender links detach, their shared session ends, and the connection closes.
Each layer still has to carry the broker's answer to the layer inside it, which fixes the order.
The connection closes only after every subscription's session has ended, the sessions of subscriptions still open at shutdown included, so a clean stop reports no error.
Every step runs even after an earlier one fails, so a stuck link cannot leave the connection open.
The shutdown reports the first failure in that order: [`AmqpError::Detach`](api:ruststream_amqp::AmqpError::Detach) for a link, `AmqpError::Session` for the session, `AmqpError::Connect` for the connection.

A publisher taken before the shutdown keeps pointing at the closed connection.
A publish through it, or a transaction begun on a [transactional publisher](../transactions/index.md), returns `AmqpError::NotConnected` and never succeeds against the dead connection.
A delivery settled after the shutdown reports an error, and the stream of a subscription that was still open ends.

## Example

Handlers still running at shutdown get ten seconds to finish before the broker closes.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_broker_shutdown.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_broker_shutdown.rs#app"
```

</TabItem>
</Tabs>
