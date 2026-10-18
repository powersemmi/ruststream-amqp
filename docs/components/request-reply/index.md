---
title: Request-reply
description: "request attaches a dynamic reply link, sends with reply-to and correlation-id, and resolves with the matching reply or RequestTimeout."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Request-reply

[`request`](api:ruststream::RequestReply::request) attaches a dynamic reply link, sends with `reply-to` and `correlation-id`, and resolves with the matching reply or [`RequestTimeout`](api:ruststream_amqp::AmqpError::RequestTimeout).

Request-reply is part of `AMQP` 1.0, and [`AmqpPublisher`](../publishing/index.md) implements the [`RequestReply`](api:ruststream::RequestReply) capability.
A request attaches a receiver link with a dynamic source, and the broker names a private address for that link.
The message goes out with that address in `reply-to` and a fresh id in `correlation-id`.
The call resolves with the first message on the link whose `correlation-id` matches, so a late answer to an earlier request is ignored.
With no match inside the timeout the call returns `RequestTimeout`.
The reply link is detached either way, so each request costs a link attach and a detach on top of the two messages.

A handler asks through a slot it binds as `Out<impl RequestReply>`.
The mount site gives the slot the plain policy: `.out(DefaultSlot, Publish).build()`.
The reply arrives already settled; the handler reads its payload and headers.
A request that opens a conversation has no delivery to answer, so it runs from [`after_startup`](api:ruststream::runtime::BrokerScope::after_startup), where the publisher is already live.
The other side of the exchange is a [responder](./responders.md).

[Request-reply](../../guides/request-reply.md) asks a pricing service for the price of each order.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_request_reply.rs#request"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_request_reply.rs#request"
```

</TabItem>
</Tabs>

The app mounts the responder and the requesting handler, each with its slot, and asks once at startup.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_request_reply.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_request_reply.rs#app"
```

</TabItem>
</Tabs>
