---
sidebar_position: 1
title: Responders
description: "A responder reads the private reply address from reply-to and answers there through an injected slot, echoing correlation-id."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Responders

A responder reads the private reply address from `reply-to` and answers there through an injected slot, echoing `correlation-id`.

The reply address is new for every request, so neither the reply type nor the mount site can name it.
The handler reads it with `ctx.headers().reply_to()` and publishes with `.to(reply_to)` at the call site.
That call site exists only for a reply type whose `Outgoing` derive names no destination.
The slot is bound as `Out<impl Publisher>` and mounted with `.out(DefaultSlot, Publish).build()`.

The answer copies the request's [`correlation-id`](../publishing/headers.md), which `ctx.headers().correlation_id()` reads.
A requester matches the answer by that id, and an answer without it never resolves the request, which then times out.
A message without `reply-to` is no request; the example drops it, which rejects the delivery.

A request whose payload is plain bytes reads them through a `Deserialized` type and answers with a `Serialized` one, so no codec touches either side.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_request_reply.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_request_reply.rs#responder"
```

</TabItem>
</Tabs>
