---
sidebar_position: 1
title: Responders
description: "A responder reads the private reply address from reply-to and answers there through an injected slot, with the request's correlation in its own AMQP type."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Responders

A responder reads the private reply address from `reply-to` and answers there through an injected slot, with the request's correlation in its own AMQP type.

The reply address is new for every request, so neither the reply type nor the mount site can name it.
The handler reads it with `ctx.headers().reply_to()` and publishes with `.to(reply_to)` at the call site.
That call site exists only for a reply type whose `Outgoing` derive names no destination.
The slot is bound as `Out<impl Publisher<Options = AmqpPublishOptions>>` and mounted with `.out(DefaultSlot, Publish).build()`.

The answer carries the request's correlation with `.correlation_id(correlation)`, read by `Ctx(correlation): Ctx<ReplyCorrelation>`.
It is the request's `correlation-id`, or its `message-id` when it has none, in the type the requester wrote it: a string, binary, a UUID or a number.
A Java or .NET requester often writes a UUID or a number and matches by that typed value, so the answer keeps the type rather than its text in the header map.
A requester matches the answer by that id, and an answer without it never resolves the request, which then times out.

A handler that answers through the reply position gets the same correlation from the `Correlate` transform: `.out_reply(Publish).transform(Correlate)`, with a `Ctx<ReplyCorrelation>` argument on the handler.
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
