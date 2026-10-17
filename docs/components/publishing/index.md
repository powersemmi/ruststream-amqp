---
title: Publish
description: "AmqpPublish, Publish in the prelude, waits for the broker's disposition and reports anything but accepted as PublishNotAccepted."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Publish

[`AmqpPublish`](api:ruststream_amqp::AmqpPublish), `Publish` in the prelude, waits for the broker's disposition and reports anything but `accepted` as [`PublishNotAccepted`](api:ruststream_amqp::AmqpError::PublishNotAccepted).

The policy has no fields, so a mount site writes it bare.
It is named wherever a registration publishes: `.out_reply(Publish)` for a reply, `.out(Slot, Publish).build()` for a slot, `.out_retry(Publish)` for retry copies.
The policy holds no connection; the runtime pairs it with the connected broker at startup into an [`AmqpPublisher`](api:ruststream_amqp::AmqpPublisher).
It is also the broker's default policy, so a reply registration that names no policy publishes through it.

The publishers share one `AMQP` session.
A sender link is attached to an address on its first publish and kept for the next ones.
Each publish waits for the broker's outcome of the transfer.
An `accepted` outcome completes it; `rejected`, `released` or `modified` fails it with the address and the outcome, so a refusal never passes as sent.

The message carries the encoded value as one `data` section.
[Headers](./headers.md) ride the message's `properties` and `application-properties` sections, and the [partition key](./keys.md) rides `group-id`.
A publish carries no per-message settings: the message goes out without the `header` section, so it is not durable and keeps the default priority and no time to live.

Where a reply goes is a property of its type: `#[outgoing(name = "receipts")]` fixes the address.
A reply to an address the request names is the [request-reply](../request-reply/index.md) pattern.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_publishing_publish.rs#publish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_publishing_publish.rs#publish"
```

</TabItem>
</Tabs>
