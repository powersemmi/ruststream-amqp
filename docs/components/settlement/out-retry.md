---
sidebar_position: 5
title: out_retry
description: "out_retry(Publish) replaces the registration's retry publisher, and the steps after it, such as a transform, apply to every deferred copy."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# out_retry

[`out_retry(Publish)`](api:ruststream::runtime::Mounting::out_retry) replaces the registration's retry publisher, and the steps after it, such as a transform, apply to every deferred copy.

The copies of a registration leave through its [`Retry`](api:ruststream::runtime::Retry) slot.
Every registration has a publisher there already, so a [`retry_after`](./retry-after.md) works with no `out_retry` at the mount site.
`out_retry` names the publisher explicitly, and on this broker it is [`Publish`](api:ruststream_amqp::AmqpPublish), the crate's publish policy.
The steps after it are the slot's steps.
A [`PublishTransform`](api:ruststream::runtime::PublishTransform) over `ForReply<C>` reads the delivery being retried and edits each copy, here to add a header.
It applies to every copy the registration publishes: the deferred copy, the copy an immediate `retry` becomes under a [cap](./retries.md), and the [dead-letter](./dead-letters.md) copy.
The transform declares `Destination = Reads`: the copy goes where the runtime sends it, and a transform that declares `Names` does not compile on this position.
The copy travels as the bytes that arrived, so no codec applies to it.

## Example

The transform stamps every copy with the subscription it came from:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_retry.rs#transform"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_retry.rs#transform"
```

</TabItem>
</Tabs>

The mount site names the publisher and the transform after it:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_retry.rs#declaration"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_retry.rs#declaration"
```

</TabItem>
</Tabs>
