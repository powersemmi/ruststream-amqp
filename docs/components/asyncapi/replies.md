---
sidebar_position: 4
title: Replies
description: "A reply reports $message.header#/reply-to as where its address is read, so a transform can answer each request where it asked."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Replies

A reply reports `$message.header#/reply-to` as where its address is read, so a transform can answer each request where it asked.

A [request](../request-reply/index.md) on this broker sets the `AMQP` `reply-to` property, and a handler reads it as the `reply-to` header.
A reply can follow it: a transform on `out_reply` that declares `Destination = Names` sets the reply's address from that header.
The reply then has no fixed address, and the receive operation's reply says where a client reads it: `$message.header#/reply-to`.
Both publish policies report that location.

The mount site still names a reply destination, such as `reply("receipts")`.
It is the reply's declared channel, and a delivery the transform leaves alone, one without `reply-to`, is answered there.

## Example

The reply of `receive_orders` reports its address location as `$message.header#/reply-to`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_asyncapi.rs#replies"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_asyncapi.rs#replies"
```

</TabItem>
</Tabs>
