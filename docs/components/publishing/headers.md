---
sidebar_position: 1
title: Headers
description: "content-type, correlation-id, reply-to and message-id ride the AMQP properties section; every other header rides application-properties."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Headers

`content-type`, `correlation-id`, `reply-to` and `message-id` ride the `AMQP` `properties` section; every other header rides `application-properties`.

The crate adds no envelope of its own, so a peer that does not use RustStream reads a plain `AMQP` message.
On publish each well-known header fills the property of the same name.
The [partition key](./keys.md) fills `group-id`.
Any other header becomes an application property: a UTF-8 value as a string, any other value as binary.

On delivery a handler reads both sections as one header map, through `ctx.headers()`.
`correlation_id()` and `reply_to()` read the two properties request-reply uses.
A message id or a correlation id the sender wrote as a UUID, a number or binary reads as text.
An application property the sender wrote as a number, a boolean or a timestamp reads as text too; a timestamp reads as milliseconds.

A message type declares its headers with `#[outgoing(headers = ..)]`, and a field renamed to `correlation-id` lands in the property.
A `HeaderMap` with the same entries works the same.
The in-process mode frames a publish through the same mapping, so a test reads the headers a live delivery carries.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_publishing_headers.rs#headers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_publishing_headers.rs#headers"
```

</TabItem>
</Tabs>
