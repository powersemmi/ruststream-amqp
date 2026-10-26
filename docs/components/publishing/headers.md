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
A property takes a value only in a form it holds, so a header that rides one travels unchanged or fails the publish.
`reply-to` and the partition key are strings and take UTF-8.
`content-type` is a symbol and takes ASCII.
`message-id` and `correlation-id` go as a string when the value is UTF-8 and as binary otherwise.
A value outside its property's form fails the publish with `AmqpError::UnsupportedHeader`, which names the header and the address.

On delivery a handler reads both sections as one header map, through `ctx.headers()`.
`correlation_id()` and `reply_to()` read the two properties [request-reply](../request-reply/index.md) uses; a [responder](../request-reply/responders.md) reads the correlation in its own type with `Ctx<ReplyCorrelation>`.
A message id or a correlation id the sender wrote as a string or binary reads as its bytes; one written as a UUID reads as its hyphenated hex, and a number as its decimal digits.
An application property reads in a form that keeps its value, though not its AMQP type:

| Sender wrote | Header value |
|---|---|
| a string or a symbol | its UTF-8 bytes |
| binary | its bytes |
| a boolean, a number or a char | its text, which parses back to the same value |
| a timestamp | milliseconds since the epoch, as text |
| a UUID | its hyphenated lowercase hex |
| null | an empty value |
| a decimal | its IEEE 754 bytes |
| a described value | its AMQP encoding |

A message type declares its headers with `#[outgoing(headers = ..)]`, and a field renamed to `correlation-id` lands in the property.
A `HeaderMap` with the same entries works the same.
The [in-process mode](../testing/index.md) frames a publish through the same mapping, so a test reads the headers a live delivery carries.

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
