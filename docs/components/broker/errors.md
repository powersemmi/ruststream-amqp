---
sidebar_position: 6
title: Errors
description: "AmqpError is the one error type of the crate: connect, session, attach, publish, receive, request, transaction and a body with no bytes."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Errors

[`AmqpError`](api:ruststream_amqp::AmqpError) is the one error type of the crate: connect, session, attach, publish, receive, request, transaction and a body with no bytes.

Each variant names the step that failed.
The client's error behind it is its `source`, so the `fe2o3-amqp` types stay out of the crate's API.

| Variant | When |
|---|---|
| `Connect` | the URL, the socket, TLS, the login or the AMQP open failed, or the connection failed to close at shutdown |
| `Session` | the broker refused a session, the publishers' or a subscription's |
| `Attach` | the broker refused a link: a subscription's receiver, a publisher's sender, a request's reply link |
| `Detach` | a link failed to close at shutdown |
| `Publish` | the connection failed while a message was sent |
| `PublishNotAccepted` | the broker settled a published message as rejected, released or modified, so the message is not on the broker |
| `Receive` | the connection failed while a subscription received |
| `UnsupportedBody` | a delivery's body is an AMQP value that is neither binary nor a string; the delivery is rejected, and the runtime logs the error |
| `RequestTimeout` | a [request](../request-reply/index.md) had no reply within its timeout |
| `NotConnected` | a publisher, a subscription or a transaction was used before the connect or after the [shutdown](./shutdown.md) |
| `InvalidAddress` | a subscription's address is empty |
| `Transaction` | a [transaction](../transactions/index.md) step was called in a state that cannot serve it (`transaction` feature) |

A service meets the error in two places.
A failed [startup](./startup.md) carries it as a source of the runtime's error, and the service exits before it reads anything.
A [publish](../publishing/index.md) from a handler returns it as the source of the core's `PublishError`, and the handler decides how the delivery settles.
The enum is `#[non_exhaustive]`, so a `match` on it ends with a wildcard arm.

## Example

The handler sends each shipment to the warehouse address its order names.
A shipment the broker refuses drops the order, since sending it again meets the same answer; any other failure retries it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_broker_errors.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_broker_errors.rs#handler"
```

</TabItem>
</Tabs>
