---
sidebar_position: 1
title: Server
description: "The server reports the host and port with protocol amqp1 and version 1.0, the container id, and never the credentials in the URL."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

The server reports the host and port with protocol `amqp1` and version `1.0`, the container id, and never the credentials in the URL.

A broker registered with `with_broker_labeled` is a server of the document, under its label.
Its `host` is the host and port of the broker's URL.
The user and the password the URL carries stay out of it.

The `protocol` is `amqp1`, and `protocolVersion` is `1.0`.
The specification's key `amqp` means `AMQP` 0.9.1, and both protocols use the `amqp://` scheme and port 5672.
The key and the version tell a reader, or a tool that generates a client, which protocol the service speaks.

The extension carries `containerId`, the name the service presents on the connection.
It is the broker's [`container_id`](api:ruststream_amqp::AmqpBroker::container_id), `ruststream` when the service sets none.
An operator finds the service's links by that name in the broker's console.

## Example

The broker's URL carries a password, and the server in the document is `broker.example.com:5672` with the container `billing-svc`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_asyncapi.rs#server"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_asyncapi.rs#server"
```

</TabItem>
</Tabs>
