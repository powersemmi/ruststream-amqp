---
sidebar_position: 3
title: Startup
description: "Startup opens the socket, authenticates and begins the publishers' session; a refusal fails the start as AmqpError::Connect."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Startup

Startup opens the socket, authenticates and begins the publishers' session; a refusal fails the start as [`AmqpError::Connect`](api:ruststream_amqp::AmqpError::Connect).

The runtime connects the broker once, before any handler runs.
The connect opens a TCP socket to the URL's host with Nagle's algorithm off: AMQP sends a small frame and waits for the answer, which that algorithm would hold back.
Over the socket it runs TLS for an `amqps://` URL, the SASL [login](./security.md), and the AMQP open with the service's [container id](./container-id.md).
It then begins one session that every publisher of the service shares.
A failure of the socket, TLS, the login or the open fails startup with `AmqpError::Connect`, and a session the broker refuses with [`AmqpError::Session`](api:ruststream_amqp::AmqpError::Session).
The service exits before it reads anything.

Each subscription then begins a session of its own and attaches a receiver link to its address.
AMQP counts flow control per session, so a slow subscription holds back neither the publishers nor another subscription.
An attach the broker refuses fails startup with [`AmqpError::Attach`](api:ruststream_amqp::AmqpError::Attach), which names the address.
An empty address fails with [`AmqpError::InvalidAddress`](api:ruststream_amqp::AmqpError::InvalidAddress) before any I/O.
A publisher attaches its sender link to an address on the first publish there, and every later publish to the address reuses that link.

A [`TestApp`](../testing/index.md) test connects in process and reads the URL as the connect reads it.
A scheme other than `amqp` or `amqps`, a URL without a host, or `amqps` without a TLS feature fails startup in a test as in production.
A refused socket or login happens only against a broker, in a [live test](../testing/live.md).

## Example

The deployment names the broker in `AMQP_URL`, credentials included.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_broker_startup.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_broker_startup.rs#app"
```

</TabItem>
</Tabs>
