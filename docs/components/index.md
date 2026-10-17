---
sidebar_position: 0
description: "The parts of ruststream-amqp an AMQP 1.0 service uses, one page per component."
---

# Components

The parts of `ruststream-amqp` an AMQP 1.0 service uses, one page per component, grouped by the
part of the service they belong to.
The [broker](./broker/index.md), [`AmqpBroker`](api:ruststream_amqp::AmqpBroker), holds the
connection.
A [subscription](./subscriptions/index.md) names its address with a descriptor, and a handler's
outcome becomes the delivery's [disposition](./settlement/index.md).
A service [publishes](./publishing/index.md) through a policy, asks another service with
[request-reply](./request-reply/index.md), and posts several messages as one
[transaction](./transactions/index.md).
The [AMQP bindings](./asyncapi/index.md) describe the service in its AsyncAPI document, and a
[`TestApp`](./testing/index.md) test runs it on an in-process transport.
