---
slug: /
sidebar_position: 0
description: "Guides to building AMQP 1.0 services with ruststream-amqp, the AMQP 1.0 broker for RustStream."
---

# Guides

`ruststream-amqp` is the AMQP 1.0 broker for [RustStream](https://powersemmi.github.io/ruststream/latest/),
built on the [fe2o3-amqp](https://docs.rs/fe2o3-amqp) client. AMQP 1.0 is an ISO standard, so one
crate serves the whole family: ActiveMQ Artemis and Classic, RabbitMQ 4.x, Azure Service Bus and
Event Hubs, Amazon MQ, Solace, Apache Qpid and IBM MQ.

On RabbitMQ, AMQP 1.0 is a separate protocol stack from the 0.9.1 that
[`ruststream-lapin`](https://powersemmi.github.io/ruststream-lapin/) speaks.
