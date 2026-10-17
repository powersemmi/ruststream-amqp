---
slug: /
sidebar_position: 0
description: "使用 RustStream 的 AMQP 1.0 Broker ruststream-amqp 构建 AMQP 1.0 服务的指南。"
---

# 指南

`ruststream-amqp` 是 [RustStream](https://powersemmi.github.io/ruststream/latest/zh/) 的 AMQP 1.0 Broker，
基于 [fe2o3-amqp](https://docs.rs/fe2o3-amqp) 客户端构建。AMQP 1.0 是 ISO 标准，因此一个 crate 即可服务整个协议家族：ActiveMQ Artemis 与 Classic、RabbitMQ 4.x、Azure Service Bus 与 Event Hubs、Amazon MQ、Solace、Apache Qpid 以及 IBM MQ。

在 RabbitMQ 上，AMQP 1.0 与 [`ruststream-lapin`](https://powersemmi.github.io/ruststream-lapin/) 所用的 AMQP 0.9.1 是两套独立的协议栈。
