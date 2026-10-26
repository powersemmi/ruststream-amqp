---
title: AMQP 事务
sidebar_position: 3
description: "AMQP 1.0 事务在 Broker 的协调器上声明，在其下发布的 transfer 要么在提交时一起到达各自的节点，要么都不到达。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "事务性发布：在协调器上声明，带 txn-id 发布，再用 discharge 提交或回滚",
  "text": "以文字阅读本图",
  "declare": ["declare", "在控制器链路上"],
  "declared": ["declared", "协调器的 txn-id"],
  "post": ["发送链路上的 transfer", "带着 txn-id"],
  "commit": ["discharge fail=false", "提交"],
  "visible": ["accepted：", "消息一起到达", "各自的节点"],
  "rollback": ["discharge", "fail=true：", "回滚，", "什么也不", "投递"],
  "hidden": ["discharge 之前，没有消费者能看到", "在事务中发布的消息"],
  "coordinator": "协调器",
  "txnId": "txn-id",
  "accepted": "accepted",
  "orElse": "或"
};

# AMQP 事务

AMQP 1.0 事务在 Broker 的协调器上声明，在其下发布的 transfer 要么在提交时一起到达各自的节点，要么都不到达。

<Diagram name="transactions" words={words} />

协调器是 Broker 的一个节点。控制器链路附加到它并发送 `declare`，协调器用一个事务 id 应答。此后，任何普通发送链路上的 transfer 都在其事务状态中带上这个 id。`discharge` 结束事务：`fail` 为 false 时提交，`fail` 为 true 时回滚。

[`AmqpTxnPublisher`](../../components/transactions/index.md) 在事务中发布消息。每次 `begin_transaction` 和每个自有事务都在连接的发布者会话上附加一条独立的控制器链路并声明一个事务，`commit` 和 `abort` 对它执行 discharge。一个连接上可以同时打开多个事务，Artemis 会把它们分开处理。控制器请求 `mixed` 发送者结算模式，因为 Artemis 以这种模式应答，而客户端要求 Broker 原样回显它所请求的模式。

各 Broker 在这里有所不同。RabbitMQ 4 不支持 AMQP 1.0 事务。Azure Service Bus 支持它们，并通过 send-via 实体在一个事务中向多个实体发布消息。

协议来自规范（[事务](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html)），各 Broker 的限制来自各自厂商的文档（[Artemis](https://artemis.apache.org/components/artemis/documentation/latest/amqp.html)、[RabbitMQ](https://www.rabbitmq.com/docs/amqp)、[Service Bus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-protocol-guide)）。
