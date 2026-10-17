---
title: 地址
sidebar_position: 1
description: "地址指明 Broker 中的一个节点，接收者请求的能力决定它的各个接收者是共用一个队列，还是各得一份副本。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "接收者与地址相遇的三种方式：竞争同一个队列、各有一个订阅队列，以及由 Broker 创建的节点",
  "text": "以文字阅读本图",
  "anycast": "地址 orders：任播",
  "queue": ["队列 orders", "每条消息一次"],
  "worker1": ["接收者 1", "能力 queue"],
  "worker2": ["接收者 2", "能力 queue"],
  "first": "消息 1",
  "second": "消息 2",
  "multicast": "地址 events：组播",
  "copy1": ["接收者 1 的", "订阅队列"],
  "copy2": ["接收者 2 的", "订阅队列"],
  "audit": ["接收者 1", "能力 topic"],
  "billing": ["接收者 2", "能力 topic"],
  "every": "每条消息",
  "dynamic": "动态源",
  "requester": ["接收者", "dynamic = true"],
  "temporary": ["临时队列", "由 Broker 命名"],
  "attach": "attach",
  "named": "地址"
};

# 地址

地址指明 Broker 中的一个节点，接收者请求的能力决定它的各个接收者是共用一个队列，还是各得一份副本。

<Diagram name="addresses" words={words} />

接收者附加时带一个源 terminus：一个节点的地址和一组能力。[`AmqpAddress::queue`](api:ruststream_amqp::AmqpAddress::queue) 请求能力 `queue`，[`AmqpAddress::topic`](api:ruststream_amqp::AmqpAddress::topic) 请求 `topic`，[`AmqpAddress::raw`](api:ruststream_amqp::AmqpAddress::raw) 不请求任何能力。

ActiveMQ Artemis 读取这个能力。请求 `queue` 时，一个地址的各个接收者附加到同一个任播队列并相互竞争：每条消息只交给其中一个。请求 `topic` 时，地址是组播的，Artemis 为每个接收者各建一个订阅队列，因此每个接收者都收到每条消息。除非 Artemis 的设置禁止，它会在第一次附加时创建地址及其队列；`orders::billing` 直接指明一个地址下的某个队列。

topic 订阅保留规范规定的 terminus 默认值：不持久，随会话一同过期。它的订阅队列与链路同生共存，因此服务停止期间发布到该主题的消息不会为它保留。

`raw` 地址原样到达 Broker，由 Broker 自己的配置决定它是什么。其他 Broker 用各自的方式为节点编址：[RabbitMQ 4](https://www.rabbitmq.com/docs/amqp) 把队列写作 `/queues/orders`，把交换器写作 `/exchanges/events/key`，而且队列必须事先存在。[Azure Service Bus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-protocol-guide) 用名称指明队列，把主题订阅写作 `events/subscriptions/billing`。

接收者可以请求一个动态源，而不是指定源。这时由 Broker 创建一个节点（Artemis 创建临时队列），并在对 attach 的应答中返回它的地址。[`request`](api:ruststream_amqp::AmqpPublisher) 为每次请求附加一个这样的接收者，把地址放进 `reply-to` 发出，应答到达或超时之后再分离这个接收者。

terminus 的字段来自规范（[消息传递，第 3.5 节](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html)），地址模型来自 Artemis（[AMQP](https://artemis.apache.org/components/artemis/documentation/latest/amqp.html)、[地址模型](https://artemis.apache.org/components/artemis/documentation/latest/address-model.html)）。
