---
title: 工作分区
sidebar_position: 5
description: "一个订阅通过一条接收链路和一个泵任务读取它的队列；workers(n, by_key) 按 group-id 把投递分到各个工作分区上。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一个订阅的分发：队列、接收链路、泵任务、一个流，以及按 group-id 划分的两个工作分区",
  "text": "以文字阅读本图",
  "queue": ["Broker 上的", "队列 orders"],
  "link": ["接收链路", "信用 256：Broker 最多提前发送多少"],
  "pump": ["泵任务", "投递等待空间时继续结算"],
  "stream": ["AmqpSubscriber 流", "深度等于信用的通道"],
  "keyed": ["workers(2, by_key)", "工作分区 = group-id 的哈希"],
  "lane0": ["工作分区 1", "组 a 和 c，", "各自有序"],
  "lane1": ["工作分区 2", "组 b，有序"],
  "replicas": ["更多实例竞争同一个队列，Artemis 的", "消息组把同一个 group-id 留在同一个接收者上"],
  "transfers": "transfer",
  "received": "已接收",
  "bounded": "交出",
  "oneAtATime": "一次一条"
};

# 工作分区

一个订阅通过一条接收链路和一个泵任务读取它的队列；`workers(n, by_key)` 按 `group-id` 把投递分到各个工作分区上。

<Diagram name="lanes" words={words} />

Broker 按链路的[信用](../components/subscriptions/credit.md)发送，默认 256 次 transfer，服务结算后客户端再授予更多信用。泵任务把每次投递交给一个深度等于信用的通道，因此服务中等待处理器的投递不会超过一份信用的量。投递等待通道腾出空间时，泵任务继续执行结算，所以即使没有人轮询流，已经交出的投递的确认也不会被卡住。

流一次产出一次投递。不用 `workers` 时，一个循环依次为每次投递运行处理器，队列的顺序得以保持。

`workers(n, by_key)` 运行 `n` 个工作分区。投递进入其[分区键](../components/publishing/keys.md)的哈希所对应的工作分区，每个工作分区按顺序运行自己的投递。分区键放在消息的 `group-id` 属性中，因此同一组的消息保持顺序，不同的组并行运行。没有分区键的投递也就没有工作分区的键，这样的投递轮流分配到各个工作分区。

要超出一个进程扩展服务，就在同一个 [`queue`](../components/subscriptions/queue.md) 地址上运行更多实例：它们的接收者相互竞争，每条消息只交给其中一个。Artemis 的消息组把同一个 `group-id` 的每条消息都发给同一个接收者，因此一个组跨实例也保持顺序。在 Azure Service Bus 上，`group-id` 就是会话 id。

[批处理器](../components/subscriptions/batches.md)接收客户端从流中组装的一个批：最多到挂载点指定的大小，`batch_wait`（默认 10 毫秒）一过就提前结束。信用与批的大小相互独立：信用限制 Broker 提前发送多少，批的大小是一次处理器调用看到多少条消息。
