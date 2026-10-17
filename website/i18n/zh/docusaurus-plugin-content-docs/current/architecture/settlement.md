---
title: 结算
sidebar_position: 6
description: "ack、drop、retry 和 retry_after 在 AMQP 1.0 上变成什么：Broker 据以行动的 disposition，以及运行时发布到同一地址的副本。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const table = {
  "title": "至少一次的订阅上处理器的每种结果：订阅发送的 disposition，以及 Broker 做什么",
  "text": "以文字阅读本图",
  "outcomeLane": "处理器",
  "wireLane": "Disposition",
  "brokerLane": "Broker",
  "ack": ["ack()"],
  "ackWire": ["accepted"],
  "ackEffect": ["完成：", "消息离开", "队列"],
  "retry": ["retry()"],
  "retryWire": ["modified", "delivery-failed"],
  "retryEffect": ["回到队列，", "计数", "加一"],
  "retryAfter": ["retry_after(d)"],
  "retryAfterWire": ["rejected，", "d 之后发副本"],
  "retryAfterEffect": ["副本是该地址上的", "一条新消息"],
  "drop": ["drop()"],
  "dropWire": ["rejected"],
  "dropEffect": ["由 Broker 的", "死信策略", "决定"],
  "unsettled": ["链路断开前", "没有", "结果"],
  "unsettledWire": ["什么也不发"],
  "unsettledEffect": ["链路分离时", "回到", "队列"],
  "atMostOnce": ["至多一次时，投递到达时已结算：ack、retry", "和 drop 返回 AckError::Unsupported"]
};

export const retries = {
  "title": "AMQP 1.0 上的一次重试：尝试上限决定把消息送回、发往死信地址，还是拒绝",
  "text": "以文字阅读本图",
  "delivery": ["一次投递：它的尝试次数是 delivery-count + 1，", "副本上则是 x-ruststream-retry-count"],
  "outcome": ["处理器返回", "retry() 或 retry_after(delay)"],
  "cap": ["max_attempts：这次尝试是否低于上限？"],
  "requeue": ["retry()：", "modified，", "回到", "队列"],
  "copy": ["retry_after：", "rejected，", "延迟之后", "发副本"],
  "deadLetter": ["到达上限：", "副本发往", "死信", "地址"],
  "rejected": ["到达上限，", "没有死信", "地址：", "rejected"],
  "returns": ["两者都回到", "订阅自己的地址"],
  "crash": ["副本在进程内等待延迟结束：", "计时器触发前停止就会丢失它"],
  "handled": "已处理",
  "retries": "重试",
  "below": "低于上限",
  "atCap": "到达上限"
};

# 结算

`ack`、`drop`、`retry` 和 `retry_after` 在 AMQP 1.0 上变成什么：Broker 据以行动的 disposition，以及运行时发布到同一地址的副本。

<Diagram name="settlement" words={table} />

订阅默认是至少一次。投递在处理器的结果作为 disposition 到达泵任务之前一直处于未结算状态。始终没有得到 disposition 的投递留在链路上，链路分离或连接断开时，Broker 把它放回队列。

`retry()` 发送带 `delivery-failed` 的 `modified`，从不发送 `released`。release 告诉 Broker 这次投递没有被处理，`delivery-count` 保持不变。要求重试的处理器确实处理了它，只是失败了，而尝试上限读取的正是这个计数：计数不增加，一条总是失败的消息就会永远回来。`undeliverable-here` 保持为 false，因此消息可以再次回到同一个订阅。

[`Settle::AtMostOnce`](api:ruststream_amqp::Settle::AtMostOnce) 在每次投递到达时就结算它。

| 结果 | 至多一次 |
|---|---|
| `ack`、`drop` | `AckError::Unsupported`，并记录日志：投递在到达时已结算 |
| `retry()` | `AckError::Unsupported`；在声明了上限时，若投递不带 `delivery-count`，立即发布一份副本 |
| `retry_after(d)` | 在 `d` 之后把一份副本发布到该地址 |
| 没有结果 | 什么也不做：Broker 已经放走了这条消息 |

一条消息被投递了多少次，等于 `delivery-count` + 1，由 [`redelivery_count`](api:ruststream_amqp::AmqpMessage) 报告。本 crate 发布的消息没有 `header` 段，因此不带计数，此时由框架的重试计数消息头来计数。

## 重试 {/* #retries */}

<Diagram name="retries" words={retries} />

上限在挂载点用 [`max_attempts`](api:ruststream::runtime::Mounting::max_attempts) 声明，第一次投递算作一次。低于上限时，`retry()` 以 `modified` 回到 Broker，由 Broker 计数。副本不带 Broker 的计数，因此在有上限时，对副本的 `retry()` 会立即发布下一份副本。

AMQP 1.0 不会扣留消息，所以 `retry_after` 由运行时自己完成。它拒绝这次投递，等待延迟结束，然后发布一份副本，副本的重试计数消息头加一。副本发往订阅自己的地址，因为接收者附加源、发送者附加目标的都是同一个节点。在 `queue` 地址上，副本像其他消息一样竞争接收者；在 `topic` 地址上，每个订阅都会收到它。

到达上限时，投递按到达时的原样发布到[死信地址](api:ruststream::runtime::Mounting::dead_letter)。没有声明死信地址时，投递被拒绝，由 Broker 自己的死信策略决定。声明了死信地址但没有上限时，每次重试都发往死信地址。
