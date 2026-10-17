---
title: Transfer
sidebar_position: 2
description: "发送者只能凭接收者的链路信用 transfer 一条消息，接收者用携带结果的 disposition 结算每一条。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const credit = {
  "title": "链路信用随时间的变化：接收者授予信用，发送者消耗信用，一半结算之后客户端授予新的窗口",
  "text": "以文字阅读本图",
  "receiverLane": "接收者：服务",
  "senderLane": "发送者：Broker",
  "grant": ["flow", "link-credit 256"],
  "send": ["transfer", "最多到信用为止"],
  "settle": ["disposition", "针对 128 次投递"],
  "refill": ["flow", "再次 link-credit 256"],
  "resume": ["transfer", "继续"],
  "flow": "flow",
  "transfer": "transfer",
  "half": "一半已结算",
  "rule": ["信用从上一个 flow 起计算 transfer：", "服务仍持有的投递不会归还信用"]
};

export const outcomes = {
  "title": "disposition 的四种结果，以及每种结果让消息在 Broker 上处于什么状态",
  "text": "以文字阅读本图",
  "outcomeLane": "Disposition",
  "brokerLane": "Broker",
  "accepted": ["accepted"],
  "acceptedEffect": ["完成：消息", "离开队列"],
  "rejected": ["rejected"],
  "rejectedEffect": ["无效：由 Broker 的", "死信策略决定"],
  "released": ["released"],
  "releasedEffect": ["未处理：回到", "队列，计数不变"],
  "modified": ["modified", "delivery-failed"],
  "modifiedEffect": ["失败：回到", "队列，delivery-count + 1"],
  "modes": ["发送者以未结算、已结算或混合（默认）方式发送；", "发送时已结算的 transfer 至多投递一次"]
};

# Transfer

发送者只能凭接收者的链路信用 transfer 一条消息，接收者用携带结果的 disposition 结算每一条。

<Diagram name="transfers" words={credit} />

信用是接收者的流控。接收者发送一个带信用的 `flow` 帧，发送者可以进行这么多次 transfer，然后等待下一个 `flow`。订阅默认授予 256，[`credit`](../../components/subscriptions/credit.md) 可以设置别的数量。

`fe2o3-amqp` 客户端自己管理信用。每当一半信用已经结算，它就按目前已收到的 transfer 重新授予完整的信用。因此服务工作期间发送者不会断供，也绝不会比上一次授予超前一个以上的窗口。

<Diagram name="outcomes" words={outcomes} />

发送者没有结算的 transfer 一直保持打开，直到接收者用 disposition 应答。它的结果是四种之一。`accepted` 完成这条消息。`rejected` 判定它无效，由 Broker 的死信策略决定如何处置。`released` 把它原样退回，下一次投递带着同样的 `delivery-count`。带 `delivery-failed` 的 `modified` 把它作为一次失败的尝试退回，下一次投递的计数加一。`modified` 上的 `undeliverable-here` 要求 Broker 不要再把它投递给这个接收者。

`delivery-count` 位于消息的 `header` 段，统计此前失败的尝试次数。各 Broker 对 `modified` 的处理不同：Artemis 计数，RabbitMQ 4 只在仲裁队列上支持它，Azure Service Bus 不使用它。

流控和结算模式来自规范（[传输，第 2.6.7 节和第 2.8 节](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html)），结果也来自规范（[消息传递，第 3.4 节](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html)）。
