---
title: 路径
sidebar_position: 4
description: "一次 AMQP 1.0 投递在 RustStream 服务中的路径：泵任务、通道、处理器、应答和 disposition。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一次需要应答的投递的路径：从 Broker 的 transfer 到它的 disposition",
  "text": "以文字阅读本图",
  "transfer": ["transfer", "在订阅的接收链路上"],
  "pump": ["泵任务：Receiver::recv", "消息头取自 properties 各段，", "字节取自 data 段"],
  "channel": ["有界通道", "深度等于链路信用"],
  "dispatch": ["分发", "一个循环，或每个键一个工作分区"],
  "decode": ["解码", "直接从投递的字节读取"],
  "body": ["订阅者的处理器", "#[subscriber] 下的 async fn"],
  "encode": ["应答", "编码进循环的缓冲区"],
  "publisher": ["AmqpPublisher", "占用该地址的发送链路，", "直到 Broker 返回 disposition"],
  "settle": ["结算", "作为命令发回泵任务"],
  "disposition": ["disposition", "accepted、modified 或 rejected"]
};

# 路径

一次 AMQP 1.0 投递在 RustStream 服务中的路径：泵任务、通道、处理器、应答和 disposition。

<Diagram name="path" words={words} />

订阅的泵任务拥有它的接收链路。接收需要独占链路，而结算只读取链路，处理器持有的投递又要在任务之间传递。所以由泵任务负责接收，每次结算都作为一条命令回到泵任务。

泵任务把一次 transfer 变成一条 [`AmqpMessage`](api:ruststream_amqp::AmqpMessage)。消息头取自 `properties` 和 `application-properties` 段，字节取自 `data` 段，`delivery-count` 取自 `header` 段。字符串或二进制的值段也按字节读取，其他任何消息体都被拒绝，并报告给订阅。随后泵任务把投递放进一个深度等于链路信用的通道，在等待通道腾出空间时继续处理结算命令。

客户端无法读取的投递，比如超过链路允许大小的投递，会被报告，链路仍保持附加。Broker 超出所给信用发送 transfer 时，链路被破坏，订阅随之结束。

处理器的应答编码进分发循环复用的缓冲区。发布者 [`AmqpPublisher`](api:ruststream_amqp::AmqpPublisher) 把这个缓冲区直接作为消息的 `data` 段，不做复制。它在该地址的发送链路上以未结算方式发送消息，并占用这条链路，直到 Broker 的 disposition 到达。因此，同一连接发往同一地址的发布逐条发出，每条一次往返。`accepted` 以外的 disposition 都是错误；应答发布失败时，这次投递以重试的方式结算。

投递在应答之后结算。它的结果变成一个 disposition，[结算](settlement.md)一页用图说明了这一点。
