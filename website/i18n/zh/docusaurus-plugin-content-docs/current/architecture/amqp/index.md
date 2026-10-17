---
title: 连接
sidebar_position: 0
description: "AMQP 1.0 用一个连接把两个容器连在一起，连接承载会话，每个会话承载单向传送消息的链路。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "服务与 Broker 之间的一个连接：发布者一个会话，每个订阅一个会话，链路位于会话之内",
  "text": "以文字阅读本图",
  "service": ["服务容器", "容器 id ruststream"],
  "connection": "一个 TCP 连接",
  "opens": "TLS、SASL、open",
  "publishers": "会话：publish",
  "publisherWindow": ["会话窗口", "由发布者共用"],
  "sender": ["发送链路", "目标 receipts", "每个地址一条"],
  "subscription": "会话：orders",
  "subscriptionWindow": ["会话窗口", "属于一个订阅"],
  "receiver": ["接收链路", "源 orders", "信用 256"],
  "broker": "Broker 容器",
  "receipts": ["节点 receipts", "队列或主题"],
  "orders": ["节点 orders", "队列或主题"],
  "names": ["链路名称在一个连接内唯一；本 crate 由容器 id", "派生链路名称，例如 ruststream-receiver-3"],
  "transfers": "transfer"
};

# 连接

AMQP 1.0 用一个连接把两个容器连在一起，连接承载会话，每个会话承载单向传送消息的链路。

<Diagram name="connection" words={words} />

容器是协议中的一个对等方：服务是一个容器，Broker 是另一个。服务出示的容器 id 是 `ruststream`，除非 [`container_id`](api:ruststream_amqp::AmqpBroker::container_id) 指定了别的 id。Broker 的控制台在这个 id 下列出服务的连接和链路。

连接就是一个 TCP 套接字。URL 为 `amqps://` 时，连接从 TLS 开始。随后 SASL 用 PLAIN、ANONYMOUS 或 EXTERNAL 对服务进行认证，两个容器交换 `open`。

会话是连接内的一对通道，每个通道都有一个窗口，限制其上正在传输的 transfer 数量。本 crate 开启一个由连接上所有发布者共用的会话，并为每个订阅再开启一个。慢的订阅只会填满自己的窗口，绝不会填满发布者的窗口或其他订阅的窗口。

链路有名称，并在一个方向上传送消息。发送链路从服务通往一个节点，接收链路从一个节点通往服务。链路的每一端都是一个 terminus，Broker 一侧的 terminus 用地址指明一个节点。链路还有自己的信用，[Transfer](transfers.md) 一页用图说明了这一点。

标准规定的是线路上的帧，而不是地址的含义。`orders` 是队列还是主题，由 Broker 决定，[地址](addresses.md)一页用图说明了这一点。

这一模型就是规范自己的模型（[传输](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html)、[安全](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html)）。
