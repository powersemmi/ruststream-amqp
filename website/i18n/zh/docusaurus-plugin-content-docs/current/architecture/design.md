---
title: 设计
sidebar_position: 2
description: "ruststream-amqp 的位置：在 RustStream 运行时与 fe2o3-amqp 客户端之间，每个订阅一个会话，所有发布者共用一个会话。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "自上而下依次是服务、RustStream 运行时、本 crate、fe2o3-amqp 客户端和 Broker",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "订阅者处理器"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream 核心",
  "runtime": "运行时：分发、工作分区、结算",
  "crate": "ruststream-amqp",
  "subscriber": ["AmqpSubscriber", "泵任务、通道"],
  "publishers": ["AmqpPublisher、", "AmqpTxnPublisher"],
  "message": "AmqpMessage",
  "request": "请求与应答",
  "broker": ["AmqpBroker", "URL、SASL、容器 id"],
  "descriptor": ["AmqpAddress", "queue、topic、raw"],
  "client": "fe2o3-amqp：一个连接",
  "receiver": ["每个订阅一个", "会话和一条接收链路"],
  "sender": ["一个会话，每个地址", "一条发送链路"],
  "server": "AMQP 1.0 Broker",
  "mounted": "挂载到",
  "polls": "轮询流"
};

# 设计

`ruststream-amqp` 所处的位置：在 RustStream 运行时与 `fe2o3-amqp` 客户端之间，每个订阅一个会话，所有发布者共用一个会话。

<Diagram name="design" words={words} />

本 crate 在 [`fe2o3-amqp`](https://docs.rs/fe2o3-amqp/0.18.2/fe2o3_amqp/) 之上实现核心的 Broker trait。它上面的运行时对每个 Broker 都相同。AMQP 1.0 特有的一切都在本 crate 里：[地址描述符](../components/subscriptions/index.md)、[ack 的含义](../components/settlement/index.md)、[重试副本发往何处](../components/settlement/retry-after.md)，以及[事务如何发布](../components/transactions/index.md)。

已连接的 Broker 持有一个连接。它的[发布者](../components/publishing/index.md)共用一个会话。向某个地址的第一次发布会附加一条发送链路，此后发往该地址的每次发布都复用它。这些链路归连接所有，因此关闭时可以逐条关闭它们，并等待 Broker 的应答。每个订阅开启一个属于自己的会话，并在其上附加一条接收链路。

订阅及其会话都在 Broker 建立连接时所在的运行时上启动，不管由哪个线程打开它们。因此，在独立线程上运行的处理器不会把连接的任何部分留在那个线程上。

本 crate 自己打开 TCP 套接字，并关闭 Nagle 算法。一次发布和一次请求都发送一个小帧，然后等待应答。Nagle 算法恰好会把这种模式延迟到对端确认之后。

消息不带框架自己的封装。常用消息头放在 `properties` 段，分区键作为 `group-id`，其余每个消息头放在 `application-properties` 段。其他 AMQP 客户端发来的消息带着它的消息头到达，本 crate 发出的消息在对方看来就是一条普通的 AMQP 消息。

`asyncapi` 特性用 `x-ruststream-amqp1` 对象描述地址，因为规范预留了 `amqp1` 绑定，但没有定义其内容。

各部分如何启动和停止，见[生命周期](lifecycle.md)；一条消息的路径，见[路径](path.md)。

## 进程内传输 {/* #the-in-process-transport */}

启用 `testing` 特性时，[`TestApp::start`](../components/testing/index.md) 把同一个 Broker 连接到进程内传输，而不是服务器。描述符、策略和投递都是生产环境中的那些，一次发布也经过同样的转换，变成 AMQP 的各个段。它按地址精确路由：同一地址上的 queue 接收者相互竞争，每个 topic 订阅各得一份副本。queue 订阅会创建它的队列，队列保留消息，直到有接收者取走。进程内传输按订阅的模式结算，把 `modified` 投递计入 `delivery-count`，在订阅分离时把未结算的投递放回队列，并通过一个私有的应答地址回答请求。事务在提交之前不发布任何东西。

属于服务器自己的行为留给[真实环境的运行](../components/testing/live.md)去验证：链路信用、服务器如何创建发布最先到达的地址、拒绝之后的死信策略、服务器自己的投递次数上限和持久性。在进程内，发往一个还没有 queue 订阅附加的地址的消息会被丢弃，`raw` 地址只投递一次，而 Artemis 可能把它分发给多个接收者。

不启用该特性时，传输不参与编译。生产构建中已连接的 Broker 就是指向连接的一个共享指针，编译器会检查这个大小。
