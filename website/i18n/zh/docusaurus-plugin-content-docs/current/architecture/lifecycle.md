---
title: 生命周期
sidebar_position: 3
description: "AmqpBroker::new 记录配置，connect 打开连接和发布者的会话，启动时附加每个订阅，关闭由内向外进行。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "以类型表示的 Broker 状态，以及每次转换在线路上做了什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "wire": "线路上",
  "unconnected": ["AmqpBroker::new", "(url)"],
  "connected": ["ConnectedAmqpBroker"],
  "closed": ["已关闭：", "Ok(()) 或", "最内层的错误"],
  "config": ["记录 URL、", "SASL、容器 id；", "不做 I/O"],
  "open": ["套接字、TLS、SASL、", "open，以及", "发布者的会话"],
  "subscriptions": ["每个订阅：", "一个会话、一个接收者", "及其泵任务"],
  "senders": ["向某个地址的", "第一次发布附加", "它的发送链路"],
  "teardown": ["泵任务结束各自的", "会话，发送链路", "关闭，会话", "结束，最后是", "连接"],
  "afterShutdown": ["在 connect 之前或 shutdown 之后保留的发布者", "返回 AmqpError::NotConnected，绝不会静默成功"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# 生命周期

[`AmqpBroker::new`](api:ruststream_amqp::AmqpBroker::new) 记录配置，connect 打开连接和发布者的会话，启动时附加每个订阅，关闭由内向外进行。

<Diagram name="lifecycle" words={words} />

`new` 不做 I/O，因此服务的构建器保持同步。应用启动时，运行时连接 Broker。connect 解析 URL，打开套接字，完成认证，打开连接，并开启发布者共用的会话。URL 无法解析、套接字被拒绝或登录被拒绝时，启动返回 `AmqpError::Connect` 错误。

每个订阅的描述符针对已连接的 Broker 解析一次。空地址在任何 I/O 之前就被拒绝。订阅开启一个会话，带着自己的信用附加接收者，并启动拥有该接收者的泵任务。Broker 拒绝附加时，启动返回 `AmqpError::Attach` 错误，其中写明地址。

发送链路由向其地址的第一次发布附加，而不是在启动时附加。只消费的服务不附加任何发送链路。

关闭时，连接先被标记为已关闭，然后由内向外拆除：每个泵任务分离自己的接收者并结束自己的会话，发送链路关闭，发布者的会话结束，连接最后关闭。每一层仍为它内部的那一层转发 Broker 的应答。即使前面某一步失败，每一步也都会执行，报告的是最内层的错误。关闭时仍打开的订阅交出它持有的投递后结束；此后再结算其中任何一条都会返回错误。

在连接之前克隆的 Broker 与克隆体共用连接。测试套件在进程内连接的克隆体之后无法再连接到服务器，反过来也会被拒绝。
