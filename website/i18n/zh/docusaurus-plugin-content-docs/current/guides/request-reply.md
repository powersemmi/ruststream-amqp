---
title: 请求-响应
sidebar_position: 5
description: "通过 AMQP 1.0 询问另一个服务并等待它的答复，在请求的回复地址上作答，并处理无人应答的请求。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 请求-响应

本指南通过 AMQP 1.0 询问另一个服务并等待它的答复，在请求的回复地址上作答，并处理无人应答的请求。

请求-响应是协议的一部分。
请求在 `reply-to` 中带着回复地址，在 `correlation-id` 中带着一个 id。
答复发往这个地址，并重复这个 id。

示例是一个应用中的两个服务。
定价服务回答关于价格的询问，订单服务向它询问每个订单的价格。

## 应答方 {/* #the-responder */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#responder"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#responder"
```

</TabItem>
</Tabs>

请求到达时，`reply-to` 消息头中带着它的回复地址，`ctx.headers().reply_to()` 读取这个地址。
`quote` 通过它的 `Answers` 槽位用 `to(reply_to)` 发布价格，因为每个请求的地址都不同。
因此 `Price` 不声明自己的地址。
答复带着请求的 `correlation-id`，请求方只接收 id 相符的答复。
没有价格的商品得不到答复，请求方的超时结束等待。

应答方读取队列 `prices.quote`。
定价服务的每个副本都是它的又一个消费者，每个请求只交给其中一个。

## 请求方 {/* #the-requester */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#requester"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#requester"
```

</TabItem>
</Tabs>

`price_order` 读取订单，并在 `prices.quote` 上询问每个订单的价格。
`Pricing` 槽位的约束是 `Out<impl RequestReply, _>`：函数体声明它需要请求-响应，而不指明 Broker。
应用把这个槽位与普通的 `Publish` 策略配对，该策略的发布者 [`AmqpPublisher`](api:ruststream_amqp::AmqpPublisher) 支持请求-响应。
[`request`](api:ruststream::RequestReply::request) 附加一条动态接收链路，于是 Broker 为这一个请求创建一个私有的回复地址。
它发布问题，设置好 `reply-to` 和 `correlation-id`，并以第一个相符的答复完成。
ActiveMQ Artemis 用一个临时队列支撑动态链路。
问题是一条原始消息，所以函数体自己编码问题并解码答复，这里用的是 `serde_json`。
价格得知后，订单的回复 `Priced` 随即发出。

无人应答的请求在超时（这里是两秒）后返回 [`AmqpError::RequestTimeout`](api:ruststream_amqp::AmqpError::RequestTimeout) 错误。
无论哪种情况，回复链路都会被分离。
订单随后在队列中等待 30 秒，再询问一次。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#app"
```

</TabItem>
</Tabs>

两个槽位都使用普通的 `Publish` 策略：一个发布答复，另一个提出问题。
发起对话的请求没有要回复的投递。
它从启动钩子 `b.after_startup(Publish, ..)` 中运行，在交给钩子的发布者上调用 `request`。

示例文件中的测试在一个进程内应用中运行这两半。
进程内 Broker 把请求路由到应用自己的应答方，按关联 id 匹配答复，并让沉默的应答方超时。
