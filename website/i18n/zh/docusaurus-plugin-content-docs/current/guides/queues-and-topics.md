---
title: 队列与主题
sidebar_position: 2
description: "通过 AMQP 1.0 读取工作队列、在主题上广播、为暂时离开的读者保留广播，以及向地址发布消息。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 队列与主题

本指南读取一个工作队列，在主题上广播，为暂时离开的读者保留广播，并向一个地址发布消息。

AMQP 1.0 规定了线上格式，地址的含义则交给 Broker 决定。
因此订阅要说明自己想要什么：[`AmqpAddress`](../components/subscriptions/index.md) 为每种意图提供一个构造函数，链路把这个意图告诉 Broker。
本页所述内容都在 ActiveMQ Artemis 上验证过，本 crate 的测试也运行在它上面。

示例是一个订单服务。
它从队列中取出订单，并在一个主题上公布每个已接收的订单；账务、审计日志和总账都关注这个主题。

## 工作队列 {/* #a-work-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/queues_and_topics.rs#queue"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/queues_and_topics.rs#queue"
```

</TabItem>
</Tabs>

[`AmqpAddress::queue`](../components/subscriptions/queue.md) 请求一个队列：链路带上 terminus 能力 `queue`。
Artemis 随后创建一个任播地址 `orders`，其后有一个队列。
一个队列的各个消费者相互竞争，每个订单只交给其中一个。
要更快地处理订单，就多运行几个服务副本：每个副本都是 `orders` 的又一个消费者。

`take_order` 用一个 `OrderTaken` 回复每个订单。
`OrderTaken` 用 `#[outgoing(name = "order-events")]` 指明自己的地址，所以不带参数的 `reply` 子句把它发往那里。

## 广播 {/* #a-broadcast */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/queues_and_topics.rs#topic"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/queues_and_topics.rs#topic"
```

</TabItem>
</Tabs>

[`AmqpAddress::topic`](../components/subscriptions/topic.md) 请求一个主题：链路带上能力 `topic`。
Artemis 把 `order-events` 设为组播地址，并为每个订阅各建一个队列。
因此 `bill` 和 `audit` 各自收到每个 `OrderTaken` 的一份副本。

主题订阅与它的链路同生共存。
服务停止期间发布到 `order-events` 的消息不会为它保留。
不能错过任何消息的读者应读取队列。

## 等待读者的广播 {/* #a-broadcast-that-waits-for-its-reader */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/queues_and_topics.rs#durable"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/queues_and_topics.rs#durable"
```

</TabItem>
</Tabs>

总账必须看到每个订单，包括它停止期间接收的订单。
运维人员在组播地址 `order-events` 上创建一个持久队列 `ledger`：

```bash
docker compose exec artemis /opt/activemq-artemis/bin/artemis queue create \
  --user artemis --password artemis --silent \
  --name ledger --address order-events --auto-create-address \
  --multicast --durable --preserve-on-no-consumers
```

此后发布到 `order-events` 的每条消息也会进入 `ledger`，无论 `post_to_ledger` 是否在运行。
`order-events::ledger` 是该队列在 Artemis 中的完全限定名：地址、两个冒号、队列名。
[`AmqpAddress::raw`](../components/subscriptions/raw.md) 按原样发送地址，不带任何能力，因此链路附加到这个队列上。
直接写 `#[subscriber("order-events::ledger")]` 效果相同。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/queues_and_topics.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/queues_and_topics.rs#app"
```

</TabItem>
</Tabs>

`.out_reply(Publish)` 指明回复的发布者，即 [prelude](../components/broker/prelude.md) 中名为 `Publish` 的 [`AmqpPublish`](../components/publishing/index.md)。
发布在第一次使用时向回复的地址附加一条发送链路，并一直保留它。
Broker 拒收的消息会让发布返回 [`AmqpError::PublishNotAccepted`](../components/broker/errors.md) 错误。

## 消息头与其他客户端 {/* #headers-and-other-clients */}

常见的[消息头](../components/publishing/headers.md)放在 AMQP 的 `properties` 段中：`content-type`、`correlation-id`、`reply-to` 和 `message-id`。
其他所有消息头放在 `application-properties` 中。
本 crate 发出的消息是普通的 AMQP 消息，因此其他语言的客户端不借助 RustStream 也能读取它。

分区键以 [`PARTITION_KEY_HEADER`](../components/publishing/keys.md) 消息头传递，并成为消息的 `group-id`。
Artemis 用 `group-id` 实现[消息分组](https://artemis.apache.org/components/artemis/documentation/latest/message-grouping.html)：同一组的每条消息都交给队列的同一个消费者。
因此以客户为键的订单在多个副本之间也保持顺序。

## 其他 Broker 上的地址 {/* #addresses-on-other-brokers */}

其他 Broker 用各自的方式为节点命名，`raw` 把它们的写法原样传过去。
这些写法见各自的文档：
[RabbitMQ 4.x](https://www.rabbitmq.com/docs/amqp)、
[Azure Service Bus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-protocol-guide)、
[ActiveMQ Classic](https://activemq.apache.org/components/classic/documentation/amqp)（[Amazon MQ](https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/working-with-activemq.html) 运行的就是它）。

## 测试路由 {/* #testing-the-routing */}

示例文件中的测试在进程内运行应用。
一个订单发布一个 `OrderTaken`，`orders` 被调用一次，`order-events` 被调用两次，每个主题订阅各一次。
进程内 Broker 按原样投递 `raw` 地址，所以它的测试直接发布到 `order-events::ledger`。
[测试 AMQP 服务](testing.md)介绍测试套件。
