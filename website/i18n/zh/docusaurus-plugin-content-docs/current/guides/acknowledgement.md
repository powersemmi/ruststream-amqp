---
title: 确认与投递保证
sidebar_position: 3
description: "用 accepted、modified 和 rejected 结算 AMQP 1.0 投递，限定尝试次数，按至多一次读取，设定链路信用，并处理批。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 确认与投递保证

本指南结算投递，限定尝试次数，按至多一次读取队列，设定链路信用，并处理批。

在 AMQP 1.0 中，接收者用一个结果结算每次投递，Broker 据此行动。
处理器的结果就是这个 disposition。

示例是一个仓库。
它拣选货件，记录每次拣选，并用成批到达的投递补充货架。

## 三种结果 {/* #the-three-outcomes */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#pick"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#pick"
```

</TabItem>
</Tabs>

| 处理器的结果 | Disposition | Broker 做什么 |
|---|---|---|
| `HandlerOutcome::ack()` | `accepted` | 删除消息 |
| `HandlerOutcome::retry()` | 带 `delivery-failed` 的 `modified` | 在 `delivery-count` 中计入这次尝试，并再次投递消息 |
| `HandlerOutcome::drop()` | `rejected` | 执行自己的死信策略 |

`pick` 接受已拣选的货件。
空货件永远无法拣选，所以 `pick` 拒绝它。
Artemis 把被拒绝的消息转入死信地址，默认是 `DLQ`。
缺货的货件以后也许能拣选，所以 `pick` 要求再次投递它。
Artemis 在 [`max-delivery-attempts`](https://artemis.apache.org/components/artemis/documentation/latest/undelivered-messages.html) 次（默认 10 次）之后停止投递一条消息，并把它转入死信地址。

## 限定尝试次数 {/* #capping-the-attempts */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) 把一个货件的投递次数限定为五次，第一次也算在内。
第五次投递被拒绝，而不是再要求一次。
消息带有 AMQP `header` 段时，计数就是 Broker 的 `delivery-count`。
本 crate 发布的消息不带这个段。
对这样的消息，框架自己统计尝试次数：`retry` 把一份副本发布回 `shipments`，消息头里带着重试次数。
[重试与死信](retries.md)在此基础上加入死信地址和延迟。

## 中途崩溃 {/* #a-crash-in-the-middle */}

订阅默认是至少一次。
服务停止时尚未结算的投递会回到队列，由下一个消费者取得。
因此处理器可能两次看到同一条消息，它的工作必须能容忍这一点。

## 至多一次 {/* #at-most-once */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#audit"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#audit"
```

</TabItem>
</Tabs>

[`Settle::AtMostOnce`](api:ruststream_amqp::Settle::AtMostOnce) 在每次投递到达时就结算它。
Broker 交出消息的那一刻就忘掉了它，所以处理器完成前的崩溃会丢失这条消息。
这适合拣选日志：缺一行的代价比让 Broker 再处理一遍要小。
处理器的结果已经没有东西可结算：`ack` 和 `nack` 返回 `AckError::Unsupported`，运行时为每一次记录一条警告。

## 信用 {/* #credit */}

`shipments` 上的 `.credit(nonzero!(32))` 是链路信用：Broker 在服务结算之前最多可以预先发送多少次投递。
默认值是 [`DEFAULT_CREDIT`](api:ruststream_amqp::DEFAULT_CREDIT)，即 256。
一半信用结算之后，客户端重新授予全部信用。
因此慢速处理器会让 Broker 放慢发送，而不是占满服务的内存，队列的其余部分则留在 Broker 上，供其他副本读取。
信用是 `NonZeroU32`，因为没有信用的链路收不到任何东西：`credit(nonzero!(0))` 无法编译。

## 批 {/* #batches */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#restock"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#restock"
```

</TabItem>
</Tabs>

接收切片的处理器得到一批消息，挂载点上的 `.batch(nonzero!(20usize))` 设定批的最大大小。
AMQP 1.0 一次 transfer 一条消息，所以批在客户端上组装。
一批最多包含 20 次投递；如果在时限内没有更多投递到达，就少一些。
[`batch_wait`](api:ruststream_amqp::AmqpAddress::batch_wait) 是未满的批等待更多投递的时长，这里是 50 毫秒，默认是 10 毫秒。
`restock` 的结果结算这批中的每一次投递。

## 测试各种结果 {/* #testing-the-outcomes */}

示例文件中的测试在进程内运行这个仓库。
它们检查每种结果、缺货货件的五次投递、拣选日志以及批。
信用、Broker 的死信策略及其投递上限属于 Artemis，所以进程内 Broker 不模拟它们。
[测试 AMQP 服务](testing.md)介绍测试套件。
