---
title: 重试与死信
sidebar_position: 4
description: "立即或稍后重试一次 AMQP 1.0 投递，限定尝试次数，把一直失败的消息转入死信地址，并标记副本。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重试与死信

本指南立即或稍后重试一次投递，限定尝试次数，把一直失败的消息转入死信地址，并标记副本。

AMQP 1.0 没有延迟重新投递，链路也无法声明投递上限或死信地址。
这两件事都由框架来做：挂载点声明上限和死信地址，延迟重试以消息副本的形式发出，消息头里带着重试次数。

示例是一个退款服务，它通过发卡处理商发出退款。

## 订阅者 {/* #the-subscriber */}

处理商有三种拒绝退款的方式，每种需要不同的结果。
`Busy` 表示它暂时不再受理大额退款：立即重试仍会被拒绝，所以 [`retry_after`](../components/settlement/retry-after.md) 等待 30 秒。
`Unavailable` 表示发卡银行没有应答：[`retry`](api:ruststream::runtime::HandlerOutcome::retry) 立即再试一次。
`Declined` 表示卡账户已经关闭，再试一次也是同样的答复，所以这笔退款被丢弃。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

`retry_after` 结算这次投递，并在延迟过去后把一份副本发布到 `refunds`。
副本带着重试次数消息头（[`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)），其值加一。
延迟的副本是至多一次的：在延迟结束前停止的进程会丢失它。
在队列上，副本像其他消息一样交给一个消费者。

## 上限与死信地址 {/* #the-cap-and-the-dead-letter-address */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](../components/settlement/retries.md) 统计一笔退款的每次投递，第一次也算在内。
第五次投递不再重试：[`dead_letter`](../components/settlement/dead-letters.md) 把它按到达时的载荷和消息头原样发布到 `refunds.dead`。
被丢弃的退款会被拒绝，由 Broker 自己的死信策略处理，而不是进入 `refunds.dead`。

在声明了上限时，框架从投递中读取计数。
消息带有 AMQP `header` 段时，计数就是 Broker 的 `delivery-count`。
本 crate 发布的消息不带这个段，所以这时 `retry` 会立即发布一份副本，由重试次数消息头统计尝试次数。

有上限而没有 `dead_letter` 时，最后一次投递会被拒绝，由 Broker 的死信策略决定去向。
Broker 也有自己的上限：Artemis 在 [`max-delivery-attempts`](https://artemis.apache.org/components/artemis/documentation/latest/undelivered-messages.html) 次（默认 10 次）之后停止投递。
上限只应由一方负责：低于 Broker 上限的上限会让框架的计数生效。

## 读取死信 {/* #reading-the-dead-letters */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`refunds.dead` 是一个普通队列，`review` 像读取其他队列一样读取它。
死信的重试次数消息头记录了它经历的投递次数，这里是 5。
`review` 用 `ctx.headers()` 读取这个值，并记录这笔退款，留给人工查看。

## 标记副本 {/* #marking-the-copies */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#stamp"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#stamp"
```

</TabItem>
</Tabs>

副本在处理器中没有调用点，所以只有[发布转换](api:ruststream::runtime::PublishTransform)能触及它。
应用用 `.out_retry(Publish).transform(Retried)` 挂载 `process_refund`。
[`.out_retry(Publish)`](../components/settlement/out-retry.md) 指明副本的发布者，其后的步骤作用于副本。
`Retried` 把投递的来源地址写入 `x-retried-from`。
每份副本都回到其订阅的地址，所以这个位置上的转换不能改变目的地。

## 主题上的重试 {/* #retries-on-a-topic */}

副本发布到订阅自己的地址。
在[主题地址](queues-and-topics.md#a-broadcast)上，这个地址是共用的，所以主题上的每个订阅都会收到副本，而不只是要求重试的那一个。
消息只归一个读者所有时，在队列上重试。

## 测试延迟 {/* #testing-the-delay */}

示例文件中的测试在暂停的时钟下于进程内运行应用，所以 `tb.advance(BACKOFF)` 让 30 秒立即过去。
[测试 AMQP 服务](testing.md)介绍测试套件。
