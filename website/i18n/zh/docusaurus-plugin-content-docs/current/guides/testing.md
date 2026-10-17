---
title: 测试 AMQP 服务
sidebar_position: 7
description: "用 TestApp 在进程内测试 AMQP 1.0 服务：生产环境的应用、回复、延迟、死信，以及在 Broker 上运行的同一个测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 测试 AMQP 服务

测试用 [`TestApp`](api:ruststream::testing::TestApp) 运行服务自己的应用，连接的是进程内 Broker 或者正在运行的 Broker。

被测服务负责银行卡扣款。
付款到达队列 `card-payments`，处理器在 `card-receipts` 上以一张收据作为回复。
没有银行卡的付款被丢弃。
超出银行即时限额的付款会被暂缓：处理器用 `retry_after` 要求一分钟后再投递它。
经过三次投递后，付款进入死信地址 `card-payments.review`。
应用由 `service(broker)` 构建，所以测试可以选择 Broker 的 URL，而 `main` 使用生产环境的 URL。
测试需要在 `[dev-dependencies]` 中启用 `ruststream-amqp` 的 `testing` feature。

## 付款及其收据 {/* #a-payment-and-its-receipt */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](../components/testing/index.md) 把 [`AmqpBroker`](../components/broker/index.md) 连接到进程内 Broker。
测试从不连接 Broker 的 URL。
测试按生产类型访问 Broker：`tb.broker::<AmqpBroker>()`。
`publish` 在处理器及其回复都完成结算后才返回。
所以接下来的几行断言的是已经完成的状态：订阅者如何结算，以及什么到达了 `card-receipts`。
测试主体把已启动的应用和付款的 id 作为参数接收，所以下面的线上测试也运行它。

## 没有银行卡的付款 {/* #a-payment-without-a-card */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#cardless"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#cardless"
```

</TabItem>
</Tabs>

`settled` 检查处理器返回的结果。
`assert_not_called` 检查没有发布任何收据。

## 不必等待的延迟 {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

测试在暂停的时钟下启动，[`advance`](api:ruststream::testing::TestApp::advance) 把时钟向前拨。
一分钟的 `HOLD` 不花任何时间。
AMQP 1.0 没有延迟重新投递，所以延迟结束时，[`retry_after`](../components/settlement/retry-after.md) 把付款的一份副本发布到它的队列。
副本在消息头里带着重试次数，`published` 把它显示在 `card-payments` 上，与测试自己发布的消息并列。
第三次投递是挂载点允许的最后一次，所以付款按到达时的原样进入[死信地址](../components/settlement/dead-letters.md) `card-payments.review`。
每次被暂缓的投递都记录为 `Outcome::Nack`。

## 进程内 Broker 模拟了什么 {/* #what-the-in-process-broker-models */}

进程内 Broker 像 `connect` 一样读取 Broker 的 URL。
客户端无法打开的 URL 会让测试启动失败；在这里，`amqps://` 同样需要 `rustls` 或 `native-tls` feature。
每次发布都会转换成 AMQP 消息再转换回来，所以处理器读到的消息头与线上投递带的相同。
它模拟 Broker 的路由：

- `queue` 地址的各个消费者相互竞争，每次投递只交给一个；每个 `topic` 订阅都收到一份副本。
- `raw` 地址按原样投递给一个消费者。
- 队列订阅会创建它的队列，没有消费者附加期间到达的消息由队列保留。
- `retry` 在 `delivery-count` 中计入这次尝试，未结算的投递在其订阅结束时回到队列。
- 至多一次订阅的投递已经结算，`ack` 返回 `AckError::Unsupported`。
- 事务在提交前什么也不发布，无人应答的请求会超时。

链路信用、Broker 的死信策略和投递上限、持久性，以及 Broker 为发布创建的地址，都需要正在运行的 Broker。
在进程内，发往尚无队列订阅打开的地址的消息会被丢弃。

## 在 Broker 上运行同一个测试 {/* #the-same-test-on-a-broker */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) 把同一个应用连接到正在运行的 Broker。
测试用 `live::url`（示例自己的辅助函数）从 `AMQP_TEST_URL` 读取 URL，未设置时跳过。
付款的 id 是进程 id，因此可以在 Broker 上区分每次运行的消息。
断言与进程内的完全相同。

```bash
cargo test
AMQP_TEST_URL=amqp://artemis:artemis@localhost:5672 cargo test -- --test-threads=1
```
