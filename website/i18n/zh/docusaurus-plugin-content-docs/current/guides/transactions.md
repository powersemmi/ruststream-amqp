---
title: 全有或全无的发布
sidebar_position: 6
description: "在一个处理器中把多条 AMQP 1.0 消息一起发布或全都不发布：事务性投放，以及什么也不发布的中止。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 全有或全无的发布

本指南在一个处理器中把多条消息一起发布，或者一条也不发布。

AMQP 1.0 在协议中定义了事务：发送方在事务中投放消息，Broker 在提交时让它们可见。
`transaction` feature 开启这一功能：

```toml title="Cargo.toml"
[dependencies]
ruststream-amqp = { version = "0.7", features = ["transaction"] }
```

示例是一个账务服务。
它把每个订单变成若干总账分录和一张发票，它们要么全部到达 Broker，要么一个也不到达。

## 处理器 {/* #the-handler */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/transactions.rs#bill"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/transactions.rs#bill"
```

</TabItem>
</Tabs>

`Billing` 槽位的约束是 `Out<impl TransactionalPublisher, _>`：函数体声明它需要事务，而不指明 Broker。
[`begin_transaction`](api:ruststream::TransactionalPublisher::begin_transaction) 在槽位的发布者上开启一个事务。
此后的每次发布都投放在事务之中，由 Broker 暂扣。
[`commit`](api:ruststream::TransactionalPublisher::commit) 让总账分录和发票同时可见。
缺少价格的行让订单无法计费，[`abort`](api:ruststream::TransactionalPublisher::abort) 丢弃已投放的内容，因此 `ledger` 和 `invoices` 什么也收不到。

提交或中止即使返回错误，事务也已经结束，所以下一次 `begin_transaction` 会开启一个新事务。
提交返回错误时，`bill` 重试这个订单。

## 应用 {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/transactions.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/transactions.rs#app"
```

</TabItem>
</Tabs>

`TransactionalPublish` 是支持事务的策略，即 prelude 中名为 `TransactionalPublish` 的 [`AmqpTransactionalPublish`](api:ruststream_amqp::AmqpTransactionalPublish)。
它的发布者一次只持有一个事务。
在已有事务开启时再次调用 `begin_transaction` 会返回错误，已开启的事务不受影响。
每次注册都得到自己的发布者，所以两个处理器可以同时各持有一个事务。
在事务之外通过这个发布者发布的消息会立即发出。

服务启动时执行、没有投递可处理的事务，放在启动钩子中运行：`b.after_startup(TransactionalPublish, ..)` 把同一个发布者交给钩子。

## 订单自身的投递 {/* #the-orders-own-delivery */}

事务涵盖处理器发布的内容。
订单的投递在处理器返回之后、在事务之外结算。
在提交与结算之间崩溃会让订单再次投递，它的总账分录和发票会再发布一次。
为消息设置一个读者可用来丢弃重复消息的 id，例如这里的订单 id。

## 测试事务 {/* #testing-the-transaction */}

示例文件中的测试在进程内运行服务。
进程内 Broker 在提交前什么也不发布，并丢弃中止事务的消息，所以测试从已发布的内容中读出结果。
[测试 AMQP 服务](testing.md)介绍测试套件。
