---
title: 快速开始
sidebar_position: 1
description: "安装 ruststream-amqp，启动本地 ActiveMQ Artemis，让第一个订阅者连上它运行，并在没有 Broker 的情况下测试它。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 快速开始

本指南安装 `ruststream-amqp`，启动一个本地 AMQP 1.0 Broker，并让第一个服务连上它运行。

## 安装 {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-amqp = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-amqp = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt"] }
```

`ruststream` 是框架本身：`macros` 提供属性宏，`json` 提供默认编解码器，`logging` 把服务的日志打印到终端。
`ruststream-amqp` 是 AMQP 1.0 Broker。
它的 `testing` feature 加入测试所用的[进程内 Broker](../components/testing/index.md)。

这个 Broker crate 有以下 feature，默认全部关闭：

| Feature | 提供的内容 |
|---|---|
| `rustls` | 通过 rustls 建立基于 [TLS](../components/broker/security.md) 的 `amqps://` 连接 |
| `native-tls` | 通过平台自带的 TLS 建立 `amqps://` 连接 |
| `transaction` | [事务性发布](transactions.md) |
| `asyncapi` | 生成的 AsyncAPI 文档中的 [AMQP 1.0 细节](../components/asyncapi/index.md) |
| `testing` | `TestApp` 背后的进程内 Broker |

底层客户端 [fe2o3-amqp](https://docs.rs/fe2o3-amqp) 用 Rust 编写，因此构建不需要任何系统库。

## 本地 Broker {/* #a-local-broker */}

开发时，一个 ActiveMQ Artemis 就够了：

```yaml title="compose.yaml"
services:
  artemis:
    image: apache/activemq-artemis:latest-alpine
    environment:
      ARTEMIS_USER: artemis
      ARTEMIS_PASSWORD: artemis
    ports:
      - "5672:5672"
```

```bash
docker compose up -d
```

Broker 在 `localhost:5672` 上监听 AMQP，用户名是 `artemis`，密码是 `artemis`。
服务第一次读取某个队列时，Artemis 会创建它，所以无需事先声明任何东西。

## 第一个服务 {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` 从队列 `orders` 读取 JSON 订单。
带金额的订单被接受，没有金额的订单被拒绝。
[`AmqpAddress::queue`](../components/subscriptions/queue.md) 指明队列，[队列与主题](queues-and-topics.md)介绍其他种类的地址。
[`AmqpBroker::new`](../components/broker/index.md) 接收 Broker 的 URL，只把它记录下来。
连接在服务启动时才打开。
服务用 URL 中的用户名和密码登录。
`#[ruststream::app]` 把 `app` 变成 `main`；在手写形式中，[`run_main`](api:ruststream::runtime::cli::run_main) 做同样的事。

运行服务：

```bash
cargo run -- run
```

在另一个终端里，用镜像自带的命令行客户端发布一个订单：

```bash
docker compose exec artemis /opt/activemq-artemis/bin/artemis producer \
  --protocol amqp --user artemis --password artemis \
  --destination queue://orders --message-count 1 --message '{"id":1,"amount_cents":1250}'
```

服务记录一条带订单 id 的 `order taken` 日志。
<kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。

## 第一个测试 {/* #the-first-test */}

示例文件里还有两个测试。
每个测试都用 [`TestApp::start`](../components/testing/index.md) 启动同一个 `app`，发布一个订单，然后检查 `take_order` 如何结算它。
在测试中，`AmqpBroker` 连接的是进程内 Broker，所以测试不需要 Artemis：

```bash
cargo test
```

[测试 AMQP 服务](testing.md)逐一讲解这类测试，以及进程内 Broker 模拟了什么。

## 从 fe2o3-amqp 迁移 {/* #coming-from-fe2o3-amqp */}

基于 fe2o3-amqp 的服务要自己打开连接和会话，附加链路，并编写接收循环。
下表列出每一部分在 RustStream 中的位置：

| fe2o3-amqp | RustStream |
|---|---|
| `Connection::open` | [`AmqpBroker::new`](api:ruststream_amqp::AmqpBroker::new)，以及 [`sasl`](../components/broker/security.md) 和 [`container_id`](../components/broker/container-id.md) |
| `Session::begin` | 无需编写：每个订阅有自己的会话，各个发布者共用一个会话 |
| `Receiver::attach` 加一个 `recv` 循环 | 一个 `#[subscriber(AmqpAddress::..)]` 处理器 |
| `CreditMode::Auto(n)` | [`AmqpAddress::credit`](../components/subscriptions/credit.md) |
| `accept`、`modify`、`reject` | 处理器的结果：`ack`、`retry`、`drop`（[确认与投递保证](acknowledgement.md)） |
| `auto_accept(true)` | [`Settle::AtMostOnce`](../components/settlement/settle.md) |
| `Sender::attach` 和 `send` | 回复或 `Out` 参数，经由 [`Publish`](../components/publishing/index.md) 策略发送 |
| 用于接收回复的动态接收者 | [`request`](../components/request-reply/index.md)（[请求与回复](request-reply.md)） |
| `Controller` 和 `Transaction::declare` | [`TransactionalPublish`](../components/transactions/index.md)（[全有或全无的发布](transactions.md)） |

通过 AMQP 0.9.1 与 RabbitMQ 通信的服务应改用 [ruststream-lapin](https://powersemmi.github.io/ruststream-lapin/)：两种协议共用端口和 `amqp://` 协议名，但它们是不同的协议。
