---
title: 连接生产环境
sidebar_position: 8
description: "把 AMQP 1.0 服务连接到生产环境的 Broker：TLS、来自环境变量的 SASL 凭据、容器 id、关闭，以及对配置的测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 连接生产环境

生产环境的 Broker 要求 TLS 和凭据，它的运维人员则按名称查找服务。

## TLS 与凭据 {/* #tls-and-credentials */}

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

```toml title="Cargo.toml"
[dependencies]
ruststream-amqp = { version = "0.7", features = ["rustls"] }
```

部署环境通过环境变量提供地址和凭据。
`amqps://` URL 通过 TLS 连接。
它用主机名而不是 IP 地址指明 Broker：TLS 会用这个名称核对 Broker 的证书。
TLS 需要两个 feature 之一：`rustls` 使用 [rustls](https://docs.rs/rustls)，`native-tls` 使用平台自带的 TLS。

[`Sasl::plain`](api:ruststream_amqp::Sasl::plain) 用用户名和密码让服务登录。
这时 URL 中不带凭据。
密码中含有在 URL 里有特殊含义的字符（例如 `/`、`?` 或 `@`）时，可以原样交给 `Sasl::plain`。
[`Sasl::anonymous`](api:ruststream_amqp::Sasl::anonymous) 不带凭据连接，[`Sasl::external`](api:ruststream_amqp::Sasl::external) 使用连接在 SASL 之外已经建立的凭据。

在 Azure Service Bus 上，用户名是共享访问策略的名称，密码是它的密钥，见其 [AMQP 指南](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-protocol-guide)。

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

[`container_id`](api:ruststream_amqp::AmqpBroker::container_id) 向 Broker 报出服务的名称，默认是 `ruststream`。
Broker 的控制台以这个名称列出连接，服务的各条链路也以它命名。
为一个服务的每个部署设置各自的容器 id，运维人员就能区分它们。

每个订阅运行在自己的 AMQP 会话上，各个发布者共用另一个会话。
流控按会话进行，所以慢速消费者不会拖住发布者或其他订阅。

## 关闭 {/* #shutdown */}

服务停止时，由内向外关闭连接：先是各订阅的会话，然后是发布者的链路及其会话，最后是连接本身。
此后使用的发布者会返回 [`AmqpError::NotConnected`](api:ruststream_amqp::AmqpError::NotConnected) 错误，而不是在已关闭的连接上报告成功。
服务的 AsyncAPI 文档写明 Broker 的主机和端口，从不包含凭据。

## 测试配置 {/* #testing-the-configuration */}

测试在 `TestApp::start` 下启动生产环境的 `service`，所用的 `broker` 也是生产环境的那个，只是传入了测试凭据。
进程内 Broker 像客户端那样检查 URL，但不连接 Broker。
客户端无法打开的 URL 会让启动失败：

```text
the scheme "https" is not an AMQP scheme
```

测试需要与服务相同的 TLS feature：没有它，`amqps://` URL 同样会让启动失败。
认证和证书检查只在真实的 Broker 上进行。
