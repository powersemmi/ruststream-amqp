# ruststream-amqp

**`ruststream-amqp`** 让 [RustStream](https://powersemmi.github.io/ruststream/) 服务运行在 AMQP 1.0
之上。该协议是 ISO 标准，因此一个 crate 就覆盖整个家族：ActiveMQ Artemis 与 Classic、RabbitMQ
4.x、Azure Service Bus 与 Event Hubs、Amazon MQ、Solace、Apache Qpid 和 IBM MQ。

在 RabbitMQ 上，AMQP 1.0 与 [`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin)
所用的 0.9.1 是两套独立的协议栈。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-amqp = "0.7"
serde = { version = "1", features = ["derive"] }
```

`ruststream_amqp::prelude::*` 是服务文件写下的唯一一条导入。它重导出 Broker 及其认证配置、地址
描述符及其投递保证、发布策略、本 crate 的错误类型，以及框架自己的 prelude。

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_service.rs:app"
```

## 接下来读什么 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[AMQP 指南](amqp.md)** - 寻址、批、disposition、请求-响应、事务和测试。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 框架本身：订阅者、发布、路由、编解码器、中间件、可观测性和 CLI。
- :material-language-rust: **[API 参考](https://docs.rs/ruststream-amqp)** - 该 crate 导出的每个类型和方法。

</div>

## 本站点与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本站点讲 AMQP 1.0 和这个 crate。框架在任何 Broker 上都一样的部分，写在
[RustStream 文档](https://powersemmi.github.io/ruststream/)里。两者相接的地方，本站点的页面会给出
链接。
