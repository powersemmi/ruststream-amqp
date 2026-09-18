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

## 这个 crate 提供什么 { #what-the-crate-offers }

寻址是显式的：AMQP 1.0 规定了线上格式，却把地址的含义留给部署。一条订阅要么指定队列（任播），要么
指定主题（组播），要么按原样给出地址，而 terminus 用自己的能力告诉 Broker 选的是哪一种。
[订阅](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#subscribing)一节讲描述符
及其设置、作为协议自身背压的信用、至多一次投递、每个处理器结果对应的 disposition、重试次数上限和
`retry_after` 背后的延后重新发布，以及在客户端组装的批。
[发布](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#publishing)一节讲发布
策略、基于动态回复链路的请求-响应，以及事务性投递。再往后是
[生成的文档](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#the-generated-document)、
基于进程内 Broker 的[测试](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#testing)，
以及[运维](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#operations)：认证、
TLS、会话和已知的限制。

## 接下来读什么 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[AMQP 参考](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html)** - 寻址、批、disposition、重试、请求-响应、事务、生成的文档和测试。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 框架本身：订阅者、发布、路由、编解码器、中间件、可观测性和 CLI。
- :material-language-rust: **[API 参考](https://docs.rs/ruststream-amqp)** - 该 crate 导出的每个类型和方法。

</div>

## 本站点与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本页是入口：这个 crate 是什么、如何安装、第一个服务长什么样。crate 的每个主题都写在实现它的代码
旁边，发布在 [docs.rs](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html) 上。框架
在任何 Broker 上都一样的部分，写在
[RustStream 文档](https://powersemmi.github.io/ruststream/)里。
