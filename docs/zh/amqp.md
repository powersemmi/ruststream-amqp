# AMQP 1.0

`ruststream-amqp` 是 AMQP 1.0 的 Broker，构建在 [`fe2o3-amqp`](https://docs.rs/fe2o3-amqp) 之上。
该协议是 ISO 标准，因此一个 crate 就覆盖 ActiveMQ Artemis、RabbitMQ 4.x、Azure Service Bus、
Amazon MQ、Solace、Apache Qpid 和 IBM MQ。框架本身的概念（写订阅者、路由、编解码器、中间件）参见
[RustStream 文档](https://powersemmi.github.io/ruststream/)。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-amqp = "0.7"
serde = { version = "1", features = ["derive"] }
```

## prelude { #the-prelude }

`use ruststream_amqp::prelude::*;` 是服务文件写下的唯一一条导入。它带来 Broker 及其 `Sasl` 配置、
地址描述符及其 `Settle` 保证、发布策略、`AmqpError`、处理器用来约束槽位的能力 trait，以及框架
自己的 prelude。

服务用两套词汇来写。处理器主体写的是能力，因此那里只要 `ruststream::prelude::*` 就够了：每个槽位
用主体需要的那个能力来约束（`Out<impl Publisher>`、`Out<impl TransactionalPublisher>`、
`Out<impl RequestReply>`），具体的发布者由挂载点给出。路由文件写的是策略。它导入这个 glob，策略在
其中已经去掉了 Broker 前缀：

| crate 根 | prelude 中 |
|---|---|
| `AmqpPublish` | `Publish` |
| `AmqpTransactionalPublish`（feature `transaction`） | `TransactionalPublish` |

因此在任何 Broker 上，挂载点都写成 `b.include(handler).out(Reply, Publish)`，把服务迁到另一个
Broker 改的是一条导入，而不是每一处挂载点。策略的名字以 `Publish` 结尾，它的活动形态所对应的能力
trait 以 `Publisher` 结尾，两套词汇因此不会重名。带前缀的原名同样导出：有的文件同时 glob 两个
Broker 的 prelude，必须说清是哪个 `Publish`，那时就写它们。

## 能力 { #capabilities }

框架的可选能力 trait，以及这个 Broker 原生实现了哪些：

| 能力 | 原生 | 说明 |
| --- | --- | --- |
| `Subscribe` | 是 | 按名字订阅；名字就是地址，原样发送 |
| `BatchSubscriber` | 是，在客户端 | [一次 transfer 只投递一条消息，因此批由框架的缓冲组装](#batches) |
| `TransactionalPublisher` | 是，需要 `transaction` feature | [事务性发送](#transactions)，每个句柄同时只有一个 Broker 端事务 |
| `OwnedTransactions` | 否 | 事务属于发布者句柄，而不属于一个独立的值 |
| `RequestReply` | 是 | [`reply-to`、`correlation-id` 和动态响应链路](#requestreply) |
| `Partitioned` | 是 | [分区键就是 `group-id` 属性](#headers-and-the-partition-key) |
| `Seekable` 和 `Positioned` | 否 | 协议没有暴露客户端可以定位过去的位置 |
| `DescribeServer` | 是 | 报告连接 URL 里的主机和端口，不含它可能带的凭据 |
| 逐条消息的发布设置 | 无 | [发布不带 `header` 段，调用点没有可调整的东西](#publishing) |

## 生命周期 { #the-lifecycle }

Broker 是一串消费 `self` 的状态转移，因此每个状态都是各自独立的类型：

```text
AmqpBroker::new(url)      只记录配置，同步，不做 I/O
  .connect()   ->  ConnectedAmqpBroker      活动连接；订阅与发布者
  .shutdown()  ->  ()                       结束会话，关闭连接
```

`new` 只记下 URL，因此服务同步组装，运行时在启动时连接一次。`shutdown` 消费已连接的 Broker，所以
在它之后发布或订阅无法通过编译。更早交出去的发布者共享连接而不拥有它：`shutdown` 之后它返回错误，
而不是对着已关闭的连接照样成功。

在同步形态上你可以设置认证和身份。`sasl` 接收 `Sasl::anonymous()`、`Sasl::plain(user, pass)` 或
`Sasl::external()` 配置，`container_id` 则向 Broker 报出这个服务的名字，默认是 `"ruststream"`。
带用户信息的 URL（`amqp://user:pass@host`）自己就会选中 PLAIN，显式设置的配置优先于它。
`amqps://` 端点需要 `rustls` 或 `native-tls` feature。

每个订阅跑在自己的 AMQP 会话上，发布者共用另外一个会话。流控窗口按会话计算，因此慢消费者不会让
发布者或另一个订阅陷入饥饿。

## 寻址 { #addressing }

AMQP 1.0 规定了协议本身，却没有规定地址的含义，因此意图由 `AmqpAddress` 写明。每个构造函数都会
声明对应的 terminus 能力，Artemis 和其他产品正是据此区分它们：

| 构造函数 | 语义 | terminus 能力 |
| --- | --- | --- |
| `AmqpAddress::queue(name)` | 任播：竞争消费者，每条消息只投递给其中一个 | `queue` |
| `AmqpAddress::topic(name)` | 组播：投递给每个订阅者 | `topic` |
| `AmqpAddress::raw(address)` | 原样使用，供部署自己的约定 | 无 |

描述符直接写在订阅者属性里：

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_service.rs:handler"
```

服务只写一次 Broker，并把处理器挂载到它上面：

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_service.rs:app"
```

描述符上有两项设置：

- `credit(nonzero!(n))` 规定 Broker 最多可以同时向这个订阅投出多少条未结算的投递，默认是 256。
  信用额度是协议自带的背压，因此调小它就限制了同时在途的工作量。没有信用额度的订阅什么也收不到，
  所以这个计数是 `NonZeroU32`，`credit(0)` 无法通过编译。
- `settle(Settle::AtMostOnce)` 把订阅切换为至多一次投递：接收方在收到时就结算每一次投递。

地址为空的描述符在任何 I/O 之前就被拒绝。

`#[subscriber("orders")]` 这种纯字符串写法也可用：名字变成 `AmqpAddress::raw`。

## 批 { #batches }

参数是切片的处理器，一次拿到的是一批消息而不是一条，批的大小由挂载点给出：

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_batches.rs:handler"
```

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_batches.rs:app"
```

AMQP 1.0 没有批量拉取：一次 transfer 只投递一条消息，信用额度是流控而不是批的大小。因此批在客户端
组装：一批最多装挂载点给出的那个数量；这段时间内只到了更少的消息，那就装更少。

描述符上的 `batch_wait` 是关闭未满的批的时限，默认 10 毫秒。流量稀疏时，更长的时限换来更满的批；
流量平稳时，先关闭批的是大小，时限根本不会触发。信用额度和批的大小是两回事：信用额度限制 Broker
同时投出多少，批的大小是一次处理器调用看到多少条消息。

## 确认与 disposition { #acknowledgement-and-dispositions }

处理器的结果就是协议的 disposition：

| 处理器结果 | disposition | 效果 |
| --- | --- | --- |
| `HandlerOutcome::ack()` | `accept` | 这次投递完成，Broker 将它丢弃 |
| `HandlerOutcome::retry()` | `release` | 这次投递退回 Broker，等待重新投递 |
| `HandlerOutcome::drop()` | `reject` | 终态；之后由 Broker 的死信策略决定 |

在至多一次的订阅上，投递到达时就已经结算，因此 `ack` 和 `nack` 返回 `AckError::Unsupported`。

AMQP 1.0 没有延迟重新投递，所以 `HandlerOutcome::retry_after(delay)` 由运行时的延后重新发布来完成。
这条路径需要自己的发布者，用 `retry_via` 接到作用域上；没有它，延迟会被丢弃，投递立刻退回 Broker。

副本发往订阅自己的地址。一个 AMQP 节点既是接收方附着的对象，也是发送方发布的目标，因此这里的订阅
总能说出发布者再次抵达它的地址，`retry_via` 也就适用于这个 Broker 打开的每一个订阅：描述符写法和
纯 `#[subscriber("orders")]` 写法都一样。副本带有 `x-ruststream-retry-count` 消息头，其中记录着
重试次数，处理器因此能区分首次投递和延迟投递。在 `queue` 地址上，它和其他消息一样参与消费者竞争；
在 `topic` 地址上，每个订阅者都会看到它。

## 发布 { #publishing }

`AmqpPublish` 是构造发布者 `AmqpPublisher` 的策略。策略在注册处理器时给出，启动时它在已连接的
Broker 上实例化发布者。它也是这个 Broker 的默认策略，因此挂载点没有点名回复发布者的
`#[subscriber(.., publish)]` 处理器，就通过它回复。

回复写 `.out(Reply, Publish)`，注入的槽位写 `.out(<marker>, Publish).build()`；`Publish` 是该策略
在 [prelude](#the-prelude) 中的名字。策略没有选项，所以直接写名字即可。

单次发布同样没有自己的设置。有些 Broker 允许调用点用发布构建器上的一个步骤调整单条消息，比如
优先级、存活时间。这里没有这样的步骤，因为它不发送 AMQP 的 `header` 段，而 `durable`、`priority`
和 `ttl` 都放在那一段里。因此处理器主体保留 `Out<impl Publisher>`，不从这个 crate 导入任何东西。

发送链路在第一次使用时附着，并按地址各留一条。Broker 用 `accept` 以外的方式结算的发布
（rejected、released、modified）返回错误，Broker 端的拒绝因此不会被当成一次成功的发布。

## 请求-响应 { #requestreply }

请求-响应就在协议里，因此 `AmqpPublisher` 实现了 `RequestReply` 能力。`request(msg, timeout)`
附着一条动态接收链路，Broker 为这一个请求单独分配一个私有的响应地址。它发出带 `reply-to` 和
`correlation-id` 的消息，并以第一条关联 id 匹配上的响应完成。超时之内无人响应的请求返回错误，
响应链路在两种情况下都会断开。

发起对话的请求没有可回答的投递，因此它从作用域的 `after_startup` 钩子里运行，那时发布者已经
在工作：

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_request_reply.rs:request"
```

响应方读出请求方写下的响应地址，把答复发布到那里，并把 `correlation-id` 原样回传。这个地址每个
请求都不同，所以答复通过注入的发布者发出，而不是通过处理器返回的回复：回复的目的地在写服务时就
已固定。槽位写的是主体需要的能力（`Out<impl Publisher>`），
`b.include(greet).out(DefaultSlot, Publish).build()` 把策略绑定到这个处理器声明的匿名槽位上。

问候类型没有 `#[outgoing(name = ..)]`，因此目的地由调用点给出：这里就是 `to(..)` 里那个逐请求的
响应地址。

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_request_reply.rs:responder"
```

两端都在
[`examples/amqp_request_reply.rs`](https://github.com/powersemmi/ruststream-amqp/blob/main/crates/ruststream-amqp/examples/amqp_request_reply.rs)
里。

## 事务 { #transactions }

开启 `transaction` feature 后，`AmqpTransactionalPublish` 是构造事务发布者 `AmqpTxnPublisher` 的
策略，后者通过协议的事务性发送来发布。`begin_transaction`、`commit` 和 `abort` 只存在于那个发布者
上，所以你写这个策略而不是 `Publish`，就拿到了它们。

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_transaction.rs:transaction"
```

一个句柄同时最多持有一个 Broker 端事务。事务已打开时再次 `begin_transaction` 返回错误，已打开的
事务不受影响；没有事务时 `commit` 或 `abort` 同样返回错误。返回了错误的提交或中止照样会关掉事务，
因此下一次 `begin_transaction` 开的是新的。事务之外的发布立刻发出，用的是同一个发布者。

事务覆盖的是发布：接收和结算不在其中。

## 消息头与分区键 { #headers-and-the-partition-key }

常用消息头映射到 AMQP 的 `properties` 段：`content-type`、`correlation-id`、`reply-to`、
`message-id`，分区键则映射为 `group-id`。其余消息头进入 `application-properties`。框架没有自己的
信封，因此另一套协议栈上的对端看到的是一条普通的 AMQP 消息，而另一个 AMQP 客户端产出的消息到达时
消息头完好无损。

分区键是 `partition-key` 消息头（导出为 `PARTITION_KEY_HEADER`），投递过来的消息实现了
`Partitioned` 能力。

## 测试 { #testing }

`testing` feature 提供 `AmqpTestBroker`：一个进程内传输，不需要服务器、不走 AMQP 网络，就复现这个
crate 的行为。测试文件按它自己的路径导入，`use ruststream_amqp::testing::AmqpTestBroker;`，与
prelude 的 glob 并列。它遵循与真实 Broker 相同的生命周期阶梯，并驱动 `TestApp` 测试套件。参见
[用 TestApp 对服务做单元测试](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp)。

整份生产声明都能在测试 Broker 上解析，因此测试跑的是服务真正交付的那套接线，而不是它的一份改写
副本。`#[subscriber(AmqpAddress::queue("orders"))]` 原样挂载到 `AmqpTestBroker` 上，
`.out(Reply, Publish)` 挂载的是生产策略，`AmqpTransactionalPublish` 配出的是一个进程内发布者，
它缓冲到提交为止。挂载点上没有只供测试替换的策略，也没有哪个能力只存在于其中一个 Broker：
`RequestReply` 和 `TransactionalPublisher` 都被带了过来，因此约束 `Out<impl RequestReply>` 或
`Out<impl TransactionalPublisher>` 的处理器在进程内同样能挂载。

行为也随它们一起过来，因为不会失败的测试毫无价值。这里决定投递的是 terminus，和在服务器上一样：
同一地址上的 `AmqpAddress::queue` 订阅争抢每一条消息，`AmqpAddress::topic` 订阅各拿一份副本。
因此，真实 Broker 会判失败的那种测试，工作队列型的服务在进程内也通不过。`AmqpAddress::raw` 地址
不声明任何能力，服务器会去查自己的配置，而进程内的这一份没有配置可查，于是每条消息只投递一次；
要断言的正是广播时，就写 `topic`。至多一次的投递到达时已经结算，
它的 `ack` 返回 `AckError::Unsupported`。批来自同一个客户端缓冲，用的是描述符自己的 `batch_wait`。
事务在提交之前什么都不发布，中止时丢弃自己的缓冲；乱序调用（事务已开时再次 `begin_transaction`、
没有事务时提交）返回错误，而不是悄悄成功。请求带着 `reply-to` 和 `correlation-id`，以关联上的响应
完成，无人应答时返回 `AmqpError::RequestTimeout`。

留在外面的，是 Broker 能持有而一个进程持有不了的东西。每一条都会让断言变得不可靠，
而不只是不精确：

- **没有存储。** 发布到没有活动订阅的地址上的消息会被记录下来然后丢弃，而服务器会把它留给之后
  附着上来的消费者。先把订阅打开。
- **没有 Broker 端的重新投递。** 被释放的投递回到原来那个订阅，绝不会转给竞争消费者，
  `nack(requeue = false)` 背后也没有死信策略。
- **没有持久性。** 已提交的事务在处理器能观察到的范围内是原子的，但缓冲就在这个进程里：崩溃之后
  什么都不剩，也没有 Broker 端的事务超时或栅栏机制。
- **没有流控。** `credit` 在这里没有对应物。像链路那样把消息压住，等于把路由器变成 Broker 端的
  队列，而处理器观察不到任何差别，因此进程内的订阅没有上界，预取窗口在这里也无法断言。
- **没有拒绝。** 发往无人消费之处的请求只会超时，而不会被拒绝或进入死信。

这些交给真机测试：`just test-brokers` 用 `docker-compose.test.yml` 启动 ActiveMQ Artemis，
对它跑集成测试和全部 `conformance` 校验套件，由 `AMQP_TEST_URL` 控制开关。
