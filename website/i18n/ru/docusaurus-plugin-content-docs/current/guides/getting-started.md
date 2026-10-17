---
title: Начало работы
sidebar_position: 1
description: "Установка ruststream-amqp, локальный ActiveMQ Artemis, первый подписчик на нём и его тест без брокера."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Начало работы

Это руководство устанавливает `ruststream-amqp`, запускает локальный брокер AMQP 1.0 и первый сервис на нём.

## Установка {/* #installation */}

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

`ruststream` - это сам фреймворк. Фича `macros` даёт атрибуты, `json` - кодек по умолчанию, а `logging` выводит журнал сервиса в терминал.
`ruststream-amqp` - брокер AMQP 1.0.
Его фича `testing` добавляет брокер внутри процесса, на котором работают тесты.

У крейта брокера такие фичи, все выключены по умолчанию:

| Фича | Что она даёт |
|---|---|
| `rustls` | соединения `amqps://` по TLS через rustls |
| `native-tls` | соединения `amqps://` через TLS платформы |
| `transaction` | [транзакционную публикацию](transactions.md) |
| `asyncapi` | подробности AMQP 1.0 в генерируемом документе AsyncAPI |
| `testing` | брокер внутри процесса, на котором работает `TestApp` |

Под ним работает клиент [fe2o3-amqp](https://docs.rs/fe2o3-amqp). Он написан на Rust, поэтому сборке не нужны системные библиотеки.

## Локальный брокер {/* #a-local-broker */}

Для разработки достаточно одного ActiveMQ Artemis:

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

Брокер принимает соединения AMQP на `localhost:5672`, пользователь - `artemis`, пароль - `artemis`.
Artemis создаёт очередь, когда сервис впервые её читает, поэтому объявлять ничего не нужно.

## Первый сервис {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` читает заказы в JSON из очереди `orders`.
Заказ с суммой он принимает, а заказ без суммы отклоняет.
Очередь называет [`AmqpAddress::queue`](api:ruststream_amqp::AmqpAddress::queue), а другие виды адресов показывает руководство [Очереди и темы](queues-and-topics.md).
[`AmqpBroker::new`](api:ruststream_amqp::AmqpBroker::new) принимает URL брокера и только запоминает его.
Соединение открывается при запуске сервиса.
Пользователь и пароль из URL служат для входа сервиса в брокер.
`#[ruststream::app]` превращает `app` в `main`. В ручной форме то же делает [`run_main`](api:ruststream::runtime::cli::run_main).

Запустите сервис:

```bash
cargo run -- run
```

Опубликуйте заказ из другого терминала консольным клиентом, который входит в образ:

```bash
docker compose exec artemis /opt/activemq-artemis/bin/artemis producer \
  --protocol amqp --user artemis --password artemis \
  --destination queue://orders --message-count 1 --message '{"id":1,"amount_cents":1250}'
```

Сервис пишет в журнал `order taken` с номером заказа.
<kbd>Ctrl</kbd>+<kbd>C</kbd> останавливает его.

## Первый тест {/* #the-first-test */}

В файле примера есть ещё два теста.
Каждый запускает то же `app` через `TestApp::start`, публикует заказ и проверяет, как `take_order` урегулировал его доставку.
`AmqpBroker` подключается там к брокеру внутри процесса, поэтому тестам не нужен Artemis:

```bash
cargo test
```

Руководство [Тестирование сервиса на AMQP](testing.md) разбирает такие тесты и то, что моделирует брокер внутри процесса.

## Переход с fe2o3-amqp {/* #coming-from-fe2o3-amqp */}

Сервис на fe2o3-amqp открывает соединение и сеансы, присоединяет связи и пишет цикл приёма.
Вот где каждая из этих частей находится в RustStream:

| fe2o3-amqp | RustStream |
|---|---|
| `Connection::open` | [`AmqpBroker::new`](api:ruststream_amqp::AmqpBroker::new), с [`sasl`](api:ruststream_amqp::AmqpBroker::sasl) и [`container_id`](api:ruststream_amqp::AmqpBroker::container_id) |
| `Session::begin` | ничего: у каждой подписки свой сеанс, а издатели делят один общий |
| `Receiver::attach` и цикл `recv` | обработчик `#[subscriber(AmqpAddress::..)]` |
| `CreditMode::Auto(n)` | [`AmqpAddress::credit`](api:ruststream_amqp::AmqpAddress::credit) |
| `accept`, `modify`, `reject` | исход обработчика: `ack`, `retry`, `drop` ([Подтверждение и гарантии доставки](acknowledgement.md)) |
| `auto_accept(true)` | [`Settle::AtMostOnce`](api:ruststream_amqp::Settle::AtMostOnce) |
| `Sender::attach` и `send` | ответ или параметр `Out`, отправленный через политику [`Publish`](api:ruststream_amqp::AmqpPublish) |
| динамический получатель для ответов | [`request`](api:ruststream::RequestReply::request) ([Запросы и ответы](request-reply.md)) |
| `Controller` и `Transaction::declare` | `TransactionalPublish` ([Публикация «всё или ничего»](transactions.md)) |

Сервису, который работает с RabbitMQ по AMQP 0.9.1, нужен [ruststream-lapin](https://powersemmi.github.io/ruststream-lapin/). У двух протоколов общие порт и схема `amqp://`, но это разные протоколы.
