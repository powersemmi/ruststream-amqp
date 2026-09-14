# ruststream-amqp

**`ruststream-amqp`** запускает сервис [RustStream](https://powersemmi.github.io/ruststream/) на
AMQP 1.0. Протокол - стандарт ISO, поэтому один крейт обслуживает всё семейство: ActiveMQ
Artemis и Classic, RabbitMQ 4.x, Azure Service Bus и Event Hubs, Amazon MQ, Solace, Apache Qpid и
IBM MQ.

В RabbitMQ AMQP 1.0 - отдельный стек протокола, а не тот 0.9.1, который реализует
[`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin).

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-amqp = "0.7"
serde = { version = "1", features = ["derive"] }
```

`ruststream_amqp::prelude::*` - единственный импорт, который пишет файл сервиса. Он реэкспортирует
брокер и его профиль аутентификации, дескриптор адреса и его гарантию доставки, политики публикации,
ошибку крейта и прелюдию самого фреймворка.

```rust
--8<-- "crates/ruststream-amqp/examples/amqp_service.rs:app"
```

## Что даёт крейт {#what-the-crate-offers}

Адресация задаётся явно: AMQP 1.0 стандартизирует формат обмена, но смысл адреса оставляет
развёртыванию. Подписка называет очередь (anycast), тему (multicast) или адрес «как есть», а
терминус своей совместимостью сообщает брокеру, что именно выбрано. В разделе
[«Подписка»](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#subscribing) -
дескриптор и его настройки, кредит как собственное обратное давление протокола, доставка
at-most-once, диспозиция для каждого исхода обработчика, ограничение числа повторов и
отложенная повторная публикация за `retry_after`, а также пакеты, которые собираются на стороне
клиента. В разделе
[«Публикация»](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#publishing) -
политики публикации, запрос и ответ по динамической обратной связи и транзакционная отправка.
Дальше -
[сгенерированный документ](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#the-generated-document),
[тестирование](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#testing) на
брокере внутри процесса и
[эксплуатация](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html#operations):
аутентификация, TLS, сеансы и известные ограничения.

## Куда идти дальше {#where-to-go-next}

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[Справочник по AMQP](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html)** - адресация, пакеты, диспозиции, повторы, запрос и ответ, транзакции, сгенерированный документ и тестирование.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: подписчики, публикация, маршрутизация, кодеки, middleware, наблюдаемость и CLI.
- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-amqp)** - каждый тип и метод, которые экспортирует крейт.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Эта страница - точка входа: что такое крейт, как его установить и как выглядит первый сервис.
Каждая тема крейта описана рядом с кодом, который её реализует, на
[docs.rs](https://docs.rs/ruststream-amqp/latest/ruststream_amqp/index.html). Что фреймворк делает
на любом брокере, описано в
[документации RustStream](https://powersemmi.github.io/ruststream/).
