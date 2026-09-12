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

## Куда идти дальше {#where-to-go-next}

<div class="grid cards" markdown>

- :material-transit-connection-variant: **[Руководство по AMQP](amqp.md)** - адресация, пакеты, диспозиции, запрос и ответ, транзакции и тестирование.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: подписчики, публикация, маршрутизация, кодеки, middleware, наблюдаемость и CLI.
- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-amqp)** - каждый тип и метод, которые экспортирует крейт.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт описывает AMQP 1.0 и этот крейт. Что фреймворк делает на любом брокере, описано в
[документации RustStream](https://powersemmi.github.io/ruststream/). Страницы этого сайта ссылаются
на неё там, где темы соприкасаются.
