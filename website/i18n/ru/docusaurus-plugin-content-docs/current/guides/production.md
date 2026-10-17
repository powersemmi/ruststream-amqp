---
title: Подключение к промышленному брокеру
sidebar_position: 8
description: "Подключение сервиса на AMQP 1.0 к промышленному брокеру: TLS, учётные данные SASL из окружения, идентификатор контейнера, остановка и проверка конфигурации."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Подключение к промышленному брокеру

Промышленный брокер требует TLS и учётных данных, а его администраторы ищут сервис по имени.

## TLS и учётные данные {/* #tls-and-credentials */}

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

```toml title="Cargo.toml"
[dependencies]
ruststream-amqp = { version = "0.7", features = ["rustls"] }
```

Адрес и учётные данные развёртывание передаёт в переменных окружения.
С URL `amqps://` сервис подключается по [TLS](../components/broker/security.md).
URL называет брокер по имени хоста или по адресу IPv4: TLS сверяет это имя с сертификатом брокера.
Адрес IPv6 не даёт TLS имени сервера, поэтому подключение по нему завершается ошибкой.
TLS требует одной из двух фич: `rustls` использует [rustls](https://docs.rs/rustls), а `native-tls` - TLS платформы.

[`Sasl::plain`](../components/broker/security.md) выполняет вход сервиса с именем пользователя и паролем.
Тогда в URL нет учётных данных.
Пароль с символами, которые имеют особый смысл в URL, например `/`, `?` или `@`, передаётся в `Sasl::plain` как есть.
[`Sasl::anonymous`](api:ruststream_amqp::Sasl::anonymous) подключается без учётных данных, а [`Sasl::external`](api:ruststream_amqp::Sasl::external) использует учётные данные, которые соединение уже установило вне SASL.

В Azure Service Bus имя пользователя - это имя политики общего доступа, а пароль - её ключ, как описывает [руководство по AMQP](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-protocol-guide).

## Сервис {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

[`container_id`](../components/broker/container-id.md) называет сервис брокеру, по умолчанию `ruststream`.
Консоль брокера показывает соединение под этим именем, и связи сервиса названы по нему.
Дайте каждому развёртыванию сервиса собственный идентификатор контейнера, чтобы администратор их различал.

Каждая подписка работает в собственном сеансе AMQP, а издатели делят ещё один.
Управление потоком действует на уровне сеанса, поэтому медленный потребитель не сдерживает издателей и другие подписки.

## Остановка {/* #shutdown */}

При остановке сервис [закрывает соединение](../components/broker/shutdown.md) изнутри наружу: сначала сеансы подписок, затем связи издателей и их сеанс, затем само соединение.
Издатель, которого используют после этого, возвращает ошибку [`AmqpError::NotConnected`](../components/broker/errors.md), а не выполняет публикацию через закрытое соединение.
[Документ AsyncAPI](../components/asyncapi/server.md) сервиса называет хост и порт брокера и никогда не содержит учётных данных.

## Проверка конфигурации {/* #testing-the-configuration */}

Тесты запускают рабочий `service` с рабочим `broker`, которому переданы тестовые учётные данные, через `TestApp::start`.
Брокер внутри процесса проверяет URL так же, как его читает клиент, без подключения к брокеру.
URL, который клиент не может открыть, не даёт запуститься:

```text
the scheme "https" is not an AMQP scheme
```

Тестам нужна та же фича TLS, что и сервису: без неё URL `amqps://` тоже не даёт запуститься.
Аутентификация и проверка сертификата выполняются только на настоящем брокере.
