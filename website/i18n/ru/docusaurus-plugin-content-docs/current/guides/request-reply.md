---
title: Запросы и ответы
sidebar_position: 5
description: "Запрос к другому сервису по AMQP 1.0 с ожиданием ответа, ответ на запросы по их адресу для ответа и запрос, на который никто не отвечает."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Запросы и ответы

Это руководство отправляет запрос другому сервису по AMQP 1.0 и ждёт ответа, отвечает на запросы по их адресу для ответа и обрабатывает запрос, на который никто не отвечает.

Запрос и ответ входят в сам протокол.
В запросе есть адрес для ответа в `reply-to` и идентификатор в `correlation-id`.
Ответ уходит на этот адрес и повторяет этот идентификатор.

Пример - два сервиса в одном приложении.
Сервис цен отвечает на вопросы о ценах, а сервис заказов спрашивает у него цену каждого заказа.

## Отвечающий сервис {/* #the-responder */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#responder"
```

</TabItem>
</Tabs>

Запрос приходит к [отвечающему обработчику](../components/request-reply/responders.md) с адресом для ответа в заголовке `reply-to`, и `ctx.headers().reply_to()` читает его.
`quote` публикует цену через свой слот `Answers` с `to(reply_to)`, потому что адрес новый для каждого запроса.
Поэтому `Price` не объявляет собственного адреса.
В ответе стоит `correlation-id` запроса, и запрашивающий сервис принимает только ответ с совпадающим идентификатором.
На товар без цены ответа нет, и ожидание заканчивает тайм-аут запрашивающего сервиса.

Отвечающий сервис читает очередь `prices.quote`.
Каждая реплика сервиса цен - ещё один её потребитель, и каждый запрос достаётся одной из них.

## Запрашивающий сервис {/* #the-requester */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#requester"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#requester"
```

</TabItem>
</Tabs>

`price_order` читает заказы и спрашивает цену каждого в `prices.quote`.
Слот `Pricing` ограничен как `Out<impl RequestReply, _>`: тело обработчика требует поддержки запроса и ответа и не называет брокер.
Приложение указывает для слота обычную политику `Publish`. Её издатель, [`AmqpPublisher`](../components/publishing/index.md), поддерживает запрос и ответ.
[`request`](../components/request-reply/index.md) присоединяет динамическую связь получателя, поэтому брокер создаёт закрытый адрес для ответа на этот единственный запрос.
Он публикует вопрос с заданными `reply-to` и `correlation-id` и возвращает первый совпавший ответ.
ActiveMQ Artemis создаёт для динамической связи временную очередь.
Вопрос публикуется как сообщение из байтов, без кодека, поэтому тело обработчика само кодирует его и декодирует ответ, здесь через `serde_json`.
Ответ на заказ, `Priced`, уходит, как только цена известна.

Запрос, на который никто не ответил, возвращает ошибку [`AmqpError::RequestTimeout`](../components/broker/errors.md) по истечении тайм-аута, здесь двух секунд.
Связь для ответа отсоединяется в любом случае.
Затем заказ ждёт 30 секунд в своей очереди и спрашивает снова.

## Приложение {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#app"
```

</TabItem>
</Tabs>

Оба слота принимают обычную политику `Publish`: один публикует ответы, другой задаёт вопросы.
У запроса, который начинает разговор, нет доставки, на которую он отвечает.
Он выполняется из хука запуска `b.after_startup(Publish, ..)` вызовом `request` на издателе, которого получает хук.

Тесты в файле примера запускают обе половины в одном приложении внутри процесса.
Брокер внутри процесса направляет запрос собственному отвечающему сервису приложения, сопоставляет ответ по `correlation-id` и завершает по тайм-ауту запрос к молчащему сервису.
