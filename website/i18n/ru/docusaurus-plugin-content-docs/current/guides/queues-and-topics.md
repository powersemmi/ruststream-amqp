---
title: Очереди и темы
sidebar_position: 2
description: "Чтение рабочей очереди, рассылка по теме, рассылка, которая ждёт отсутствующего читателя, и публикация в адрес по AMQP 1.0."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Очереди и темы

Это руководство читает рабочую очередь, делает рассылку по теме, сохраняет рассылку для читателя, которого нет на месте, и публикует в адрес.

AMQP 1.0 задаёт формат обмена, а смысл адреса оставляет брокеру.
Поэтому подписка сама говорит, что ей нужно. У [`AmqpAddress`](../components/subscriptions/index.md) есть конструктор для каждого намерения, и связь сообщает брокеру, какое из них выбрано.
Всё, что утверждает эта страница, проверено на ActiveMQ Artemis, на котором работают тесты крейта.

Пример - сервис заказов.
Он берёт заказы из очереди и объявляет о каждом принятом заказе в теме, на которую подписаны биллинг, журнал аудита и бухгалтерская книга.

## Рабочая очередь {/* #a-work-queue */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/queues_and_topics.rs#queue"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/queues_and_topics.rs#queue"
```

</TabItem>
</Tabs>

[`AmqpAddress::queue`](../components/subscriptions/queue.md) запрашивает очередь: связь передаёт возможность терминуса `queue`.
Artemis создаёт адрес anycast `orders` с одной очередью за ним.
Потребители очереди конкурируют, и каждый заказ достаётся одному из них.
Чтобы принимать заказы быстрее, запустите больше реплик сервиса: каждая реплика - ещё один потребитель `orders`.

`take_order` отвечает на каждый заказ сообщением `OrderTaken`.
`OrderTaken` называет свой адрес через `#[outgoing(name = "order-events")]`, поэтому аргумент `reply` макроса без имени отправляет его туда.

## Рассылка {/* #a-broadcast */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/queues_and_topics.rs#topic"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/queues_and_topics.rs#topic"
```

</TabItem>
</Tabs>

[`AmqpAddress::topic`](../components/subscriptions/topic.md) запрашивает тему: связь передаёт возможность `topic`.
Artemis делает `order-events` адресом multicast и даёт каждой подписке собственную очередь.
Поэтому `bill` и `audit` получают каждый свою копию каждого `OrderTaken`.

Подписка на тему существует, пока существует её связь.
То, что опубликовано в `order-events`, пока сервис остановлен, для него не сохраняется.
Читатель, которому нельзя пропустить сообщение, читает очередь.

## Рассылка, которая ждёт читателя {/* #a-broadcast-that-waits-for-its-reader */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/queues_and_topics.rs#durable"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/queues_and_topics.rs#durable"
```

</TabItem>
</Tabs>

Бухгалтерская книга должна увидеть каждый заказ, в том числе принятые, пока она была остановлена.
Оператор создаёт долговечную очередь `ledger` на адресе multicast `order-events`:

```bash
docker compose exec artemis /opt/activemq-artemis/bin/artemis queue create \
  --user artemis --password artemis --silent \
  --name ledger --address order-events --auto-create-address \
  --multicast --durable --preserve-on-no-consumers
```

Теперь каждое сообщение, опубликованное в `order-events`, попадает и в `ledger`, независимо от того, работает ли `post_to_ledger`.
`order-events::ledger` - полное имя очереди в Artemis: адрес, два двоеточия, очередь.
[`AmqpAddress::raw`](../components/subscriptions/raw.md) отправляет адрес как написан, без возможности, поэтому связь присоединяется к этой очереди.
Строковая форма `#[subscriber("order-events::ledger")]` делает то же самое.

## Приложение {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/queues_and_topics.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/queues_and_topics.rs#app"
```

</TabItem>
</Tabs>

`.out_reply(Publish)` называет издателя ответов - [`AmqpPublish`](../components/publishing/index.md) под именем из [прелюдии](../components/broker/prelude.md).
При первой публикации издатель присоединяет связь отправителя к адресу ответа и дальше держит её.
Если брокер отказывается принять сообщение, публикация возвращает ошибку [`AmqpError::PublishNotAccepted`](../components/broker/errors.md).

## Заголовки и другие клиенты {/* #headers-and-other-clients */}

Общеизвестные [заголовки](../components/publishing/headers.md) передаются в секции AMQP `properties`: `content-type`, `correlation-id`, `reply-to`, `message-id`.
Все остальные заголовки передаются в `application-properties`.
Сообщение этого крейта - обычное сообщение AMQP, поэтому клиент на другом языке читает его без RustStream.

Ключ партиционирования передаётся заголовком [`PARTITION_KEY_HEADER`](../components/publishing/keys.md) и становится полем `group-id` сообщения.
Artemis использует `group-id` для [группировки сообщений](https://artemis.apache.org/components/artemis/documentation/latest/message-grouping.html): все сообщения одной группы достаются одному потребителю очереди.
Поэтому заказы с ключом по клиенту сохраняют свой порядок на нескольких репликах.

## Адреса в других брокерах {/* #addresses-on-other-brokers */}

Другие брокеры называют свои узлы по-своему, и `raw` передаёт их запись как есть.
Её описывает собственная документация брокера:
[RabbitMQ 4.x](https://www.rabbitmq.com/docs/amqp),
[Azure Service Bus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-protocol-guide),
[ActiveMQ Classic](https://activemq.apache.org/components/classic/documentation/amqp), на котором работает [Amazon MQ](https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/working-with-activemq.html).

## Тестирование маршрутизации {/* #testing-the-routing */}

Тесты в файле примера запускают приложение внутри процесса.
Один заказ публикует один `OrderTaken`, обработчик очереди `orders` вызывается один раз, а `order-events` - дважды, по разу на каждую подписку на тему.
Брокер внутри процесса доставляет адрес `raw` точно в том виде, в каком он написан, поэтому его тест публикует прямо в `order-events::ledger`.
Руководство [Тестирование сервиса на AMQP](testing.md) разбирает обвязку.
