---
title: Подтверждение и гарантии доставки
sidebar_position: 3
description: "Урегулирование доставок AMQP 1.0 через accept, modified и reject, предел попыток, чтение at-most-once, кредит связи и пакеты."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Подтверждение и гарантии доставки

Это руководство урегулирует доставки, ограничивает число попыток, читает очередь в режиме at-most-once, выбирает кредит связи и обрабатывает пакеты.

В AMQP 1.0 получатель урегулирует каждую доставку исходом, и брокер действует по нему.
Исход обработчика и есть эта диспозиция.

Пример - склад.
Он комплектует отгрузки, записывает каждую комплектацию в журнал и пополняет полки из поставок, которые приходят пакетами.

## Три исхода {/* #the-three-outcomes */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#pick"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#pick"
```

</TabItem>
</Tabs>

| Исход обработчика | Диспозиция | Что делает брокер |
|---|---|---|
| `HandlerOutcome::ack()` | `accepted` | удаляет сообщение |
| `HandlerOutcome::retry()` | `modified` с `delivery-failed` | учитывает попытку в `delivery-count` и доставляет сообщение повторно |
| `HandlerOutcome::drop()` | `rejected` | применяет свою политику недоставленных сообщений |

`pick` принимает отгрузку, которую скомплектовал.
Пустую отгрузку скомплектовать невозможно, поэтому `pick` её отклоняет.
Artemis переносит отклонённое сообщение в адрес недоставленных сообщений, по умолчанию `DLQ`.
Отгрузку товаров, которых нет на складе, можно скомплектовать позже, поэтому `pick` просит доставить её снова.
Artemis прекращает доставлять сообщение после [`max-delivery-attempts`](https://artemis.apache.org/components/artemis/documentation/latest/undelivered-messages.html) попыток, по умолчанию 10, и переносит его в адрес недоставленных сообщений.

## Предел попыток {/* #capping-the-attempts */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) ограничивает доставки одной отгрузки пятью, включая первую.
Пятая доставка отклоняется, а не повторяется.
Число попыток - это `delivery-count` брокера, если у сообщения есть секция AMQP `header`.
У сообщения, которое публикует этот крейт, её нет.
Для такого сообщения фреймворк считает попытки сам: `retry` публикует копию обратно в `shipments` с номером повтора в заголовке.
Руководство [Повторы и недоставленные сообщения](retries.md) добавляет адрес недоставленных сообщений и задержку.

## Сбой посреди обработки {/* #a-crash-in-the-middle */}

По умолчанию подписки работают в режиме at-least-once.
Доставка, которую сервис не урегулировал к моменту остановки, возвращается в очередь, и её получает следующий потребитель.
Поэтому обработчик может увидеть одно сообщение дважды, и его работа должна это выдерживать.

## Не более одного раза {/* #at-most-once */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#audit"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#audit"
```

</TabItem>
</Tabs>

[`Settle::AtMostOnce`](api:ruststream_amqp::Settle::AtMostOnce) урегулирует каждую доставку при получении.
Брокер забывает сообщение в тот момент, когда отдаёт его, поэтому сбой до завершения обработчика его теряет.
Это подходит журналу комплектаций: пропущенная строка обходится дешевле второго прохода по брокеру.
Исходу обработчика урегулировать уже нечего: `ack` и `nack` возвращают `AckError::Unsupported`, и рантайм пишет предупреждение в журнал для каждого.

## Кредит {/* #credit */}

`.credit(nonzero!(32))` на `shipments` - кредит связи: сколько доставок брокер может отправить вперёд, не дожидаясь урегулирования от сервиса.
По умолчанию это [`DEFAULT_CREDIT`](api:ruststream_amqp::DEFAULT_CREDIT), 256.
Когда урегулирована половина кредита, клиент снова выдаёт полный кредит.
Поэтому медленный обработчик сдерживает брокер, а не заполняет память сервиса, и остаток очереди остаётся на брокере для других реплик.
Кредит имеет тип `NonZeroU32`, потому что связь без кредита ничего не получает: `credit(nonzero!(0))` не компилируется.

## Пакеты {/* #batches */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/acknowledgement.rs#restock"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/acknowledgement.rs#restock"
```

</TabItem>
</Tabs>

Обработчик, который принимает срез, получает пакет, а `.batch(nonzero!(20usize))` в точке монтирования задаёт его наибольший размер.
В AMQP 1.0 одна передача содержит одно сообщение, поэтому пакет собирает клиент.
Пакет содержит не больше 20 доставок, а меньше - если больше не пришло вовремя.
[`batch_wait`](api:ruststream_amqp::AmqpAddress::batch_wait) - сколько неполный пакет ждёт новых доставок: здесь 50 миллисекунд, по умолчанию 10.
Исход `restock` урегулирует все доставки пакета.

## Тестирование исходов {/* #testing-the-outcomes */}

Тесты в файле примера запускают склад внутри процесса.
Они проверяют каждый исход, пять доставок отгрузки товара, которого нет на складе, журнал комплектаций и пакеты.
Кредит, политика недоставленных сообщений брокера и его предел доставок принадлежат Artemis, поэтому брокер внутри процесса их не моделирует.
Руководство [Тестирование сервиса на AMQP](testing.md) разбирает обвязку.
