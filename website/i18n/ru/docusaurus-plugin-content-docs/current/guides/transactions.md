---
title: Транзакции
sidebar_position: 6
description: "Публикация нескольких сообщений AMQP 1.0 из одного обработчика вместе или никак: транзакционная отправка и прерывание, которое ничего не публикует."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Транзакции

Это руководство публикует несколько сообщений из одного обработчика вместе или не публикует ни одного.

Транзакции входят в протокол AMQP 1.0: отправитель передаёт сообщения внутри транзакции, и брокер делает их видимыми при фиксации.
Их включает фича `transaction`:

```toml title="Cargo.toml"
[dependencies]
ruststream-amqp = { version = "0.7", features = ["transaction"] }
```

Пример - сервис биллинга.
Он превращает каждый заказ в проводки и счёт, и до брокера доходят либо все они, либо ни одна.

## Обработчик {/* #the-handler */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/transactions.rs#bill"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/transactions.rs#bill"
```

</TabItem>
</Tabs>

Слот `Billing` ограничен как `Out<impl TransactionalPublisher, _>`: тело обработчика требует транзакций и не называет брокер.
[`begin_transaction`](api:ruststream::TransactionalPublisher::begin_transaction) открывает транзакцию на издателе слота.
Каждая публикация после этого отправляется внутри транзакции, и брокер придерживает её.
[`commit`](api:ruststream::TransactionalPublisher::commit) делает проводки и счёт видимыми одновременно.
По заказу со строкой без цены нельзя выставить счёт, и [`abort`](api:ruststream::TransactionalPublisher::abort) отбрасывает отправленное, поэтому ничего не доходит до `ledger` и `invoices`.

Фиксация или прерывание, вернувшие ошибку, всё равно завершают транзакцию, поэтому следующий `begin_transaction` начинает новую.
Если фиксация вернула ошибку, `bill` повторяет заказ.

## Приложение {/* #the-app */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/transactions.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/transactions.rs#app"
```

</TabItem>
</Tabs>

`TransactionalPublish` - политика с поддержкой транзакций, [`AmqpTransactionalPublish`](api:ruststream_amqp::AmqpTransactionalPublish) под именем из прелюдии.
Её издатель держит одну транзакцию за раз.
Второй `begin_transaction` при открытой транзакции возвращает ошибку и не трогает открытую.
Каждая регистрация получает собственного издателя, поэтому два обработчика могут держать по транзакции одновременно.
Публикация на этом издателе вне транзакции уходит сразу.

Транзакция, которая начинает работу сервиса и не обрабатывает никакую доставку, выполняется из хука запуска: `b.after_startup(TransactionalPublish, ..)` передаёт хуку того же издателя.

## Доставка самого заказа {/* #the-orders-own-delivery */}

Транзакция охватывает то, что публикует обработчик.
Доставка заказа урегулируется после возврата из обработчика, вне транзакции.
Сбой между фиксацией и урегулированием доставляет заказ повторно, и его проводки и счёт публикуются второй раз.
Дайте сообщениям идентификатор, по которому читатели отбросят повтор, например номер заказа, как здесь.

## Тестирование транзакции {/* #testing-the-transaction */}

Тесты в файле примера запускают сервис внутри процесса.
Брокер внутри процесса ничего не публикует до фиксации и отбрасывает сообщения прерванной транзакции, поэтому тесты читают исход по тому, что опубликовано.
Руководство [Тестирование сервиса на AMQP](testing.md) разбирает обвязку.
