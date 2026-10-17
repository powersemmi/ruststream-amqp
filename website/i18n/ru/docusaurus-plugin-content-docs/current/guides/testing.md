---
title: Тестирование сервиса на AMQP
sidebar_position: 7
description: "Тестирование сервиса на AMQP 1.0 внутри процесса через TestApp: рабочее приложение, ответы, задержки, недоставленные сообщения и тот же тест на брокере."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Тестирование сервиса на AMQP

Тест запускает собственное приложение сервиса через [`TestApp`](api:ruststream::testing::TestApp) на брокере внутри процесса или на работающем брокере.

Тестируемый сервис списывает платежи по картам.
Платёж приходит в очередь `card-payments`, и обработчик отвечает квитанцией в `card-receipts`.
Платёж без карты отбрасывается.
Платёж сверх мгновенного лимита банка откладывается: обработчик просит доставить его снова через минуту с `retry_after`.
После трёх доставок платёж уходит в `card-payments.review`, адрес недоставленных сообщений.
Приложение строит `service(broker)`, поэтому тест выбирает URL брокера сам, а `main` берёт рабочий.
Тестам нужна фича `testing` крейта `ruststream-amqp` в `[dev-dependencies]`.

## Платёж и квитанция {/* #a-payment-and-its-receipt */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) подключает [`AmqpBroker`](api:ruststream_amqp::AmqpBroker) к брокеру внутри процесса.
Соединение по URL брокера не устанавливается.
Тест обращается к брокеру по его рабочему типу: `tb.broker::<AmqpBroker>()`.
`publish` возвращает управление, когда обработчик и его ответ урегулированы.
Поэтому следующие строки проверяют уже готовое состояние: как подписчик урегулировал доставку и что дошло до `card-receipts`.
Тело теста принимает запущенное приложение и номер платежа аргументами, поэтому его же выполняет и тест на брокере ниже.

## Платёж без карты {/* #a-payment-without-a-card */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#cardless"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#cardless"
```

</TabItem>
</Tabs>

`settled` проверяет исход, который вернул обработчик.
`assert_not_called` проверяет, что квитанция не опубликована.

## Задержка без ожидания {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

Тест начинается с остановленными часами, а [`advance`](api:ruststream::testing::TestApp::advance) сдвигает их вперёд.
Минута `HOLD` не стоит ничего.
В AMQP 1.0 нет отложенной повторной доставки, поэтому по окончании задержки `retry_after` публикует копию платежа в его очередь.
В копии номер повтора хранится в заголовке, и `published` показывает её в `card-payments` рядом с собственным сообщением теста.
Третья доставка - последняя, которую допускает точка монтирования, поэтому платёж уходит в `card-payments.review` в том виде, в каком пришёл.
Каждая отложенная доставка записывается как `Outcome::Nack`.

## Что моделирует брокер внутри процесса {/* #what-the-in-process-broker-models */}

Брокер внутри процесса читает URL брокера так же, как `connect`.
URL, который клиент не может открыть, не даёт тесту запуститься, и `amqps://` там тоже требует фичи `rustls` или `native-tls`.
Публикация превращается в сообщение AMQP и обратно, поэтому обработчик читает те же заголовки, что и при доставке с работающего брокера.
Брокер внутри процесса моделирует маршрутизацию брокера:

- Потребители адреса `queue` конкурируют, и каждое сообщение доставляется одному из них; каждая подписка `topic` получает копию.
- Адрес `raw` доставляется точно в том виде, в каком написан, одному потребителю.
- Подписка на очередь создаёт свою очередь, и очередь хранит то, что приходит, пока к ней не присоединён ни один потребитель.
- `retry` учитывает попытку в `delivery-count`, а неурегулированная доставка возвращается в очередь, когда её подписка завершается.
- Подписка at-most-once получает доставки уже урегулированными, и `ack` возвращает `AckError::Unsupported`.
- Транзакция ничего не публикует до фиксации, а запрос завершается по тайм-ауту, когда никто не отвечает.

Для кредита связи, политики недоставленных сообщений и предела доставок брокера, долговечности и адресов, которые брокер создаёт при публикации, нужен работающий брокер.
Внутри процесса сообщение в адрес, на который не открыта ни одна подписка на очередь, отбрасывается.

## Тот же тест на брокере {/* #the-same-test-on-a-broker */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) подключает то же приложение к работающему брокеру.
Тест читает URL из `AMQP_TEST_URL` через `live::url`, собственную вспомогательную функцию примера, и пропускается, если переменная не задана.
Номер платежа - идентификатор процесса, поэтому сообщения разных прогонов на брокере не смешиваются.
Проверки те же, что и внутри процесса, без изменений.

```bash
cargo test
AMQP_TEST_URL=amqp://artemis:artemis@localhost:5672 cargo test -- --test-threads=1
```
