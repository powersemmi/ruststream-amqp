---
title: Повторы и недоставленные сообщения
sidebar_position: 4
description: "Повтор доставки AMQP 1.0 сразу или позже, предел попыток, перенос того, что раз за разом не обрабатывается, в адрес недоставленных сообщений, и пометка копий."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Повторы и недоставленные сообщения

Это руководство повторяет доставку сразу или позже, ограничивает число попыток, переносит то, что раз за разом не обрабатывается, в адрес недоставленных сообщений и помечает копии.

В AMQP 1.0 нет отложенной повторной доставки, а связь не может объявить предел доставок или адрес недоставленных сообщений.
И то и другое делает фреймворк: точка монтирования объявляет предел и адрес недоставленных сообщений, а отложенный повтор публикуется как копия сообщения с номером повтора в заголовке.

Пример - сервис возвратов, который проводит возвраты через процессинг карт.

## Подписчик {/* #the-subscriber */}

Процессинг отказывает в возврате тремя способами, и каждому нужен свой исход.
`Busy` значит, что крупные возвраты он пока не принимает. Повтор прямо сейчас снова получит отказ, поэтому [`retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after) ждёт 30 секунд.
`Unavailable` значит, что банк карты не ответил: [`retry`](api:ruststream::runtime::HandlerOutcome::retry) пробует снова сразу.
`Declined` значит, что карточный счёт закрыт. Новая попытка получит тот же ответ, поэтому возврат отбрасывается.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

`retry_after` урегулирует доставку и по истечении задержки публикует копию в `refunds`.
В копии заголовок с номером повтора ([`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)) увеличен на единицу.
Отложенная копия доставляется at-most-once: процесс, который остановился до конца задержки, её теряет.
В очереди копия достаётся одному потребителю, как любое другое сообщение.

## Предел и адрес недоставленных сообщений {/* #the-cap-and-the-dead-letter-address */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) считает все доставки одного возврата, включая первую.
Пятая доставка не повторяется: [`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) публикует её в `refunds.dead` с полезной нагрузкой и заголовками в том виде, в каком они пришли.
Отброшенный возврат отклоняется и попадает под собственную политику недоставленных сообщений брокера, а не в `refunds.dead`.

При объявленном пределе фреймворк читает число попыток из доставки.
Это `delivery-count` брокера, если у сообщения есть секция AMQP `header`.
У сообщения, которое опубликовал этот крейт, её нет, поэтому `retry` в этом случае сразу публикует копию, и попытки считает заголовок с номером повтора.

Предел без `dead_letter` вместо этого отклоняет последнюю доставку, и дальше решает политика недоставленных сообщений брокера.
У брокера есть и собственный предел: Artemis останавливается после [`max-delivery-attempts`](https://artemis.apache.org/components/artemis/documentation/latest/undelivered-messages.html), по умолчанию 10.
Выберите одного владельца предела: при пределе ниже брокерского действует счёт фреймворка.

## Чтение недоставленных сообщений {/* #reading-the-dead-letters */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`refunds.dead` - обычная очередь, и `review` читает её, как любую другую.
Заголовок с номером повтора у недоставленного сообщения хранит число доставок, через которые оно прошло, здесь 5.
`review` читает его через `ctx.headers()` и пишет возврат в журнал, чтобы его посмотрел человек.

## Пометка копий {/* #marking-the-copies */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#stamp"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#stamp"
```

</TabItem>
</Tabs>

Копию в обработчике не публикует ни один вызов, поэтому добраться до неё может только [преобразование публикации](api:ruststream::runtime::PublishTransform).
Приложение монтирует `process_refund` через `.out_retry(Publish).transform(Retried)`.
`.out_retry(Publish)` называет издателя копий, и шаги после него применяются к ним.
`Retried` записывает в `x-retried-from` адрес, из которого пришла доставка.
Каждая копия возвращается в адрес своей подписки, поэтому преобразование в этой позиции не может менять адрес назначения.

## Повторы в теме {/* #retries-on-a-topic */}

Копия публикуется в собственный адрес подписки.
В [адресе темы](queues-and-topics.md#a-broadcast) этот адрес общий, поэтому копию получает каждая подписка на тему, а не только та, что просила повтор.
Повторяйте через очередь, когда сообщение принадлежит одному читателю.

## Проверка задержки {/* #testing-the-delay */}

Тесты в файле примера запускают приложение внутри процесса с остановленными часами, поэтому `tb.advance(BACKOFF)` проматывает 30 секунд сразу.
Руководство [Тестирование сервиса на AMQP](testing.md) разбирает обвязку.
