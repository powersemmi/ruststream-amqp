---
title: Транзакции AMQP
sidebar_position: 3
description: "Транзакция AMQP 1.0 объявляется у координатора брокера, и передачи, отправленные в ней, доходят до своих узлов вместе при фиксации или не доходят вовсе."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "Транзакционная отправка: declare у координатора, отправка под txn-id и discharge для фиксации или отката",
  "text": "Схема текстом",
  "declare": ["declare", "по управляющей связи"],
  "declared": ["declared", "txn-id координатора"],
  "post": ["передачи по отправляющим", "связям с txn-id"],
  "commit": ["discharge fail=false", "фиксация"],
  "visible": ["accepted:", "сообщения доходят", "до узлов вместе"],
  "rollback": ["discharge", "fail=true:", "откат,", "ничего не", "доставлено"],
  "hidden": ["До discharge ни один потребитель не видит", "сообщения, отправленного в транзакции"],
  "coordinator": "координатор",
  "txnId": "txn-id",
  "accepted": "accepted",
  "orElse": "или"
};

# Транзакции AMQP

Транзакция AMQP 1.0 объявляется у координатора брокера, и передачи, отправленные в ней, доходят до
своих узлов вместе при фиксации или не доходят вовсе.

<Diagram name="transactions" words={words} />

Координатор - это узел брокера. К нему присоединяется управляющая связь (controller) и отправляет
`declare`, а координатор отвечает идентификатором транзакции. Затем передача по любой обычной
отправляющей связи указывает этот идентификатор в своём транзакционном состоянии. `discharge`
завершает транзакцию: с `fail` false она фиксируется, с `fail` true откатывается.

[`AmqpTxnPublisher`](api:ruststream_amqp::AmqpTxnPublisher) отправляет сообщения в транзакции.
Каждый вызов `begin_transaction` присоединяет управляющую связь на сеансе издателей соединения и
объявляет одну транзакцию, а `commit` и `abort` её завершают. Управляющая связь запрашивает режим
урегулирования отправителя `mixed`, потому что Artemis отвечает этим режимом, а клиент требует,
чтобы брокер повторил запрошенный режим.

Здесь брокеры различаются. Artemis обслуживает одну транзакцию на сеанс, а издатели одного
соединения делят один сеанс. RabbitMQ 4 не поддерживает транзакции AMQP 1.0. Azure Service Bus их
поддерживает и отправляет сообщения в несколько сущностей в одной транзакции через сущность
send-via.

Протокол описан в спецификации
([транзакции](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html)),
а ограничения брокеров - у каждого поставщика
([Artemis](https://artemis.apache.org/components/artemis/documentation/latest/amqp.html),
[RabbitMQ](https://www.rabbitmq.com/docs/amqp),
[Service Bus](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-protocol-guide)).
