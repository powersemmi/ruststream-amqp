---
slug: /
sidebar_position: 0
description: "Руководства по сервисам на AMQP 1.0 с ruststream-amqp, брокером AMQP 1.0 для RustStream."
---

# Руководства

Крейт `ruststream-amqp` реализует брокер AMQP 1.0 для [RustStream](https://powersemmi.github.io/ruststream/latest/ru/).
Он построен на клиенте [fe2o3-amqp](https://docs.rs/fe2o3-amqp). AMQP 1.0 - стандарт ISO, поэтому
один крейт обслуживает всё семейство брокеров: ActiveMQ Artemis и Classic, RabbitMQ 4.x, Azure Service Bus и
Event Hubs, Amazon MQ, Solace, Apache Qpid и IBM MQ.

В RabbitMQ протокол AMQP 1.0 реализован отдельно от AMQP 0.9.1, на котором работает
[`ruststream-lapin`](https://powersemmi.github.io/ruststream-lapin/).
