---
title: Design
sidebar_position: 2
description: "Where ruststream-amqp sits: between the RustStream runtime and the fe2o3-amqp client, a session per subscription and a shared session for publishers."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the RustStream runtime, this crate, the fe2o3-amqp client and the broker, top to bottom",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream core",
  "runtime": "Runtime: dispatch, lanes, settlement",
  "crate": "ruststream-amqp",
  "subscriber": ["AmqpSubscriber", "pump task, channel"],
  "publishers": ["AmqpPublisher,", "AmqpTxnPublisher"],
  "message": "AmqpMessage",
  "request": "Request and reply",
  "broker": ["AmqpBroker", "URL, Sasl, container"],
  "descriptor": ["AmqpAddress", "queue, topic, raw"],
  "client": "fe2o3-amqp: one connection",
  "receiver": ["A session and a receiver", "per subscription"],
  "sender": ["One session, a sender", "link per address"],
  "server": "AMQP 1.0 broker",
  "mounted": "mounted",
  "polls": "polls the stream"
};

# Design

Where `ruststream-amqp` sits: between the RustStream runtime and the `fe2o3-amqp` client, a session
per subscription and a shared session for publishers.

<Diagram name="design" words={words} />

The crate implements the core's broker traits over
[`fe2o3-amqp`](https://docs.rs/fe2o3-amqp/0.18.2/fe2o3_amqp/). The runtime above it is the same for
every broker. What is particular to AMQP 1.0 is in this crate: the
[address descriptor](../components/subscriptions/index.md),
[what an ack means](../components/settlement/index.md),
[where a retry copy goes](../components/settlement/retry-after.md) and
[how a transaction posts](../components/transactions/index.md).

A connected broker holds one connection. Its [publishers](../components/publishing/index.md) share
one session, and the first publish to an address attaches a sender link that every later publish to
that address reuses. The connection owns those links, so a shutdown can close each one and wait for
the broker's answer. Each subscription begins a session of its own and attaches one receiver link on
it.

Subscriptions and their sessions start on the runtime the broker connected on, whichever thread
opens them. A handler on a thread of its own therefore leaves no part of the connection on that
thread.

The crate opens the TCP socket itself, with Nagle's algorithm off. A publish and a request each
send a small frame and wait for the answer, which is the pattern Nagle's algorithm delays until the
peer's acknowledgement.

A message carries no envelope of the framework's own. The well-known headers ride the
`properties` section, the partition key as `group-id`, and every other header rides
`application-properties`. A message from another AMQP client arrives with its headers, and one
this crate sent reads as a plain AMQP message.

The `asyncapi` feature describes the addresses with `x-ruststream-amqp1` objects, because the
specification reserves its `amqp1` bindings and leaves them empty.

How the pieces start and stop is on [Lifecycle](lifecycle.md), and the way of one message on
[Path](path.md).

## The in-process transport

With the `testing` feature, [`TestApp::start`](../components/testing/index.md) connects
the same broker to an in-process transport instead of a server. The descriptors, the policies and
the deliveries are the production ones, and a publish goes through the same conversion into AMQP
sections. It routes by exact address: the queue receivers of an address compete, and each topic
subscription gets a copy. A queue subscription creates its queue, which keeps a message until a
receiver takes it. It settles in the subscription's mode, counts a `modified` delivery in
`delivery-count`, returns an unsettled delivery to the queue when its subscription detaches, and
answers a request over a private reply address. A transaction publishes nothing before its commit.

What is the server's own is left to a [live run](../components/testing/live.md): link credit, how a
server creates an address a publish reaches first, the dead-letter policy behind a rejection, the
server's own delivery limit and durability. In process a message to an address no queue subscription
has attached to is dropped, and a `raw` address is delivered once, where Artemis may fan it out.

Without the feature the transport is not compiled. A production build's connected broker is one
shared pointer to the connection, and the compiler checks that size.
