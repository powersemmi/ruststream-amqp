---
sidebar_position: 5
description: "Ask another service over AMQP 1.0 and wait for its answer, answer requests on their reply address, and handle a request nobody answers."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Request-reply

This guide asks another service over AMQP 1.0 and waits for its answer, answers requests on their reply address, and handles a request nobody answers.

Request-reply is part of the protocol.
A request carries a reply address in `reply-to` and an id in `correlation-id`.
The answer goes to that address and repeats that id.

The example is two services in one app.
The pricing service answers questions about prices, and the orders service asks it for the price of each order.

## The responder

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#responder"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#responder"
```

</TabItem>
</Tabs>

A request arrives with its reply address in the `reply-to` header, and `ctx.headers().reply_to()` reads it.
`quote` publishes the price through its `Answers` slot with `to(reply_to)`, because the address is new for every request.
`Price` therefore declares no address of its own.
The answer carries the request's `correlation-id`, and the requester takes only an answer whose id matches.
An item without a price gets no answer, and the requester's timeout ends the wait.

The responder reads the queue `prices.quote`.
Each replica of the pricing service is one more consumer of it, and each request goes to one of them.

## The requester

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#requester"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#requester"
```

</TabItem>
</Tabs>

`price_order` reads orders and asks for each price on `prices.quote`.
The `Pricing` slot is bounded `Out<impl RequestReply, _>`: the body says it needs request-reply and names no broker.
The app pairs the slot with the plain `Publish` policy, whose publisher, [`AmqpPublisher`](api:ruststream_amqp::AmqpPublisher), carries request-reply.
[`request`](api:ruststream::RequestReply::request) attaches a dynamic receiver link, so the broker creates a private reply address for this one request.
It publishes the question with `reply-to` and `correlation-id` set and resolves with the first matching answer.
ActiveMQ Artemis backs a dynamic link with a temporary queue.
The question is a raw message, so the body encodes it and decodes the answer itself, here with `serde_json`.
The order's reply, `Priced`, goes out once the price is known.

A request nobody answers fails after its timeout, two seconds here, with [`AmqpError::RequestTimeout`](api:ruststream_amqp::AmqpError::RequestTimeout).
The reply link is detached either way.
The order then waits 30 seconds on its queue and asks again.

## The app

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/request_reply.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/request_reply.rs#app"
```

</TabItem>
</Tabs>

Both slots take the plain `Publish` policy: one publishes answers, the other asks questions.
A request that starts a conversation has no delivery to answer.
It runs from a startup hook, `b.after_startup(Publish, ..)`, with `request` on the publisher the hook is handed.

The tests in the example file run both halves in one app in process.
The in-process broker routes a request to the app's own responder, matches the answer by its correlation id, and times a silent responder out.
