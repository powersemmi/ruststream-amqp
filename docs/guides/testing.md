---
sidebar_position: 7
description: "Test an AMQP 1.0 service in process with TestApp: the production app, replies, delays, dead letters, and the same test against a broker."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing an AMQP service

A test runs the service's own app with [`TestApp`](api:ruststream::testing::TestApp), against an in-process broker or against a running one.

The service under test charges card payments.
A payment arrives on the queue `card-payments`, and the handler replies with a receipt on `card-receipts`.
A payment without a card is dropped.
A payment over the bank's instant limit is held: the handler asks for it again in a minute with `retry_after`.
After three deliveries a payment goes to `card-payments.review`, the dead-letter address.
The app is built by `service(broker)`, so a test chooses the broker's URL and `main` uses the production one.
The tests need the `testing` feature of `ruststream-amqp` in `[dev-dependencies]`.

## A payment and its receipt

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) connects [`AmqpBroker`](api:ruststream_amqp::AmqpBroker) to an in-process broker.
The broker's URL is never dialled.
The test addresses the broker by its production type, `tb.broker::<AmqpBroker>()`.
`publish` returns once the handler and its reply have settled.
So the next lines assert on finished state: how the subscriber settled and what reached `card-receipts`.
The body takes the started app and the payment's id as arguments, so the live test below runs it too.

## A payment without a card

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#cardless"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#cardless"
```

</TabItem>
</Tabs>

`settled` checks the outcome the handler returned.
`assert_not_called` checks that no receipt was published.

## A delay, without waiting for it

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

The test starts on a paused clock, and [`advance`](api:ruststream::testing::TestApp::advance) moves it forward.
A minute of `HOLD` costs nothing.
AMQP 1.0 has no delayed redelivery, so `retry_after` publishes a copy of the payment to its queue when the delay ends.
The copy carries the retry count in a header, and `published` shows it on `card-payments` next to the test's own message.
The third delivery is the last one the mount site allows, so the payment goes to `card-payments.review` as it arrived.
Each held delivery is recorded as `Outcome::Nack`.

## What the in-process broker models

The in-process broker reads the broker's URL the way `connect` does.
A URL the client cannot open fails the test's start, and `amqps://` needs the `rustls` or `native-tls` feature there too.
A publish is turned into an AMQP message and back, so a handler reads the headers a live delivery carries.
It models the broker's routing:

- Consumers of a `queue` address compete, one delivery each; every `topic` subscription gets a copy.
- A `raw` address is delivered exactly as written, to one consumer.
- A queue subscription creates its queue, and the queue keeps what arrives while no consumer is attached.
- `retry` counts the attempt in `delivery-count`, and a delivery left unsettled goes back to the queue when its subscription ends.
- An at-most-once subscription gets its deliveries settled, and `ack` reports `AckError::Unsupported`.
- A transaction publishes nothing before its commit, and a request times out when nothing answers.

Link credit, the broker's dead-letter policy and delivery limit, durability, and the addresses a broker creates for a publish need a running broker.
In process, a message to an address that no queue subscription has opened is dropped.

## The same test on a broker

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) connects the same app to a running broker.
The test reads the URL from `AMQP_TEST_URL` with `live::url`, a helper of the example's own, and skips when it is unset.
The payment's id is the process id, so the messages of one run are told apart on the broker.
The assertions are the in-process ones, unchanged.

```bash
cargo test
AMQP_TEST_URL=amqp://artemis:artemis@localhost:5672 cargo test -- --test-threads=1
```
