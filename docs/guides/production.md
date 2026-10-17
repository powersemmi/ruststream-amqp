---
sidebar_position: 8
description: "Connect an AMQP 1.0 service to a production broker: TLS, SASL credentials from the environment, the container id, shutdown, and a test of the configuration."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Connecting to production

A production broker asks for TLS and credentials, and its operators look for a service by name.

## TLS and credentials

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

```toml title="Cargo.toml"
[dependencies]
ruststream-amqp = { version = "0.7", features = ["rustls"] }
```

The deployment provides the address and the credentials in environment variables.
An `amqps://` URL connects over TLS.
It names the broker by its host name, not an IP address: TLS checks that name against the broker's certificate.
TLS needs one of two features: `rustls` uses [rustls](https://docs.rs/rustls), and `native-tls` uses the platform's TLS.

[`Sasl::plain`](api:ruststream_amqp::Sasl::plain) logs the service in with a user name and a password.
The URL then carries no credentials.
A password with characters that have a meaning in a URL, such as `/`, `?` or `@`, goes through `Sasl::plain` as it is.
[`Sasl::anonymous`](api:ruststream_amqp::Sasl::anonymous) connects without credentials, and [`Sasl::external`](api:ruststream_amqp::Sasl::external) uses a credential the connection already established outside SASL.

On Azure Service Bus the user name is the name of a shared access policy and the password is its key, as its [AMQP guide](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-amqp-protocol-guide) describes.

## The service

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

[`container_id`](api:ruststream_amqp::AmqpBroker::container_id) names the service to the broker, `ruststream` by default.
The broker's console lists the connection under that name, and the links of the service are named after it.
Give each deployment of a service its own container id, so an operator tells them apart.

Each subscription runs on an AMQP session of its own, and the publishers share one more.
Flow control works per session, so a slow consumer does not hold back the publishers or another subscription.

## Shutdown

When the service stops, it closes the connection from the inside out: the subscriptions' sessions, the publishers' links and their session, then the connection.
A publisher used after that reports [`AmqpError::NotConnected`](api:ruststream_amqp::AmqpError::NotConnected) instead of succeeding against a closed connection.
The AsyncAPI document of the service names the broker's host and port and never the credentials.

## Testing the configuration

The tests start the production `service` with the production `broker`, given test credentials, under `TestApp::start`.
The in-process broker checks the URL the way the client reads it, without dialling the broker.
A URL the client cannot open fails the start:

```text
the scheme "https" is not an AMQP scheme
```

The tests need the same TLS feature as the service: without it, an `amqps://` URL fails the start too.
Authentication and the certificate check happen only on a real broker.
