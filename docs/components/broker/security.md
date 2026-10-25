---
sidebar_position: 2
title: Security
description: "Sasl picks the authentication (anonymous, plain, external); amqps:// needs rustls or native-tls, and Tls sets the trusted roots and client certificate."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Security

[`Sasl`](api:ruststream_amqp::Sasl) picks the authentication: anonymous, plain or external; an `amqps://` URL needs the `rustls` or `native-tls` feature, and [`Tls`](api:ruststream_amqp::Tls) sets the trusted authorities and the client certificate.

A service logs in one of two ways, and a broker uses one of them.
Credentials in the URL, `amqp://user:password@host`, log in with SASL PLAIN.
A profile set with [`sasl`](api:ruststream_amqp::AmqpBroker::sasl) goes with a URL that carries no credentials:

| Profile | Login |
|---|---|
| [`Sasl::plain(user, password)`](api:ruststream_amqp::Sasl::plain) | SASL PLAIN with the user and password as written |
| [`Sasl::anonymous()`](api:ruststream_amqp::Sasl::anonymous) | SASL ANONYMOUS, for a broker that admits clients without credentials |
| [`Sasl::external()`](api:ruststream_amqp::Sasl::external) | SASL EXTERNAL: the broker takes the identity of the client certificate; it needs an `amqps://` URL and the [certificate](#client-certificate) |

A password the deployment hands the service, from the environment or a secret store, goes through `Sasl::plain`.
So does a password with a character the URL reserves, such as `@`, `:`, `/` or `%`.
A connection with neither credentials nor a profile opens without SASL, and a broker that requires a login refuses it.
A refused login, such as a wrong password, and `Sasl::external` on an `amqp://` URL fail [startup](./startup.md) with [`AmqpError::Connect`](api:ruststream_amqp::AmqpError::Connect).

An `amqps://` URL encrypts the connection with TLS.
The handshake starts as soon as the socket opens, which is what a broker's TLS port expects.
The `rustls` feature checks the broker's certificate against the Mozilla root certificates built into the binary.
The `native-tls` feature uses the operating system's TLS library and its trust store; with both features enabled, `native-tls` is the one used.
The connect checks the certificate against the host in the URL, so the URL names the broker by a host name or an IPv4 address.
An IPv6 address gives TLS no server name, so the connect fails on it.

[`AmqpBroker::tls`](api:ruststream_amqp::AmqpBroker::tls) takes the connection's TLS settings, a [`Tls`](api:ruststream_amqp::Tls) value:

| Setting | What it does |
|---|---|
| [`root_certificates(pem)`](api:ruststream_amqp::Tls::root_certificates) | Trusts the certificate authorities in the PEM, beside the default ones |
| [`client_identity(certificate, key)`](api:ruststream_amqp::Tls::client_identity) | Presents the client certificate, proven by its PKCS #8 key |

Every setting is PEM text the deployment hands the service.
The settings are read when the broker connects, so a PEM that does not parse fails [startup](./startup.md).
TLS settings on an `amqp://` URL fail startup too: that connection would go out in the clear.
Neither the certificates nor the key appear in the broker's `Debug` output or the generated document.

A [`TestApp`](../testing/index.md) test reads the URL as the connect reads it, so an `amqps://` URL without a TLS feature fails startup there too.
The test does not read the certificates, so it runs without the deployment's.
The login itself happens only against a broker, in a [live test](../testing/live.md).
The generated [AsyncAPI document](../asyncapi/server.md) reports the host and port and never the credentials in the URL.

[Connecting to production](../../guides/production.md) takes the URL and the credentials from the environment.

## Example

The dependency line:

```toml
ruststream-amqp = { version = "0.7", features = ["rustls"] }
```

The service logs in as `orders-svc` over TLS.
The password comes from the environment.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_broker_security.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_broker_security.rs#broker"
```

</TabItem>
</Tabs>

## Client certificate

The dependency line is the same.
The service logs in with SASL EXTERNAL, and the broker takes its identity from the client certificate.
The broker's own authority verifies the broker.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_broker_security.rs#certificate"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_broker_security.rs#certificate"
```

</TabItem>
</Tabs>
