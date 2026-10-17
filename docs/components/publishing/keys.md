---
sidebar_position: 2
title: Keys
description: "The partition-key header rides the AMQP group-id property, and a broker that honours message groups keeps one group on one consumer."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Keys

The partition-key header, [`PARTITION_KEY_HEADER`](api:ruststream_amqp::PARTITION_KEY_HEADER), rides the `AMQP` `group-id` property, and a broker that honours message groups keeps one group on one consumer.

The header is named `partition-key`, the name the in-memory broker uses, so a service keeps its headers when it changes brokers.
On publish its value becomes the message's `group-id` and does not travel as an application property.
On delivery `group-id` comes back as the `partition-key` header.

Message groups are the broker's feature.
On `ActiveMQ` Artemis every message of one group goes to one consumer of the queue, so the consumer sees the group in the order it was sent.
Competing consumers of an [`AmqpAddress::queue`](api:ruststream_amqp::AmqpAddress::queue) address then share the groups instead of the messages.

A message type declares the header with `#[outgoing(headers = ..)]` and a field renamed to `partition-key`; a `HeaderMap` with that entry works the same.
In process the key travels as on a server, and the consumers of a queue take turns whatever the group.
Pinning a group to one consumer is the server's work, so a running broker is where it shows.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_publishing_keys.rs#keys"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_publishing_keys.rs#keys"
```

</TabItem>
</Tabs>
