---
sidebar_position: 2
title: Retries
description: "max_attempts caps deliveries; the count is the broker's delivery-count, or the framework's header on a copy this crate published."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries

[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) caps deliveries; the count is the broker's `delivery-count`, or the framework's header on a copy this crate published.

The cap counts the deliveries of one message at the mount site, the first one included.
A delivery whose message carries the AMQP `header` section reports the broker's `delivery-count`, which every `retry` increments.
A message this crate published carries no `header` section, so the broker has counted nothing for it, and the count is [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) instead.
Under a cap, a `retry` of such a message is the runtime's copy, not the broker's redelivery: the runtime rejects the delivery and publishes it again to the subscription's address at once, with the header incremented.
A delivery that reaches the cap is not retried: it goes to the [dead-letter address](./dead-letters.md), or is rejected where the registration names none.
A `retry` on a registration with no cap stays the broker's own redelivery, and nothing limits it.
The runtime rejects every original it replaces with a copy, so a broker that dead-letters rejected deliveries keeps those originals: ActiveMQ Artemis moves them to its `DLQ` address by default.
The copies leave through the [retry publisher](./out-retry.md) of the registration.

[Retries and dead letters](../../guides/retries.md) caps the attempts of a refunds service.

## Example

Five deliveries of one payment; the fifth failure goes to `payments.dead`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_retry.rs#declaration"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_retry.rs#declaration"
```

</TabItem>
</Tabs>
