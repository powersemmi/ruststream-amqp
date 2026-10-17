---
sidebar_position: 3
title: Dead letters
description: "dead_letter names the address a spent delivery is published to; a cap without one rejects it, and the broker's own dead-letter policy applies."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dead letters

[`dead_letter(address)`](api:ruststream::runtime::Mounting::dead_letter) names the address a spent delivery is published to; a cap without one rejects it, and the broker's own dead-letter policy applies.

It follows [`max_attempts(n)`](./retries.md) at the mount site.
When the delivery that fails is the `n`-th one, the runtime rejects it and publishes a copy to the dead-letter address instead of the subscription's.
The copy keeps the payload and the headers, and [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) on it says how many times the message was delivered.
It leaves through the registration's [retry publisher](./out-retry.md), so the steps that follow `out_retry` apply to it too.
The dead-letter address is an ordinary address: a subscription of this service, or of another one, reads it.
A cap without `dead_letter` rejects the spent delivery, and a warning names the subscription.
Where a rejected delivery goes is the broker's policy: ActiveMQ Artemis moves it to its `DLQ` address by default.
The original of a dead-letter copy is rejected as well, so on Artemis the spent delivery also reaches `DLQ`.
In process a rejected delivery leaves the transport, so a test asserts the copy on the named address, as the example's test does.

## Example

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

The dead-letter address is read by an ordinary subscription:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_retry.rs#dead_letter"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_retry.rs#dead_letter"
```

</TabItem>
</Tabs>
