---
sidebar_position: 1
title: Settle
description: "Settle::AtMostOnce settles each delivery on receipt, so the message is gone whether the handler finished or not, and ack reports Unsupported."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Settle

[`Settle::AtMostOnce`](api:ruststream_amqp::Settle::AtMostOnce) settles each delivery on receipt, so the message is gone whether the handler finished or not, and `ack` reports `Unsupported`.

[`AmqpAddress::settle`](api:ruststream_amqp::AmqpAddress::settle) sets the mode on the descriptor.
`Settle::AtLeastOnce` is the default: the handler's outcome settles the delivery, as [Dispositions](./index.md) describes.
`Settle::AtMostOnce` accepts each delivery as it arrives, before the handler runs.
The broker forgets the message at once, so a crash in the middle of the handler, a `retry` or a `drop` brings nothing back.
The outcome the handler returns has nothing left to settle: the settlement reports [`AckError::Unsupported`](api:ruststream::AckError::Unsupported), and the runtime logs a warning that names the subscription.
The mode suits traffic where a lost message costs less than a redelivered one, such as a reading that a sensor sends again a second later.
The [credit](../subscriptions/credit.md) of an at-most-once subscription tops itself up as the deliveries arrive.
In process, [`TestApp`](../testing/index.md) settles on receipt the same way, so a test sees a `retry` bring nothing back.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_settlement_settle.rs#settle"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_settlement_settle.rs#settle"
```

</TabItem>
</Tabs>
