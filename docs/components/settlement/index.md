---
title: Dispositions
description: "A handler's outcome is the delivery's disposition: ack accepts, retry sends modified with delivery-failed, drop rejects."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Dispositions

A handler's outcome is the delivery's disposition: `ack` accepts, `retry` sends `modified` with `delivery-failed`, `drop` rejects.

In AMQP 1.0 a receiver settles each delivery with a disposition, and the broker acts on its outcome.
[`ack`](api:ruststream::runtime::HandlerOutcome::ack) sends `accepted`, and the broker forgets the message.
[`retry`](api:ruststream::runtime::HandlerOutcome::retry) sends `modified` with `delivery-failed` set and `undeliverable-here` cleared.
The broker adds the failed attempt to the message's `delivery-count` and delivers it again, to this subscription or to another receiver on the address.
A `retry` never sends `released`: that outcome says the delivery was not acted on, so the broker leaves the count as it was, and a [cap on the attempts](./retries.md) reads that count.
[`drop`](api:ruststream::runtime::HandlerOutcome::drop) sends `rejected`, which ends the delivery: the broker's dead-letter policy decides where it goes, and ActiveMQ Artemis moves it to its `DLQ` address by default.
A delivery that is never settled, because the process stopped, stays on its link until the link detaches, and the broker then hands it to the next receiver.
AMQP 1.0 has no delayed redelivery, so [`retry_after`](./retry-after.md) is the runtime's own copy.
Under [`Settle::AtMostOnce`](./settle.md) the delivery is settled on receipt, and the outcome reaches no broker.
In process, `TestApp` keeps the same rules: a `retry` comes back with one more counted attempt, and a `drop` removes the delivery.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_settlement_dispositions.rs#dispositions"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_settlement_dispositions.rs#dispositions"
```

</TabItem>
</Tabs>
