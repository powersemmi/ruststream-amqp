---
title: AMQP bindings
description: "With asyncapi the document says AMQP 1.0: the protocol key amqp1, and the crate's facts under the x-ruststream-amqp1 extension."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# AMQP bindings

With the `asyncapi` feature the generated document says `AMQP` 1.0: the protocol key `amqp1`, and the crate's facts under the `x-ruststream-amqp1` extension.

The core builds the `AsyncAPI` document from the app, before anything connects.
The specification has an `amqp1` binding, and every one of its objects must stay empty.
So this crate writes what it knows into one extension object, `x-ruststream-amqp1`, at each level where the binding would sit.
A [server](./server.md) names the protocol and the container the service presents.
A [channel](./channels.md) names its node address and, for a subscription, the link it reads over.
A send [operation](./operations.md) says how it posts.
A [reply](./replies.md) routed per request says where its address is read.

Everything comes from the service's own declarations: the broker, the descriptors and the publish policies.
The document is published and shared, so no binding carries a credential.
The binary `#[ruststream::app]` writes prints the document with `asyncapi gen`.

## Example

The dependency line:

```toml
ruststream-amqp = { version = "0.7", features = ["asyncapi"] }
```

The service registers its broker under the label `amqp`, the key of its server in the document.
`cargo run --example amqp_asyncapi --features asyncapi,transaction -- asyncapi gen` prints the document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_asyncapi.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_asyncapi.rs#app"
```

</TabItem>
</Tabs>

Its server and the channel of `orders` in the document:

```json reference="tests/asyncapi_excerpt.json"
```
