---
sidebar_position: 1
title: Live
description: "TestApp::start_live runs the same test body against a running broker, for what only a server has: credit, dead-letter policy, grouping."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Live

[`TestApp::start_live`](api:ruststream::testing::TestApp::start_live) runs the same test body against a running broker, for what only a server has: credit, dead-letter policy, grouping.

It connects the broker through its ordinary connect, so the socket, the login and every subscription's link are real.
The body is the one an in-process test runs; only the start call differs.
A publish, `settle` and `advance` wait on the real clock until every subscription has handled what was published to it.
The wait lasts ten seconds at most, and [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets another bound.

The live mode covers what the [in-process transport](./index.md) leaves to the server: [link credit](../subscriptions/credit.md), the broker's [dead-letter policy](../settlement/dead-letters.md) behind a rejection and its own delivery limit, message groups by [partition key](../publishing/keys.md), the fan-out a broker gives a [raw](../subscriptions/raw.md) address, how a broker creates an address a publish reaches first, and the [login](../broker/security.md).
A broker keeps what earlier runs left on it.
Each run reads addresses of its own, named with the process id, so it never takes a message an earlier run left behind.

In the example, `live::url` returns the broker's URL from `AMQP_TEST_URL`, and the test returns early without one, so `cargo test` passes on a machine with no broker.
A job that stood a broker up sets `RUSTSTREAM_REQUIRE_LIVE=1`, and a missing URL then fails the test instead of skipping it.
The repository's stand is ActiveMQ Artemis in `docker-compose.test.yml`, which `just brokers-up` starts on `localhost:5672` with the user `artemis`.

## Example

The two tests share one body: one in process, one against the broker at `AMQP_TEST_URL`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/amqp_testing_live.rs#both"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/amqp_testing_live.rs#both"
```

</TabItem>
</Tabs>
