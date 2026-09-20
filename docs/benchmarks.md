# Benchmarks

Between the `fe2o3-amqp` client and your handler there are two layers, and each one costs time on
every message: this crate, which turns a link into a subscription and a message into a delivery,
and the framework's runtime, which dispatches that delivery to a handler. This page measures both,
separately, against the same work written by hand on the client.

One process runs each scenario three times over, as three loops that differ in one thing each:

- **Raw client** drives `fe2o3-amqp` directly: receive, decode, read a field, accept.
- **Adapter** drives this crate's own consumer and publisher - the broker, the address descriptor,
  the subscription's stream, the delivery's `ack` - from a loop written here, with no service
  around it.
- **RustStream** is the whole service a user writes: a `#[subscriber]` handler, the app, the
  runtime.

Everything else is held equal - the connection, the session each link rides, the link credit, the
delivery guarantee, the position of the settlement, the decode into the same type, the payload
bytes, the tokio runtime and the build. The procedure is the framework's own and is described on
the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

## The numbers

Medians over interleaved rounds, with the observed spread in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Adapter", "framework": "RustStream", "adapterOverhead": "Adapter cost", "overhead": "Total cost", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "build": "Build", "roundTrip": "Round trip", "versions": "Versions", "measured": "Measured", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

The table is read in your browser from the document the last run wrote, so nothing on this page is
a copy that could have gone stale.

`Adapter cost` is the adapter against the raw client: what this crate's own consumer and publisher
cost over the client they wrap, and the figure this repository answers for. `Total cost` is the
service against the raw client, which adds what the runtime spends on top. The difference between
the two is the runtime's share over this broker, and it is published here because it depends on how
the two meet - how the subscription's stream yields, how deliveries arrive, how back-pressure
reaches the loop - and not only on the runtime itself.

The queue row is a consumer and nothing else: a delivery arrives, the body decodes, a field is
read, and the delivery is accepted. The request/reply row adds the publish path to the same
delivery: the responder answers on the address the request named and waits for the broker to accept
the reply. In both rows the requester and the producer are hand-written on all three loops, so the
only side that changes is the consuming one.

A row reported as `indistinguishable` is one whose two halves differ by less than the spread between
runs of either. That is the honest outcome wherever the protocol costs far more than the code above
it, and a figure below the run-to-run noise would read as precision that was never measured.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-amqp/latest/benchmarks/results.json).

## The machine

<div id="benchmark-environment"></div>

`Round trip` is the transport's own latency, measured outside every loop: a send whose disposition
the client waits for, taken tens of thousands of times over one connection. A row is marked
`broker-bound` when the round trips one delivery makes - none for a plain delivery, one for a reply
that waits to be accepted - account for at least half of what that delivery costs. The mark means
the transport paced the run and the figures beside it are a lower bound on the code's cost rather
than a measurement of it.

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one consumer, one address, a small body and a broker on the loopback. It measures what a
delivery costs in this crate, not what the broker can carry, and a row here is not comparable with
a row published for another broker: the transports do different work per message. A different
AMQP 1.0 broker would move all three figures of a row together, which is why the image is published
with them.

The window a run measures opens at the first delivery and closes when the last one's work ends, in
every loop alike. The settlement follows that point, so one disposition out of the hundreds of
thousands a run carries sits outside the number everywhere.

The load is published pre-settled, in every loop and in both scenarios. A producer that waited for
a disposition per message would make the row a measurement of how fast this broker confirms a send,
and the consumer under test would spend the run idle.

The numbers are a snapshot of one machine on one day. They are re-measured on demand, never in CI:
a shared runner's noise is larger than the difference this page is about.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs both scenarios, stops the stand
and rewrites `docs/benchmarks/results.json` with what it measured. It takes about half an hour and
wants the machine to itself. The message count is not fixed: a probe run sets it so that every
measured run lasts at least five seconds on whatever machine it is taken on.
