# Benchmarks

Between the `fe2o3-amqp` client and your handler there are two layers, and each one costs time on
every message: this crate, which turns a link into a subscription and a message into a delivery,
and the framework's runtime, which dispatches that delivery to a handler. This page measures both,
separately, against the same work written by hand on the client.

One process runs the scenario three times over, as three loops that differ in one thing each:

- **Raw client** drives `fe2o3-amqp` directly: receive, decode, read a field, accept.
- **Adapter** drives this crate's own consumer - the broker, the address descriptor, the
  subscription's stream, the delivery's `ack` - from a loop written here, with no service around
  it.
- **RustStream** is the whole service a user writes: a `#[subscriber]` handler, the app, the
  runtime.

Everything else is held equal - the connection, the session each link rides, the link credit, the
delivery guarantee, the position of the settlement, the decode into the same type, the payload
bytes, the tokio runtime and the build. The procedure is the framework's own and is described on
the
[RustStream benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology);
this page publishes what it produced here.

## The numbers

The best of three interleaved rounds, with the median round in parentheses. Higher is better.

<div id="benchmark-results" data-benchmark-labels='{"loading": "Loading the published results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Adapter", "framework": "RustStream", "adapterOverhead": "Adapter cost", "overhead": "Total cost", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "machine": "Machine", "os": "OS", "broker": "Broker", "build": "Build", "roundTrip": "Round trip", "versions": "Versions", "measured": "Measured", "codeMeasured": "Code costs measured", "codeUnpublished": "This results document carries no code costs.", "instructions": "Instructions per message", "allocations": "Allocations per message", "cold": "Cold start (instructions / allocations)", "unavailable": "No results could be read. They are published at {url}.", "unknownSchema": "The published results declare schema {schema}, which this page does not render."}'></div>

The table is read in your browser from the document the last run wrote, so nothing on this page is
a copy that could have gone stale.

`Adapter cost` is the adapter against the raw client: what this crate's own consumer costs over the
client it wraps, and the figure this repository answers for. `Total cost` is the
service against the raw client, which adds what the runtime spends on top. The difference between
the two is the runtime's share over this broker, and it is published here because it depends on how
the two meet - how the subscription's stream yields, how deliveries arrive, how back-pressure
reaches the loop - and not only on the runtime itself.

A negative cost means the column was faster than the raw client. A hand-written loop holds one
delivery at a time: while it decodes and settles, nothing is reading the socket. This crate's
subscription reads ahead instead, into a buffer bounded by the same link credit, so the next
transfer is already in hand when the loop asks for it; the runtime adds another step of the same
kind. What the columns measure here is therefore not a tax but the difference between a loop that
waits and a pipeline that does not.

The row is a consumer and nothing else: a delivery arrives, the body decodes, a field is read, and
the delivery is accepted. The producer is hand-written in all three loops, so the only side that
changes is the consuming one.

A row reported as `indistinguishable` is one whose two halves differ by less than the spread between
runs of either, and whose rounds did not all come out the same way round. That is the honest outcome
wherever the protocol costs far more than the code above it, and a figure below the run-to-run noise
would read as precision that was never measured. The second half of the rule matters here: the
faster a column is on this transport, the wider its own spread, and a difference every round agrees
on is a result rather than noise.

The machine-readable form of the same run, which the framework's site reads to build its
cross-broker table, is at
[`benchmarks/results.json`](https://powersemmi.github.io/ruststream-amqp/latest/benchmarks/results.json).

## The crate's own code

<div id="benchmark-code"></div>

The second table is this crate's own cost per message, counted rather than timed: instructions
under callgrind and allocations under DHAT. Each scenario is the service a user writes, built on
`AmqpBroker` and connected to the same Artemis stand, on a single-threaded runtime. What is counted
is everything that runs on the service's thread: the framework's dispatch, this crate's
subscription, message and publisher, and the `fe2o3-amqp` client's framing and decoding, which run
there as the connection's tasks. The broker is another process, and none of its work is in the
number. The queue is filled before the drain starts, over another connection on another thread,
and that thread is not counted either.

Instructions and allocations are per message in the steady state: the slope between a run of 1000
deliveries and a run of 2000. The last column is what connecting the service, attaching its
subscription and taking the first delivery cost once. The numbers are absolute, the framework's and
the client's cost included; the core publishes the framework's cost alone on its
[benchmarks page](https://powersemmi.github.io/ruststream/latest/benchmarks/).

The socket is real, so a count moves a little between runs of one binary. Over five runs the
instructions per message stayed within two tenths of a percent and the cold start within one
percent, and the longest run's allocations moved by up to four blocks in two thousand deliveries.
`just bench-code` fails when a run allocates more than the floor its scenario declares, which is
the highest count seen plus a tenth of a percent. With `--baseline=main` it also fails on more than
two percent more instructions. A pull request that changes the cost cites its numbers.

## The machine

<div id="benchmark-environment"></div>

`Round trip` is the transport's own latency, measured outside every loop: a send whose disposition
the client waits for, taken tens of thousands of times over one connection. A row is marked
`broker-bound` when the round trips one delivery makes account for at least half of what that
delivery costs. A delivery here makes none: transfers arrive against credit the subscription
replenishes in the background, and an accept is written without an answer being waited for. The
mark means the transport paced the run and the figures beside it are a lower bound on the code's
cost rather than a measurement of it.

The build flags are published with the numbers because they change them: a binary built with
`-C target-cpu=native` produces a figure no other machine can reproduce, so the recipe clears the
variable before it builds.

## What they do not mean

This is one consumer, one address, a small body and a broker on the loopback. It measures what a
delivery costs in this crate, not what the broker can carry, and a row here is not comparable with
a row published for another broker: the transports do different work per message. A different
AMQP 1.0 broker would move all three figures of a row together, which is why the image is published
with them.

Only the consuming side is measured. A second scenario for the publish path - a responder answering
every delivery on the address the request named - was written and thrown away, because it measured
a TCP timer rather than any code: the socket the client opened was left with Nagle's algorithm on,
and a reply that waits for its disposition on a connection that is also writing dispositions sat in
the kernel's buffer until the peer's delayed acknowledgement released it. This crate now opens that
socket itself and sets `TCP_NODELAY` on it, which takes one request/reply round trip against the
Artemis stand from 140 ms to 2 ms. The scenario has not been rewritten yet, so this comparison
covers the consuming side; the reply row of the code table counts what a publish costs on the
service's thread.

The window a run measures opens at the first delivery and closes when the last one's work ends, in
every loop alike. The settlement follows that point, so one disposition out of the hundreds of
thousands a run carries sits outside the number everywhere.

The load is published pre-settled, in every loop. A producer that waited for a disposition per
message would make the row a measurement of how fast this broker confirms a send, and the consumer
under test would spend the run idle.

The numbers are a snapshot of one machine on one day. They are re-measured by hand, on a machine
given to the run alone: the difference this page is about is smaller than the noise of a shared one.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs the scenario, stops the stand and
rewrites `docs/benchmarks/results.json` with what it measured. It takes about a minute and wants
the machine to itself. The message count is not fixed: a probe run sets it so that every
measured run lasts at least five seconds on whatever machine it is taken on.

```bash
just bench-code
```

The recipe starts the same stand, counts the code table under valgrind, stops the stand and rewrites
the `code` section of the same document. It takes about half a minute and needs valgrind and the benchmark
runner: `cargo install --locked gungraun-runner --version =0.19.4`.
