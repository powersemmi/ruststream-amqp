---
title: Methodology
description: "How ruststream-amqp measures itself: the three loops, the run on a real broker, the broker-bound arithmetic and the code count."
---

# Methodology

How `ruststream-amqp` measures itself: the three loops, the run on a real broker, the broker-bound
arithmetic and the code count. The procedure is the framework's own
([RustStream methodology](https://powersemmi.github.io/ruststream/latest/benchmarks/#methodology)),
and this page states what it is on AMQP 1.0.

## The three loops

One process runs the scenario three times over, as three loops that differ in one thing: what
carries the messages.

- **Raw client.** An `fe2o3-amqp` receiver, driven directly: receive, decode, read a field,
  accept.
- **Broker crate.** This crate and nothing above it: the broker connects, the
  [address descriptor](api:ruststream_amqp::AmqpAddress) opens a subscription, and a loop pulls
  deliveries off its stream, decodes them and acks them.
- **RustStream.** The service a user writes: a `#[subscriber]` handler and the app around it.

Everything else is equal across the three: the connection, the session each link rides, the link
credit of 256, the delivery guarantee, the place of the ack, the decode into the same struct, the
payload bytes, the tokio runtime with four worker threads, and the binary.

The body is 512 bytes of JSON. It decodes into a struct of two integer fields, so the decode
allocates nothing and the number is about this crate rather than about the JSON parser.

## The run

- **A fresh address per run.** A run never sees what an earlier one left.
- **The consumer attaches first.** A producer on a second connection then feeds it, at most 8192
  messages ahead. It publishes them pre-settled, so the run measures the consuming side rather than
  how fast the broker confirms a send.
- **The window opens at the first delivery and closes when the last one is decoded**, in all three
  loops. Connecting, beginning the sessions and attaching the links are outside it.
- **The message count comes from a probe**, which measures the raw loop's rate, so a measured run
  lasts at least five seconds on any machine.
- **The loops are interleaved**: raw, crate, RustStream, three rounds. Each loop reports its best,
  median and worst round.

A row is `broker-bound` when the consumer spent most of the run waiting on the broker. That is
arithmetic: a probe times 20000 sends that wait for their disposition and takes the median, the
transport states how many round trips one delivery costs, and the row is flagged when their product
reaches half the measured time per message. A delivery here costs none: the broker transfers
against credit the client tops up in the background, and an accept is sent without waiting for an
answer. The round trip is published with the machine, so the arithmetic can be checked.

## The code count

Each scenario of the [cost chart](/architecture/benchmarks/) is the service a user writes, on
[`AmqpBroker`](api:ruststream_amqp::AmqpBroker) against the same broker, built without the
`testing` feature and run on a single-threaded runtime. A thread of its own, on another connection,
fills the queue between the measured regions, and every one of its publishes waits for the broker
to accept it.

Instructions come from callgrind and allocations from DHAT. A scenario runs over one delivery,
then 1000, then 2000. The difference between the last two is the steady cost of a message, and the
single delivery is the cold start.

The socket is real, so a count moves a little between runs of one binary: how many transfers one
read brings in depends on what the broker had written by then. Over five runs the instructions per
message stayed within two tenths of a percent. A run against a saved baseline fails on two percent
more instructions in a scenario. Allocations are held to a floor each scenario declares, the
highest count seen plus a tenth of a percent, on every run.

## Running it yourself

```bash
just bench
```

The recipe starts the stand from `docker-compose.test.yml`, runs the comparison, stops the stand
and rewrites `docs/benchmarks/results.json`. It takes about a minute and wants the machine to
itself. It clears `RUSTFLAGS`, so the binary is not tied to the CPU it was built on. Both recipes
build the benchmarks the way a release is built, with fat link-time optimization across crates, so
the numbers describe the code a service ships.

```bash
just bench-code
```

The code count takes about half a minute and needs valgrind. The recipe installs the benchmark
runner itself, at the release the lock file pins. `just bench-code --save-baseline=main` on `main`
records a baseline, and `just bench-code --baseline=main` on a branch compares with it.
