---
title: Numbers
description: "What the benchmark numbers of ruststream-amqp measure, and what they do not allow you to conclude."
---

# Numbers

What the benchmark numbers of `ruststream-amqp` measure, and what they do not allow you to
conclude.

## The comparison against a raw client

The three loops of a row are comparable with each other, because they ran in one process,
interleaved, on one broker. A row is not comparable with a row another broker crate publishes: the
transports do different work per message. Another AMQP 1.0 broker would move all three figures of a
row together, which is why the broker's image is published with them.

The broker runs on the same machine, on the loopback. With no network latency, the same cost per
message is a larger share of the time than it would be against a broker across a network. Read the
percentage as an upper bound on what a deployed service pays.

A negative difference is the read-ahead, not a saving of the code: the crate's subscription keeps
receiving while the loop above it works, and the raw loop does not.

A `broker-bound` row is one where the consumer waited on the broker for most of the run. The work
of the crate and the runtime then happens inside that wait, and the difference comes out near zero.
That is a lower bound on the cost, not a measurement of it.

## The cost of the code

An instruction count holds this crate's code, the runtime above it and the `fe2o3-amqp` client's
work on the service's thread. It moves a little between runs, because the broker is real. The same
count costs more time where it misses the cache, so a count is not a time.

Every figure is the steady state, the difference between a run of 1000 deliveries and one of 2000.
Connecting, attaching and the first allocations behind them are the cold start, paid once.

## What they do not mean

A row is one consumer reading one queue address, a small body and a single broker. It measures what
a delivery costs inside this crate, not what the broker can carry.

The comparison measures the consuming side only. What a publish costs on the service's thread is
the reply row of the code chart. Throughput of publishing is bounded by the broker's round trip,
because a publish waits for its disposition, as [Path](../path.md) draws.

The numbers are a snapshot of one machine on one day, with the build recorded beside them. They are
measured again by hand, on a machine given to the run alone: the differences they show are smaller
than the noise of a shared one.
