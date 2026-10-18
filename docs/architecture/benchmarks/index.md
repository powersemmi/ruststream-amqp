---
title: Benchmarks
description: "What ruststream-amqp costs on every message: throughput against the raw fe2o3-amqp client, and instructions and allocations of its code."
---

import {Environment, MovedSections, Results} from '@site/src/components/Benchmarks';
import {amqp} from '@site/src/components/Benchmarks/amqp';

export const labels = {"loading": "Loading the results...", "scenario": "Scenario", "raw": "Raw client", "adapter": "Broker crate", "framework": "RustStream", "overhead": "Overhead", "against": "({percent} over the client)", "indistinguishable": "indistinguishable", "brokerBound": "broker-bound", "measured": "measured", "details": "How the numbers were taken", "pending": "No results published yet: {brokers}.", "instructions": "Instructions", "allocations": "Allocations", "cold": "Cold start", "table": "Show the numbers as a table", "axis": "How far each loop falls below the raw client's best round", "rounds": "rounds, best to slowest", "best": "best round", "median": "median round", "noise": "spread of the rounds", "comparison": "Against a raw client", "code": "Cost of the code", "broker": "The broker", "plugin": "The {plugin} plugin", "pluginRaw": "Without the plugin", "pluginAdapter": "Plugin written by hand", "pluginFramework": "The crate's plugin", "pluginAxis": "How far each loop falls below the best round of the app without the plugin", "pluginAgainst": "({percent} over no plugin)", "pluginOverhead": "Over the plugin by hand", "pluginOverHand": "({percent} over by hand)"};

export const machine = {"cpu": "Processor", "architecture": "Architecture", "cpu_frequency": "Frequency", "cores": "Cores", "memory": "Memory", "memory_speed": "Memory speed", "os": "Operating system", "broker": "Broker", "rustc": "Compiler", "profile": "Build profile", "features": "Features", "rustflags": "RUSTFLAGS", "round_trip": "Round trip", "valgrind": "Valgrind"};

# Benchmarks

What `ruststream-amqp` costs on every message: throughput against the raw `fe2o3-amqp` client, and
instructions and allocations of its code.

## Against a raw client

The first chart runs one scenario three ways on a real AMQP 1.0 broker, ActiveMQ Artemis.
`Raw client` drives an `fe2o3-amqp` receiver directly. `Broker crate` drives this crate's
subscription with a loop of its own and no runtime. `RustStream` is the service a user writes, a
`#[subscriber]` handler in an app. The first gap is what this crate costs over the client it
wraps, and the second is what the runtime adds on top.

Each loop is a bar from its best round to its slowest, with a dot at the best round and a notch at
the median. The zero of the axis is the raw client's best round, and a dot right of zero is slower.
A hollow dot is a loop whose difference from the raw client is smaller than the spread of the
rounds. The table under the chart prints the best round with the median in parentheses.

A dot left of zero, where one appears, is a loop faster than the raw client. The raw loop holds one
delivery at a time, and nothing reads the socket while it decodes and settles. This crate's
subscription reads ahead into a channel as deep as the link credit, so the next delivery is already
in hand when the loop asks for it.

## Cost of the code

The second chart counts what one message costs on the service's thread: instructions under
callgrind and allocations under DHAT, in the steady state. Each scenario is a service on
[`AmqpBroker`](api:ruststream_amqp::AmqpBroker) against the same broker. The count includes the
framework's dispatch, this crate's code and the `fe2o3-amqp` client's framing and decoding, which
run on that thread as the connection's tasks. The broker is another process, and none of its work
is counted. `Cold start` is what connecting the service, attaching its subscription and taking the
first delivery cost once.

## Results

<Results entry={amqp} labels={labels} details="/architecture/benchmarks/methodology/" />

## The machine

<Environment entry={amqp} labels={machine} />

The numbers come from `docs/benchmarks/results.json` of this version, written by the benchmark
recipe. [Methodology](methodology.md) says how they were taken, and [Numbers](numbers.md) what
they allow you to conclude.

<MovedSections />
