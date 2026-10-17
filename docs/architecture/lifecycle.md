---
title: Lifecycle
sidebar_position: 3
description: "AmqpBroker::new records configuration, connect opens the connection and the publishers' session, startup attaches each subscription, and shutdown runs inwards."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The broker's states as types, and what each transition does on the wire",
  "text": "The diagram as text",
  "states": "Broker",
  "wire": "On the wire",
  "unconnected": ["AmqpBroker::new", "(url)"],
  "connected": ["ConnectedAmqpBroker"],
  "closed": ["Shut down:", "Ok(()) or the", "innermost error"],
  "config": ["Records the URL,", "SASL, container id;", "no I/O"],
  "open": ["Socket, TLS, SASL,", "open, and the", "publishers' session"],
  "subscriptions": ["Per subscription:", "a session, a receiver", "and its pump task"],
  "senders": ["The first publish", "to an address attaches", "its sender link"],
  "teardown": ["Pumps end their", "sessions, sender links", "close, the session", "ends, then the", "connection"],
  "afterShutdown": ["A publisher kept from before connect or after shutdown", "answers AmqpError::NotConnected, never a silent success"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)"
};

# Lifecycle

[`AmqpBroker::new`](../components/broker/index.md) records configuration, connect opens the
connection and the publishers' session, startup attaches each subscription, and shutdown runs
inwards.

<Diagram name="lifecycle" words={words} />

`new` does no I/O, so the service's builder stays synchronous. The runtime connects the broker when
the app starts. Connect parses the URL, opens the socket, authenticates, opens the connection and
begins the session the publishers share. A URL that does not parse, a refused socket or a refused
login fails the [start](../components/broker/startup.md) with `AmqpError::Connect`.

Each subscription's descriptor resolves once, against the connected broker. An empty address is
refused before any I/O. The subscription begins a session, attaches its receiver with its
[credit](../components/subscriptions/credit.md), and starts the pump task that owns the receiver. A
broker that refuses the attach fails the start with `AmqpError::Attach`, naming the address.

A sender link is attached by the first publish to its address, not at startup. A service that only
consumes attaches none.

[Shutdown](../components/broker/shutdown.md) marks the connection closed and tears it down from the
inside out: each pump detaches its receiver and ends its session, the sender links close, the
publishers' session ends, and the connection closes last. Each layer still routes the broker's
answer for the one inside it. Every step runs even after an earlier one failed, and the error
reported is the innermost one. A subscription still open at shutdown yields the deliveries it holds
and ends; settling one of them afterwards reports an error.

A broker cloned before it connected shares its connection with the clone. A clone that the [test
harness](../components/testing/index.md) connected in process cannot then connect to a server, and
the reverse is refused too.
