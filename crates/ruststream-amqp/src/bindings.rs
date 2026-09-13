//! What this crate contributes to a generated `AsyncAPI` document.
//!
//! The specification does have an `amqp1` binding, and all four of its objects are reserved: each
//! one "MUST NOT contain any properties". So everything this crate knows about a node, a link and
//! a publisher travels in one extension object instead, at the level the binding would have sat
//! at. Nothing here is read from a connection - the document is built before anything connects -
//! and nothing here is a credential.

use ruststream::asyncapi::{Binding, Bindings};
use serde::Serialize;

use crate::address::Settle;

/// The extension key this crate writes at every level.
const EXTENSION: &str = "x-ruststream-amqp1";

/// The `AsyncAPI` runtime expression naming where a client reads the address of an answer.
///
/// This crate's request/reply sets the `AMQP` `reply-to` property, which reaches a handler as the
/// `reply-to` header, so a transform that routes a reply per delivery reads it there.
pub(crate) const REPLY_ADDRESS_LOCATION: &str = "$message.header#/reply-to";

/// One extension object at whichever level the caller is filling.
///
/// A body here is a struct of owned scalars, so serializing it cannot fail; the empty set is the
/// fallback rather than an error path, because a description must not hold up a service.
fn extension<T: Serialize>(body: &T) -> Bindings {
    Binding::extension(EXTENSION, body)
        .map(|binding| Bindings::new().with(binding))
        .unwrap_or_default()
}

/// What a subscription says about the node it reads and the link it reads it over.
#[derive(Debug, Serialize)]
struct Channel<'a> {
    /// The node address: what a receiver attaches its source to and a sender its target.
    address: &'a str,
    /// The terminus capability the subscription asks for, absent on a verbatim address.
    #[serde(skip_serializing_if = "Option::is_none")]
    capability: Option<&'static str>,
    /// Link credit: how many unsettled deliveries the broker may have in flight.
    credit: u32,
    /// The delivery guarantee, `at-least-once` or `at-most-once`.
    #[serde(rename = "settleMode")]
    settle_mode: &'static str,
}

/// How a send through one publish policy reaches the node.
#[derive(Debug, Serialize)]
struct Operation {
    posting: &'static str,
}

/// What the connection says about itself.
#[derive(Debug, Serialize)]
struct Server<'a> {
    #[serde(rename = "containerId")]
    container_id: &'a str,
}

/// The channel object of a subscription on `address`.
pub(crate) fn channel(
    address: &str,
    capability: Option<&'static str>,
    credit: u32,
    settle: Settle,
) -> Bindings {
    extension(&Channel {
        address,
        capability,
        credit,
        settle_mode: match settle {
            Settle::AtLeastOnce => "at-least-once",
            Settle::AtMostOnce => "at-most-once",
        },
    })
}

/// The send operation of a publisher that waits for the peer's disposition on every transfer.
pub(crate) fn confirmed_posting() -> Bindings {
    extension(&Operation {
        posting: "confirmed",
    })
}

/// The send operation of a publisher whose transfers carry a transactional state.
#[cfg(feature = "transaction")]
pub(crate) fn transactional_posting() -> Bindings {
    extension(&Operation {
        posting: "transactional",
    })
}

/// The server object of a connection identifying itself as `container_id`.
pub(crate) fn server(container_id: &str) -> Bindings {
    extension(&Server { container_id })
}
