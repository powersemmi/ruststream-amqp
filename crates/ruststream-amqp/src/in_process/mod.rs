//! The broker's in-process mode, behind the `testing` feature: the transport a connected broker
//! carries when the test harness connects it through `InProcess::connect_in_process` rather than
//! through `connect`.
//!
//! The connected broker, its subscriber, its publishers and its delivery type each carry this
//! transport as a variant of their own, so a service's descriptors and publish policies run
//! against it unchanged. It has no settings of its own: it reads the production broker's, and it
//! frames a publish with the same conversion a live publish goes through, so a delivery carries
//! the headers a live one carries. It never succeeds where a server fails: a URL `connect` cannot
//! open, a message to the empty address, a handle outliving its connection, a settlement after
//! shutdown, a descriptor that cannot form a subscription and a transaction begun on a closed
//! connection are refused with the live error.
//!
//! What it models: exact-address routing, the queue terminus (competing consumers, one delivery
//! each) and the topic terminus (a copy for every subscription), the settle mode, the
//! `delivery-count` a `modified` disposition adds, a released delivery going back to a consumer
//! that is still attached, request/reply over a private reply address, and transactional posting.
//! What belongs to the server and is left to the live mode: an address's storage while nothing
//! consumes it (a message no subscription takes is dropped here), link credit, the dead-letter
//! policy behind a rejection and the server's own delivery limit, and a request refused by a peer.

mod bus;
mod deliveries;
mod request;
mod router;
#[cfg(feature = "transaction")]
mod txn;

use std::sync::Arc;

use bytes::Bytes;
use fe2o3_amqp_types::messaging::{Data, Message};
use ruststream::{OutgoingFor, OutgoingMessage, Take};
use url::Url;

pub(crate) use bus::Bus;
pub(crate) use deliveries::{BusDeliveries, Settlement};
pub(crate) use request::request;
#[cfg(feature = "transaction")]
pub(crate) use txn::TxnBuffer;

pub(crate) use self::router::Delivery;
use crate::address::AmqpAddress;
use crate::broker::is_at_most_once;
use crate::error::{AmqpError, box_err};
use crate::message::{build_message, headers_from_amqp, to_amqp_message};

/// Checks the broker URL the way `connect` reads it, so a broker a service could not connect is
/// not one a test can connect either: it has to parse, name a host, and use a scheme the client
/// opens (`amqps` only with a TLS feature).
///
/// # Errors
///
/// Returns [`AmqpError::Connect`] for a URL `connect` would refuse before any I/O.
pub(crate) fn check_url(url: &str) -> Result<(), AmqpError> {
    let url = Url::parse(url).map_err(|e| AmqpError::Connect(box_err(e)))?;
    let tls = cfg!(any(feature = "rustls", feature = "native-tls"));
    match url.scheme() {
        "amqp" => {}
        "amqps" if tls => {}
        "amqps" => {
            return Err(AmqpError::Connect(Box::from(
                "an amqps:// URL needs the `rustls` or `native-tls` feature",
            )));
        }
        other => {
            return Err(AmqpError::Connect(
                format!("the scheme {other:?} is not an AMQP scheme").into(),
            ));
        }
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(AmqpError::Connect(Box::from("the URL names no host")));
    }
    Ok(())
}

/// Opens a subscription on the in-process transport.
///
/// # Errors
///
/// Returns [`AmqpError::NotConnected`] once the connection has shut down.
pub(crate) fn subscribe(bus: &Arc<Bus>, address: &AmqpAddress) -> Result<BusDeliveries, AmqpError> {
    bus.ensure_live()?;
    let (id, receiver) = bus.subscribe(address.address().to_owned(), address.routing());
    Ok(BusDeliveries::new(
        Arc::clone(bus),
        id,
        address.address().to_owned(),
        receiver,
        is_at_most_once(address.settle_value()),
    ))
}

/// Publishes one message on the in-process transport.
///
/// # Errors
///
/// Returns [`AmqpError::NotConnected`] once the connection has shut down, and
/// [`AmqpError::PublishNotAccepted`] for a message to the empty address.
pub(crate) fn publish(bus: &Bus, msg: OutgoingFor<'_, Take>) -> Result<(), AmqpError> {
    bus.ensure_live()?;
    let address = msg.name();
    ensure_node(address)?;
    bus.route(address, &frame(msg));
    Ok(())
}

/// Refuses a message to the empty address, as the peer does: a sender with no target address is
/// an anonymous one, and the peer rejects a message it sends that names no destination of its own.
pub(crate) fn ensure_node(address: &str) -> Result<(), AmqpError> {
    if address.is_empty() {
        return Err(AmqpError::PublishNotAccepted {
            address: String::new(),
            outcome: "rejected: a message sent to the empty address names no node".to_owned(),
        });
    }
    Ok(())
}

/// Takes a message a test injects, as an external producer's message arrives: framed like any
/// other, with no publisher of this crate behind it.
pub(crate) fn inject(bus: &Bus, msg: &OutgoingMessage<'_>) -> Result<(), AmqpError> {
    bus.ensure_live()?;
    ensure_node(msg.name())?;
    let message = build_message(msg.headers(), msg.payload().to_vec());
    bus.route(msg.name(), &delivered(message));
    Ok(())
}

/// The delivery a live subscription would read for `msg`: the message this crate sends for it,
/// read back the way a delivery is read. The headers a live delivery carries are the ones the
/// `AMQP` sections hold, not the map the publish was handed.
fn frame(msg: OutgoingFor<'_, Take>) -> Delivery {
    delivered(to_amqp_message(msg))
}

fn delivered(message: Message<Data>) -> Delivery {
    Delivery {
        headers: headers_from_amqp(&message),
        payload: Bytes::from(message.body.0.into_vec()),
        // A message this crate publishes carries no `header` section.
        count: None,
    }
}
