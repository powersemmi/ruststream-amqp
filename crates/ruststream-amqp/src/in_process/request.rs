//! Request/reply on the in-process transport.

use std::sync::Arc;
use std::time::Duration;

use ruststream::{IncomingMessage, OutgoingFor, Take};

use super::bus::Bus;
use super::deliveries::Settlement;
use super::router::Delivery;
use super::{ensure_node, frame};
use crate::address::Routing;
use crate::error::AmqpError;
use crate::message::AmqpMessage;

/// One request and its correlated reply, the way the live publisher performs it.
///
/// The transport mints the private reply address the request advertises, which is the peer's job
/// on a server (a dynamic terminus), and the request overwrites `reply-to` and `correlation-id` as
/// the live one does. A reply carrying another correlation id is discarded and the wait continues,
/// so a late answer to an earlier request cannot resolve this one.
///
/// # Errors
///
/// Returns [`AmqpError::NotConnected`] once the connection has shut down,
/// [`AmqpError::PublishNotAccepted`] for a request to the empty address, and
/// [`AmqpError::RequestTimeout`] when no correlated reply arrives within `timeout`.
pub(crate) async fn request(
    bus: &Arc<Bus>,
    msg: OutgoingFor<'_, Take>,
    timeout: Duration,
) -> Result<AmqpMessage, AmqpError> {
    bus.ensure_live()?;
    let address = msg.name();
    ensure_node(address)?;
    let reply_to = bus.next_reply_address();
    let correlation_id = format!("{reply_to}-corr");
    let mut delivery = frame(msg);
    delivery.headers.insert("reply-to", reply_to.clone());
    delivery
        .headers
        .insert("correlation-id", correlation_id.clone());

    // The private reply address carries one consumer, this request.
    let (id, mut replies) = bus.subscribe(reply_to, Routing::Anycast);
    bus.route(address, &delivery);

    let reply = tokio::time::timeout(timeout, async {
        loop {
            let Delivery {
                payload, headers, ..
            } = replies.recv().await?;
            // Every delivery becomes a message before it is judged, so a discarded one still
            // releases its place in the harness's in-flight count when it drops.
            let reply = AmqpMessage::settled(payload, headers, None)
                .settling(Settlement::settled(Arc::clone(bus)));
            if reply.headers().correlation_id() == Some(correlation_id.as_str()) {
                return Some(reply);
            }
        }
    })
    .await
    .ok()
    .flatten();

    // Detaching the reply link: the address stops taking anything once the exchange is over, and
    // whatever raced the deadline into the channel is released through the same kind of message.
    bus.unsubscribe(id);
    while let Ok(Delivery {
        payload, headers, ..
    }) = replies.try_recv()
    {
        drop(
            AmqpMessage::settled(payload, headers, None)
                .settling(Settlement::settled(Arc::clone(bus))),
        );
    }

    reply.ok_or(AmqpError::RequestTimeout)
}
