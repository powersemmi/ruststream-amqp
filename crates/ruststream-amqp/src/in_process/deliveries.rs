//! The consuming half of the in-process transport: one subscription's stream, and how a delivery
//! taken from it settles.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use ruststream::{AckError, HeaderMap, Subscriber};

use super::bus::Bus;
use super::router::{Delivery, DeliveryReceiver, SubscriptionId};
use crate::error::AmqpError;
use crate::message::{AmqpMessage, SettleKind};

/// One subscription on the in-process transport, one delivery at a time: what the subscriber
/// batches over, as it batches over a live link.
pub(crate) struct BusDeliveries {
    bus: Arc<Bus>,
    id: SubscriptionId,
    address: String,
    receiver: DeliveryReceiver,
    /// An at-most-once subscription settles its deliveries on receipt, so they carry no way back.
    at_most_once: bool,
}

impl BusDeliveries {
    pub(crate) fn new(
        bus: Arc<Bus>,
        id: SubscriptionId,
        address: String,
        receiver: DeliveryReceiver,
        at_most_once: bool,
    ) -> Self {
        Self {
            bus,
            id,
            address,
            receiver,
            at_most_once,
        }
    }
}

impl Drop for BusDeliveries {
    /// Detaching the link: the address stops handing this subscription anything.
    fn drop(&mut self) {
        self.bus.unsubscribe(self.id);
    }
}

impl fmt::Debug for BusDeliveries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BusDeliveries")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl Subscriber for BusDeliveries {
    type Message = AmqpMessage;
    type Error = AmqpError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let Self {
            bus,
            id,
            address,
            receiver,
            at_most_once,
        } = self;
        // Polls the receiver in place rather than wrapping it in an owning stream, so `stream` can
        // be called again after the returned stream is dropped.
        futures::stream::poll_fn(move |cx| {
            receiver.poll_recv(cx).map(|next| {
                next.map(|delivery| {
                    let origin = (!*at_most_once).then(|| Origin {
                        id: *id,
                        address: address.clone(),
                    });
                    let settlement = Settlement::new(Arc::clone(bus), origin);
                    Ok(AmqpMessage::in_process(delivery, settlement))
                })
            })
        })
    }
}

/// The subscription a delivery came from, which a requeue returns it to.
#[derive(Debug)]
struct Origin {
    id: SubscriptionId,
    address: String,
}

/// How an in-process delivery settles, in place of the settle handle a live one carries.
///
/// It is also the delivery's place in the harness's in-flight count: taken when the delivery was
/// enqueued, released exactly once when this is dropped, whether the delivery was settled or not.
pub(crate) struct Settlement {
    bus: Arc<Bus>,
    /// `None` when the delivery arrived settled: an at-most-once subscription, a request's reply.
    origin: Option<Origin>,
}

impl Settlement {
    fn new(bus: Arc<Bus>, origin: Option<Origin>) -> Self {
        Self { bus, origin }
    }

    /// A delivery that arrived settled, such as the reply to a request.
    pub(crate) fn settled(bus: Arc<Bus>) -> Self {
        Self::new(bus, None)
    }

    /// Settles the delivery as the live link would: an at-most-once delivery reports
    /// [`AckError::Unsupported`], a settlement after the connection shut down reports the ended
    /// session, and a `modified` disposition returns the message with one more counted attempt.
    pub(crate) fn settle(
        self,
        kind: SettleKind,
        payload: Bytes,
        headers: HeaderMap,
        delivery_count: Option<u32>,
    ) -> Result<(), AckError> {
        let Some(origin) = &self.origin else {
            return Err(AckError::Unsupported);
        };
        if self.bus.is_closed() {
            return Err(AckError::Broker(Box::from(
                "the subscription's session ended with the connection",
            )));
        }
        // `accept` and `reject` both take the message off the address; which dead-letter policy a
        // rejection meets is the server's configuration, which a process does not have.
        if matches!(kind, SettleKind::Modify) {
            self.bus.requeue(
                origin.id,
                &origin.address,
                Delivery {
                    payload,
                    headers,
                    count: Some(delivery_count.unwrap_or(0) + 1),
                },
            );
        }
        Ok(())
    }
}

impl Drop for Settlement {
    fn drop(&mut self) {
        if let Some(coordinator) = self.bus.coordinator() {
            coordinator.consumed();
        }
    }
}

impl fmt::Debug for Settlement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Settlement")
            .field("settled", &self.origin.is_none())
            .finish_non_exhaustive()
    }
}
