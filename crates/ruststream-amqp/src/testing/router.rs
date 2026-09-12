//! Subscription registry and routing for the in-process `AMQP` stand-in.
//!
//! An exact-address match selects the subscriptions a published message reaches, and the terminus
//! each of them declared decides how: an anycast subscription competes for the message with the
//! other anycast subscriptions on that address, and a multicast one always gets its own copy. That
//! is the distinction between a work queue and a broadcast, so it is reproduced rather than
//! flattened - a service that splits work across consumers must not pass a test that a server
//! would fail. A per-address log records everything published, for assertions.
//!
//! What the registry does not have is a broker's storage, and two consequences follow. A message
//! published to an address with no live subscription is logged and dropped, where a server would
//! hold it until a consumer attaches, so a test opens its subscriptions before publishing. And a
//! released delivery (`nack(requeue = true)`) returns to the subscription that had it rather than
//! to the address, so a test cannot assert that a competing consumer picks up what another
//! released - that is broker-side redelivery, and the live suite covers it.

use std::collections::HashMap;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use bytes::Bytes;
use ruststream::{HeaderMap, RawMessage, testing::Coordinator};
use tokio::sync::mpsc;

use crate::address::Routing;

/// Opaque handle identifying one subscription inside an [`AddressRouter`].
///
/// Ordered by attach order, which is the order competing consumers take their turns in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SubscriptionId(u64);

/// Single delivery handed to a matching subscriber.
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) payload: Bytes,
    pub(crate) headers: HeaderMap,
}

pub(crate) type DeliverySender = mpsc::UnboundedSender<Delivery>;
pub(crate) type DeliveryReceiver = mpsc::UnboundedReceiver<Delivery>;

struct Subscription {
    address: String,
    routing: Routing,
    sender: DeliverySender,
}

#[derive(Default)]
struct RouterState {
    subscriptions: HashMap<SubscriptionId, Subscription>,
    log: HashMap<String, Vec<RawMessage>>,
    /// Whose turn it is among the competing consumers of an address, so a work queue spreads its
    /// traffic instead of always picking the same one.
    anycast_turn: HashMap<String, usize>,
}

/// In-memory exact-address router.
#[derive(Default)]
pub(crate) struct AddressRouter {
    state: Mutex<RouterState>,
    next_id: AtomicU64,
}

impl AddressRouter {
    /// Registers a subscription on `address` and returns the channel pair the subscriber will
    /// use, together with the [`SubscriptionId`] needed to unsubscribe.
    ///
    /// The returned [`DeliverySender`] is the same one fanout uses, so subscribers can re-send
    /// a delivery into their own queue to implement `nack(requeue = true)`.
    pub(crate) fn subscribe(
        &self,
        address: String,
        routing: Routing,
    ) -> (SubscriptionId, DeliverySender, DeliveryReceiver) {
        // Unbounded on purpose: a bounded channel would be the wrong shape for link credit, which
        // holds messages on the broker rather than dropping or blocking. See the module docs.
        let (tx, rx) = mpsc::unbounded_channel();
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.state
            .lock()
            .expect("amqp test router mutex poisoned")
            .subscriptions
            .insert(
                id,
                Subscription {
                    address,
                    routing,
                    sender: tx.clone(),
                },
            );
        (id, tx, rx)
    }

    /// Removes a subscription. No-op if the id is unknown (double-drop of the subscriber).
    pub(crate) fn unsubscribe(&self, id: SubscriptionId) {
        self.state
            .lock()
            .expect("amqp test router mutex poisoned")
            .subscriptions
            .remove(&id);
    }

    /// Routes `payload` to the subscriptions on `address` and records it in the published log:
    /// every multicast subscription gets a copy, and the anycast ones share, one message each in
    /// turn. Under a harness run every live enqueue is counted with [`Coordinator::enqueued`].
    pub(crate) fn publish(
        &self,
        address: &str,
        payload: Bytes,
        headers: HeaderMap,
        coordinator: Option<&Coordinator>,
    ) {
        let snapshot = RawMessage::new(address, payload.clone()).with_headers(headers.clone());
        let mut copies: Vec<DeliverySender> = Vec::new();
        let mut competing: Vec<(SubscriptionId, DeliverySender)> = Vec::new();
        let turn = {
            let mut state = self.state.lock().expect("amqp test router mutex poisoned");
            state
                .log
                .entry(address.to_owned())
                .or_default()
                .push(snapshot);
            for (id, sub) in &state.subscriptions {
                if sub.address != address {
                    continue;
                }
                match sub.routing {
                    Routing::Multicast => copies.push(sub.sender.clone()),
                    Routing::Anycast => competing.push((*id, sub.sender.clone())),
                }
            }
            // Attach order, so the rotation below is the consumers' own order rather than
            // whatever order the map happens to iterate in.
            competing.sort_unstable_by_key(|(id, _)| *id);
            let next = {
                let turn = state.anycast_turn.entry(address.to_owned()).or_insert(0);
                let next = *turn;
                *turn = turn.wrapping_add(1);
                next
            };
            drop(state);
            next
        };

        let delivery = Delivery { payload, headers };
        for tx in copies {
            if tx.send(delivery.clone()).is_ok()
                && let Some(coordinator) = coordinator
            {
                coordinator.enqueued();
            }
        }

        // One of the competing consumers takes it. A send fails only when that subscriber is
        // already gone, and a broker would hand the message to another consumer rather than lose
        // it, so the rotation continues until one takes it.
        for offset in 0..competing.len() {
            let (_, tx) = &competing[(turn.wrapping_add(offset)) % competing.len()];
            if tx.send(delivery.clone()).is_ok() {
                if let Some(coordinator) = coordinator {
                    coordinator.enqueued();
                }
                break;
            }
        }
    }

    /// Returns every message recorded for `address`, in publish order.
    pub(crate) fn published(&self, address: &str) -> Vec<RawMessage> {
        self.state
            .lock()
            .expect("amqp test router mutex poisoned")
            .log
            .get(address)
            .cloned()
            .unwrap_or_default()
    }

    /// Drops every subscription and clears the published log. Used by broker shutdown.
    pub(crate) fn clear(&self) {
        let mut state = self.state.lock().expect("amqp test router mutex poisoned");
        state.subscriptions.clear();
        state.log.clear();
        state.anycast_turn.clear();
    }
}

impl std::fmt::Debug for AddressRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().expect("amqp test router mutex poisoned");
        f.debug_struct("AddressRouter")
            .field("subscriptions", &state.subscriptions.len())
            .field("logged_addresses", &state.log.len())
            .finish_non_exhaustive()
    }
}
