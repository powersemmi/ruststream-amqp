//! Subscription registry and routing of the in-process transport.
//!
//! An exact-address match selects the subscriptions a published message reaches, and the terminus
//! each of them declared decides how: an anycast subscription competes for the message with the
//! other anycast subscriptions on that address, and a multicast one always gets its own copy. That
//! is the distinction between a work queue and a broadcast, so it is reproduced rather than
//! flattened: a service that splits work across consumers must not pass a test that a server
//! would fail. A per-address log records everything published, for assertions.
//!
//! The registry holds the only sending end of each subscription's channel. Closing the transport
//! empties it, so every subscription's stream yields what it already holds and then ends, as a
//! subscription's stream does when the connection shuts down.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use bytes::Bytes;
use ruststream::testing::Coordinator;
use ruststream::{HeaderMap, RawMessage};
use tokio::sync::mpsc;

use crate::address::Routing;

/// Opaque handle identifying one subscription inside an [`AddressRouter`].
///
/// Ordered by attach order, which is the order competing consumers take their turns in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SubscriptionId(u64);

/// One delivery handed to a subscription.
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) payload: Bytes,
    pub(crate) headers: HeaderMap,
    /// The `delivery-count` of the message's `header` section: failed delivery attempts counted so
    /// far. A message this crate published carries no header section, so a fresh one has none,
    /// and a delivery settled with `modified` and `delivery-failed` comes back with one more.
    pub(crate) count: Option<u32>,
}

type DeliverySender = mpsc::UnboundedSender<Delivery>;
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
    /// Set by `close` under the lock, so a subscribe or a publish either lands before the
    /// shutdown or is refused.
    closed: bool,
}

/// In-memory exact-address router.
#[derive(Default)]
pub(crate) struct AddressRouter {
    state: Mutex<RouterState>,
    next_id: AtomicU64,
}

impl AddressRouter {
    fn state(&self) -> MutexGuard<'_, RouterState> {
        self.state
            .lock()
            .expect("amqp in-process router mutex poisoned")
    }

    /// Registers a subscription on `address` and returns its id and the channel it reads.
    pub(crate) fn subscribe(
        &self,
        address: String,
        routing: Routing,
    ) -> Option<(SubscriptionId, DeliveryReceiver)> {
        // Unbounded on purpose: link credit keeps messages on the broker rather than dropping or
        // blocking them, and a bounded channel would be the wrong shape for that.
        let (sender, receiver) = mpsc::unbounded_channel();
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let mut state = self.state();
        if state.closed {
            return None;
        }
        state.subscriptions.insert(
            id,
            Subscription {
                address,
                routing,
                sender,
            },
        );
        drop(state);
        Some((id, receiver))
    }

    /// How many queue and topic subscriptions are attached to `address` now.
    pub(crate) fn termini(&self, address: &str) -> (usize, usize) {
        let state = self.state();
        let on = || {
            state
                .subscriptions
                .values()
                .filter(|sub| sub.address == address)
        };
        let anycast = on().filter(|sub| sub.routing == Routing::Anycast).count();
        let multicast = on().filter(|sub| sub.routing == Routing::Multicast).count();
        drop(state);
        (anycast, multicast)
    }

    /// Removes a subscription. No-op if the id is unknown (the transport already closed).
    pub(crate) fn unsubscribe(&self, id: SubscriptionId) {
        self.state().subscriptions.remove(&id);
    }

    /// Records `delivery` under `address` and routes it: every multicast subscription gets a copy,
    /// and the anycast ones share, one message each in turn. Every enqueue is counted with
    /// [`Coordinator::enqueued`] under a harness run.
    pub(crate) fn publish(
        &self,
        address: &str,
        delivery: &Delivery,
        coordinator: Option<&Coordinator>,
    ) -> bool {
        let mut state = self.state();
        if state.closed {
            return false;
        }
        publish_one(&mut state, address, delivery, coordinator);
        drop(state);
        true
    }

    /// Publishes every message of a committed transaction in one step with the check for
    /// shutdown: all of them, or none once the connection has shut down.
    pub(crate) fn publish_all<'a>(
        &self,
        messages: impl IntoIterator<Item = (&'a str, &'a Delivery)>,
        coordinator: Option<&Coordinator>,
    ) -> bool {
        let mut state = self.state();
        if state.closed {
            return false;
        }
        for (address, delivery) in messages {
            publish_one(&mut state, address, delivery, coordinator);
        }
        drop(state);
        true
    }

    /// Returns a released delivery to the subscription that had it, or, when that one is gone, to
    /// another anycast subscription on its address: the broker keeps a released message and hands
    /// it to a consumer that is still attached.
    ///
    /// Refuses once the connection has shut down, in one step with the return, so a release
    /// never reports success into a router that has let its subscriptions go.
    pub(crate) fn requeue(
        &self,
        id: SubscriptionId,
        address: &str,
        delivery: Delivery,
        coordinator: Option<&Coordinator>,
    ) -> bool {
        let mut state = self.state();
        if state.closed {
            return false;
        }
        if let Some(sub) = state.subscriptions.get(&id) {
            send(&sub.sender, delivery, coordinator);
        } else {
            hand_to_one(&mut state, address, &delivery, coordinator);
        }
        drop(state);
        true
    }

    /// Returns every message recorded for `address`, in publish order.
    pub(crate) fn published(&self, address: &str) -> Vec<RawMessage> {
        self.state().log.get(address).cloned().unwrap_or_default()
    }

    /// Drops every subscription, which ends their streams once they have yielded what they hold.
    /// The log stays readable.
    pub(crate) fn close(&self) {
        let mut state = self.state();
        state.closed = true;
        state.subscriptions.clear();
        state.anycast_turn.clear();
    }
}

/// Logs one message on `address` and hands it to the subscriptions there: a copy to every
/// multicast one, and the message to one anycast one in turn.
fn publish_one(
    state: &mut RouterState,
    address: &str,
    delivery: &Delivery,
    coordinator: Option<&Coordinator>,
) {
    let snapshot = RawMessage::new(address.to_owned(), delivery.payload.clone())
        .with_headers(delivery.headers.clone());
    state
        .log
        .entry(address.to_owned())
        .or_default()
        .push(snapshot);
    for sub in state.subscriptions.values() {
        if sub.address == address && sub.routing == Routing::Multicast {
            send(&sub.sender, delivery.clone(), coordinator);
        }
    }
    hand_to_one(state, address, delivery, coordinator);
}

/// Hands `delivery` to one of the competing consumers of `address`, in turn. A send fails only
/// when that consumer is already gone, and a broker hands the message to another consumer rather
/// than lose it, so the rotation continues until one takes it.
fn hand_to_one(
    state: &mut RouterState,
    address: &str,
    delivery: &Delivery,
    coordinator: Option<&Coordinator>,
) {
    let mut competing: Vec<(SubscriptionId, DeliverySender)> = state
        .subscriptions
        .iter()
        .filter(|(_, sub)| sub.address == address && sub.routing == Routing::Anycast)
        .map(|(id, sub)| (*id, sub.sender.clone()))
        .collect();
    if competing.is_empty() {
        return;
    }
    // Attach order, so the rotation is the consumers' own order rather than the map's.
    competing.sort_unstable_by_key(|(id, _)| *id);
    let turn = state.anycast_turn.entry(address.to_owned()).or_insert(0);
    let first = *turn;
    *turn = turn.wrapping_add(1);
    for offset in 0..competing.len() {
        let (_, sender) = &competing[first.wrapping_add(offset) % competing.len()];
        if send(sender, delivery.clone(), coordinator) {
            break;
        }
    }
}

/// Sends one delivery, counting it with the harness when it was taken.
fn send(sender: &DeliverySender, delivery: Delivery, coordinator: Option<&Coordinator>) -> bool {
    // Counted before the send: a consumer on another task may settle the delivery, and release
    // its count, before `send` returns here.
    if let Some(coordinator) = coordinator {
        coordinator.enqueued();
    }
    let sent = sender.send(delivery).is_ok();
    if !sent && let Some(coordinator) = coordinator {
        coordinator.consumed();
    }
    sent
}

impl fmt::Debug for AddressRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state();
        f.debug_struct("AddressRouter")
            .field("subscriptions", &state.subscriptions.len())
            .field("logged_addresses", &state.log.len())
            .finish_non_exhaustive()
    }
}
