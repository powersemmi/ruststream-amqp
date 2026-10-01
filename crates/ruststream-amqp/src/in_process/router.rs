//! Subscription registry and routing of the in-process transport.
//!
//! An exact-address match selects the subscriptions a published message reaches, and the terminus
//! each of them declared decides how: an anycast subscription competes for the message with the
//! other anycast subscriptions on that address, and a multicast one always gets its own copy. That
//! is the distinction between a work queue and a broadcast, so it is reproduced rather than
//! flattened: a service that splits work across consumers must not pass a test that a server
//! would fail. A per-address log records everything published, for assertions.
//!
//! An address that a queue subscription attached to holds a queue, as the server auto-creates one
//! for the attach: a message no consumer is there to take waits in it for the next queue
//! subscription, and the queue goes once it is empty with no consumer left. A delivery a consumer
//! dropped without settling stays outstanding on its subscription, as an unsettled delivery stays
//! on its link, and goes back to the queue when the subscription detaches.
//!
//! The registry holds the only sending end of each subscription's channel. Closing the transport
//! empties it, so every subscription's stream yields what it already holds and then ends, as a
//! subscription's stream does when the connection shuts down.

use std::collections::{HashMap, VecDeque};
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
    /// The queue of each address a queue subscription attached to, holding what no consumer took.
    queues: HashMap<String, VecDeque<Delivery>>,
    /// The deliveries each subscription holds unsettled after its consumer dropped them, returned
    /// to the queue when the subscription detaches.
    outstanding: HashMap<SubscriptionId, Vec<Delivery>>,
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

    /// Registers a subscription on `address` and returns its id and the channel it reads. A queue
    /// subscription creates the address's queue, or takes over what the queue holds.
    pub(crate) fn subscribe(
        &self,
        address: String,
        routing: Routing,
        coordinator: Option<&Coordinator>,
    ) -> Option<(SubscriptionId, DeliveryReceiver)> {
        // Unbounded on purpose: link credit keeps messages on the broker rather than dropping or
        // blocking them, and a bounded channel would be the wrong shape for that.
        let (sender, receiver) = mpsc::unbounded_channel();
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let mut state = self.state();
        if state.closed {
            return None;
        }
        if routing == Routing::Anycast {
            let stored = state.queues.entry(address.clone()).or_default();
            for delivery in stored.drain(..) {
                send(&sender, delivery, coordinator);
            }
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

    /// Whether the next message published to `address` reaches each subscription attached there,
    /// in attach order: every topic subscription, and the one queue subscription whose turn it is.
    pub(crate) fn recipients(&self, address: &str) -> Vec<bool> {
        let state = self.state();
        let mut attached: Vec<(SubscriptionId, Routing)> = state
            .subscriptions
            .iter()
            .filter(|(_, sub)| sub.address == address)
            .map(|(id, sub)| (*id, sub.routing))
            .collect();
        attached.sort_unstable_by_key(|(id, _)| *id);
        let competing = attached
            .iter()
            .filter(|(_, routing)| *routing == Routing::Anycast)
            .count();
        let turn = state.anycast_turn.get(address).copied().unwrap_or(0);
        drop(state);
        let mut anycast = 0;
        attached
            .into_iter()
            .map(|(_, routing)| match routing {
                Routing::Multicast => true,
                Routing::Anycast => {
                    let picked = anycast == turn % competing;
                    anycast += 1;
                    picked
                }
            })
            .collect()
    }

    /// Removes a subscription, handing back to its address what it held unsettled: `pending`, the
    /// deliveries its consumer never read, after the ones its consumer dropped. No-op if the id is
    /// unknown (the transport already closed).
    pub(crate) fn unsubscribe(
        &self,
        id: SubscriptionId,
        pending: Vec<Delivery>,
        coordinator: Option<&Coordinator>,
    ) {
        let mut state = self.state();
        let Some(sub) = state.subscriptions.remove(&id) else {
            return;
        };
        let held = state.outstanding.remove(&id).unwrap_or_default();
        if sub.routing == Routing::Anycast {
            for delivery in held.into_iter().chain(pending) {
                hand_to_one(&mut state, &sub.address, &delivery, coordinator);
            }
        }
        let consumed = !state
            .subscriptions
            .values()
            .any(|other| other.address == sub.address && other.routing == Routing::Anycast);
        if consumed
            && state
                .queues
                .get(&sub.address)
                .is_some_and(VecDeque::is_empty)
        {
            state.queues.remove(&sub.address);
        }
        drop(state);
    }

    /// Keeps a delivery its consumer dropped without settling on the subscription that had it,
    /// until that subscription detaches; one that detached already hands it back at once.
    pub(crate) fn release(
        &self,
        id: SubscriptionId,
        address: &str,
        delivery: Delivery,
        coordinator: Option<&Coordinator>,
    ) {
        let mut state = self.state();
        if state.closed {
            return;
        }
        if state.subscriptions.contains_key(&id) {
            state.outstanding.entry(id).or_default().push(delivery);
        } else {
            hand_to_one(&mut state, address, &delivery, coordinator);
        }
        drop(state);
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
        state.queues.clear();
        state.outstanding.clear();
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
/// than lose it, so the rotation continues until one takes it. With no consumer to take it, the
/// address's queue keeps it, where the address has one.
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
        if let Some(queue) = state.queues.get_mut(address) {
            queue.push_back(delivery.clone());
        }
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
            return;
        }
    }
    if let Some(queue) = state.queues.get_mut(address) {
        queue.push_back(delivery.clone());
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
