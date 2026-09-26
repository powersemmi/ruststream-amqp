//! [`Bus`]: the shared state of one in-process connection.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use ruststream::RawMessage;
use ruststream::testing::Coordinator;

use super::router::{AddressRouter, Delivery, DeliveryReceiver, SubscriptionId};
use crate::address::Routing;
use crate::error::AmqpError;

/// What an in-process connection is: the router every handle on it routes through, the harness
/// coordinator it counts deliveries with, and whether it is still open.
#[derive(Debug, Default)]
pub(crate) struct Bus {
    router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
    closed: AtomicBool,
    /// Names the private reply addresses of requests, as the peer names a dynamic terminus.
    reply_seq: AtomicU64,
}

impl Bus {
    pub(crate) fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    /// Installs the harness coordinator. A second install is ignored.
    pub(crate) fn install(&self, coordinator: Coordinator) {
        let _ = self.coordinator.set(coordinator);
    }

    /// `Ok` while the connection is open, and [`AmqpError::NotConnected`] once it has shut down:
    /// the error a live handle reports when it outlived its connection.
    pub(crate) fn ensure_live(&self) -> Result<(), AmqpError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(AmqpError::NotConnected);
        }
        Ok(())
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Shuts the connection down: nothing routes again, and every subscription's stream ends.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.router.close();
    }

    /// Opens a subscription, or refuses once the connection has shut down: the check and the
    /// registration are one step under the router's lock, so shutdown cannot fall between them.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] once the connection has shut down.
    pub(crate) fn subscribe(
        &self,
        address: String,
        routing: Routing,
    ) -> Result<(SubscriptionId, DeliveryReceiver), AmqpError> {
        self.router
            .subscribe(address, routing)
            .ok_or(AmqpError::NotConnected)
    }

    /// How many queue and topic subscriptions are attached to `address` now.
    pub(crate) fn termini(&self, address: &str) -> (usize, usize) {
        self.router.termini(address)
    }

    pub(crate) fn unsubscribe(&self, id: SubscriptionId) {
        self.router.unsubscribe(id);
    }

    /// Routes one framed message to the subscriptions on `address`, or refuses once the
    /// connection has shut down, in one step with the routing.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] once the connection has shut down.
    pub(crate) fn route(&self, address: &str, delivery: &Delivery) -> Result<(), AmqpError> {
        if self.router.publish(address, delivery, self.coordinator()) {
            Ok(())
        } else {
            Err(AmqpError::NotConnected)
        }
    }

    /// Returns a released delivery to its address.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] once the connection has shut down.
    pub(crate) fn requeue(
        &self,
        id: SubscriptionId,
        address: &str,
        delivery: Delivery,
    ) -> Result<(), AmqpError> {
        if self
            .router
            .requeue(id, address, delivery, self.coordinator())
        {
            Ok(())
        } else {
            Err(AmqpError::NotConnected)
        }
    }

    /// Routes a committed transaction's messages, all of them or none once the connection has
    /// shut down.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] once the connection has shut down.
    pub(crate) fn route_all(&self, messages: &[(String, Delivery)]) -> Result<(), AmqpError> {
        let messages = messages
            .iter()
            .map(|(address, delivery)| (address.as_str(), delivery));
        if self.router.publish_all(messages, self.coordinator()) {
            Ok(())
        } else {
            Err(AmqpError::NotConnected)
        }
    }

    pub(crate) fn published(&self, address: &str) -> Vec<RawMessage> {
        self.router.published(address)
    }

    pub(crate) fn next_reply_address(&self) -> String {
        let seq = self.reply_seq.fetch_add(1, Ordering::Relaxed);
        format!("in-process-reply-{seq}")
    }
}
