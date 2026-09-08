//! [`AmqpTestSubscriber`] and [`AmqpTestMessage`].

use std::future::{Future, ready};
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};

use futures::Stream;

use ruststream::{
    AckError, BatchSubscriber, BufferedSubscriber, HeaderMap, IncomingMessage, Partitioned,
    Subscriber, testing::Coordinator,
};

use crate::error::AmqpError;
use crate::testing::broker::TestState;
use crate::testing::router::{Delivery, DeliveryReceiver, DeliverySender, SubscriptionId};
use crate::{DEFAULT_BATCH_WAIT, PARTITION_KEY_HEADER};

/// Subscriber returned by [`ConnectedAmqpTestBroker`](crate::testing::ConnectedAmqpTestBroker).
///
/// Dropping it unregisters the subscription, so handlers stop receiving as soon as their task
/// finishes.
pub struct AmqpTestSubscriber {
    state: Arc<TestState>,
    id: SubscriptionId,
    deliveries: BufferedSubscriber<Deliveries>,
}

impl std::fmt::Debug for AmqpTestSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpTestSubscriber").finish_non_exhaustive()
    }
}

impl AmqpTestSubscriber {
    pub(crate) fn new(
        state: Arc<TestState>,
        id: SubscriptionId,
        rx: DeliveryReceiver,
        requeue: DeliverySender,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            state,
            id,
            deliveries: BufferedSubscriber::new(Deliveries {
                rx,
                requeue,
                coordinator,
            })
            .max_wait(DEFAULT_BATCH_WAIT),
        }
    }
}

impl Drop for AmqpTestSubscriber {
    fn drop(&mut self) {
        self.state.router.unsubscribe(self.id);
    }
}

impl Subscriber for AmqpTestSubscriber {
    type Message = AmqpTestMessage;
    type Error = AmqpError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.deliveries.stream()
    }
}

/// Batches come from the same client-side buffer the real subscriber uses, so a batch handler runs
/// against this broker exactly as it does against a server.
impl BatchSubscriber for AmqpTestSubscriber {
    type Batch = Vec<AmqpTestMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        self.deliveries.batches(size)
    }
}

/// The routed stream under the buffer: one delivery per item, off the subscription's channel.
struct Deliveries {
    rx: DeliveryReceiver,
    requeue: DeliverySender,
    /// A clone of the broker's harness coordinator, threaded into each yielded message so a
    /// requeue re-counts and a consumed delivery decrements. `None` outside a harness run.
    coordinator: Option<Coordinator>,
}

impl Subscriber for Deliveries {
    type Message = AmqpTestMessage;
    type Error = AmqpError;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        let requeue = self.requeue.clone();
        let coordinator = self.coordinator.clone();
        // Poll the receiver in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| {
            self.rx.poll_recv(cx).map(|next| {
                next.map(|delivery| {
                    Ok(AmqpTestMessage::new(
                        delivery,
                        requeue.clone(),
                        coordinator.clone(),
                    ))
                })
            })
        })
    }
}

/// Message handed to handlers from an [`AmqpTestSubscriber`].
///
/// `ack` consumes the handle; `nack(requeue = true)` re-queues the delivery on the owning
/// subscription's channel so the next handler invocation sees it again; `nack(requeue = false)`
/// drops it, matching the real subscriber's reject path in effect.
pub struct AmqpTestMessage {
    delivery: Option<Delivery>,
    requeue: DeliverySender,
    /// A clone of the broker's harness coordinator. When set, this delivery is counted in
    /// flight and is decremented exactly once when the message is consumed or dropped.
    coordinator: Option<Coordinator>,
}

impl Drop for AmqpTestMessage {
    /// Counts this delivery consumed exactly once: on ack, nack, or an unsettled drop. A
    /// requeue re-enqueues a fresh delivery first, so the in-flight count stays balanced.
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            coordinator.consumed();
        }
    }
}

impl std::fmt::Debug for AmqpTestMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpTestMessage").finish_non_exhaustive()
    }
}

impl AmqpTestMessage {
    pub(crate) fn new(
        delivery: Delivery,
        requeue: DeliverySender,
        coordinator: Option<Coordinator>,
    ) -> Self {
        Self {
            delivery: Some(delivery),
            requeue,
            coordinator,
        }
    }
}

impl Partitioned for AmqpTestMessage {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

impl IncomingMessage for AmqpTestMessage {
    fn payload(&self) -> &[u8] {
        self.delivery
            .as_ref()
            .map(|d| d.payload.as_ref())
            .unwrap_or_default()
    }

    fn headers(&self) -> &HeaderMap {
        static EMPTY: OnceLock<HeaderMap> = OnceLock::new();
        self.delivery
            .as_ref()
            .map_or_else(|| EMPTY.get_or_init(HeaderMap::new), |d| &d.headers)
    }

    fn ack(mut self) -> impl Future<Output = Result<(), AckError>> {
        self.delivery.take();
        ready(Ok(()))
    }

    fn nack(mut self, requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        let delivery = self
            .delivery
            .take()
            .expect("AmqpTestMessage ack/nack invoked twice");
        if requeue {
            let sent = self.requeue.send(delivery);
            // The requeue bypasses fanout, so count the re-enqueue here to balance this
            // message's `Drop` decrement. The redelivered copy is consumed in turn.
            if sent.is_ok()
                && let Some(coordinator) = &self.coordinator
            {
                coordinator.enqueued();
            }
        }
        ready(Ok(()))
    }

    fn partition_key(&self) -> Option<&[u8]> {
        Partitioned::partition_key(self)
    }
}
