//! How a live delivery settles: the handle ships a disposition to its subscription's pump task and
//! waits for the answer the client gives.
//!
//! The client settles through the receiver the pump owns, so a handle cannot dispose of its
//! delivery itself. Every handle of a subscription shares one [`SettleLine`]: the command channel
//! to the pump and the answer slots the pump writes into. A slot belongs to the subscription and
//! is reused, and the subscription holds at most its link credit of them, so settling a delivery
//! allocates nothing once those slots exist.

use std::fmt;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_queue::ArrayQueue;

use fe2o3_amqp::link::delivery::DeliveryInfo;
use multishot::{Receiver, Recv, Sender};
use ruststream::AckError;
use tokio::sync::{Notify, mpsc};

/// How a delivered message asks its pump task to settle it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SettleKind {
    /// Accept the delivery (ack).
    Accept,
    /// Hand the delivery back for redelivery and count the attempt as failed (nack with requeue).
    ///
    /// The disposition is `modified` with `delivery-failed`, not `released`. `released` is the
    /// protocol's way of saying the delivery was not acted upon at all, and the peer leaves
    /// `delivery-count` where it was; a handler that answered `retry()` did act on it and failed,
    /// so the attempt has to be counted. Without that the registration's `max_attempts(..)` cap
    /// reads the same count on every redelivery and a poison message circulates forever.
    Modify,
    /// Reject the delivery as undeliverable (nack without requeue); the broker's dead-letter
    /// policy decides what happens next.
    Reject,
}

/// What the client answered for one settlement.
pub(crate) type Answer = Result<(), AckError>;

/// A settlement request shipped from a message handle to the subscription's pump task.
pub(crate) struct SettleCmd<Delivery = DeliveryInfo> {
    pub(crate) delivery: Delivery,
    pub(crate) kind: SettleKind,
    pub(crate) reply: Reply,
}

/// Where the pump writes the answer to one settlement.
pub(crate) struct Reply(Sender<Answer>);

impl Reply {
    /// Hands the client's answer back to the handle that asked.
    pub(crate) fn send(self, answer: Answer) {
        // A handle that stopped waiting dropped its slot with it, and the answer goes nowhere.
        self.0.send(answer);
    }
}

impl<Delivery: fmt::Debug> fmt::Debug for SettleCmd<Delivery> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SettleCmd")
            .field("delivery", &self.delivery)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// The pump's end of a [`SettleLine`].
pub(crate) type SettleCommands<Delivery = DeliveryInfo> =
    mpsc::UnboundedReceiver<SettleCmd<Delivery>>;

/// The settlement path every handle of one subscription shares.
///
/// The pump holds one reference while it receives and every unsettled handle holds one more. The
/// command channel closes when the last of them is gone, which is how the pump's drain learns that
/// no settlement can arrive any more.
pub(crate) struct SettleLine<Delivery = DeliveryInfo> {
    commands: mpsc::UnboundedSender<SettleCmd<Delivery>>,
    /// The answer slots no settlement is using, as many as the subscription's credit. A
    /// settlement takes one and puts it back once it has read its answer.
    idle: ArrayQueue<Receiver<Answer>>,
    /// The slots that exist, idle or in use. They are made on first need, so a subscription that
    /// never settles many deliveries at once never makes the credit's worth.
    made: AtomicUsize,
    /// How many settlements wait for a slot to come free.
    waiting: AtomicUsize,
    /// Wakes a waiting settlement when a slot comes free.
    freed: Notify,
}

impl<Delivery> fmt::Debug for SettleLine<Delivery> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SettleLine").finish_non_exhaustive()
    }
}

impl<Delivery> SettleLine<Delivery> {
    /// A line for a new subscription, with a slot for each delivery its `credit` lets the broker
    /// send ahead, and the end its pump reads commands from.
    pub(crate) fn open(credit: NonZeroUsize) -> (Arc<Self>, SettleCommands<Delivery>) {
        let (commands, received) = mpsc::unbounded_channel();
        let line = Self {
            commands,
            idle: ArrayQueue::new(credit.get()),
            made: AtomicUsize::new(0),
            waiting: AtomicUsize::new(0),
            freed: Notify::new(),
        };
        (Arc::new(line), received)
    }

    /// Asks the pump to settle `delivery` and waits for the client's answer.
    ///
    /// The deliveries held unsettled can outnumber the credit, so more settlements than there
    /// are slots can run at once. One beyond them waits until a slot comes free rather than
    /// making another, so the line never holds more slots than the credit.
    ///
    /// # Errors
    ///
    /// The client's refusal, or [`AckError::Broker`] when the pump has stopped: the subscription
    /// ended or the connection shut down, and the settlement can no longer reach the peer.
    pub(crate) async fn settle(&self, delivery: Delivery, kind: SettleKind) -> Answer {
        let (mut slot, reply) = self.take_slot().await;
        self.commands
            .send(SettleCmd {
                delivery,
                kind,
                reply: Reply(reply),
            })
            .map_err(|_| pump_stopped())?;
        let answer = slot.recv().await;
        slot.release();
        answer.map_err(|_| pump_stopped())?
    }

    /// A free slot and the sender its answer arrives through, once one is free.
    async fn take_slot(&self) -> (Taken<'_, Delivery>, Sender<Answer>) {
        loop {
            if let Some(taken) = self.try_take() {
                return taken;
            }
            let mut freed = pin!(self.freed.notified());
            // Registered before the second look, so a slot freed in between still wakes it.
            freed.as_mut().enable();
            let _waiting = Waiting::enter(&self.waiting);
            if let Some(taken) = self.try_take() {
                return taken;
            }
            freed.await;
        }
    }

    fn try_take(&self) -> Option<(Taken<'_, Delivery>, Sender<Answer>)> {
        let mut slot = self.idle.pop().or_else(|| self.make_slot())?;
        // A slot goes back only after its answer was read, so its sender is always available
        // again; a fresh receiver stands in for one that would say otherwise.
        let reply = slot.sender().unwrap_or_else(|| {
            slot = Receiver::new();
            slot.sender()
                .expect("a new receiver hands out its first sender")
        });
        let taken = Taken {
            line: self,
            slot: Some(slot),
        };
        Some((taken, reply))
    }

    /// A new slot, while the line holds fewer than the credit.
    fn make_slot(&self) -> Option<Receiver<Answer>> {
        let mut made = self.made.load(Ordering::SeqCst);
        while made < self.idle.capacity() {
            match self.made.compare_exchange_weak(
                made,
                made + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Some(Receiver::new()),
                Err(current) => made = current,
            }
        }
        None
    }

    /// Wakes a settlement waiting for a slot, if one waits.
    fn wake_one(&self) {
        // The load pairs with the waiter's increment before its second look at the slots: either
        // that look sees the slot this caller just freed, or this load sees the waiter.
        if self.waiting.load(Ordering::SeqCst) > 0 {
            self.freed.notify_one();
        }
    }

    /// How many answer slots wait for their next settlement.
    #[cfg(test)]
    fn idle_slots(&self) -> usize {
        self.idle.len()
    }
}

/// A slot one settlement holds.
///
/// Released once the answer is read. A settlement dropped before that gives its slot up instead:
/// the pump may still write into it, so it must not serve another settlement, and the line may
/// make a new one in its place.
struct Taken<'line, Delivery> {
    line: &'line SettleLine<Delivery>,
    slot: Option<Receiver<Answer>>,
}

impl<Delivery> Taken<'_, Delivery> {
    fn recv(&mut self) -> Recv<'_, Answer> {
        self.slot
            .as_mut()
            .expect("a taken slot is held until it is released")
            .recv()
    }

    fn release(mut self) {
        if let Some(slot) = self.slot.take()
            && self.line.idle.push(slot).is_err()
        {
            // The line never makes more slots than the queue holds, so this does not happen;
            // dropping the slot keeps the count true if it ever did.
            self.line.made.fetch_sub(1, Ordering::SeqCst);
        }
        self.line.wake_one();
    }
}

impl<Delivery> Drop for Taken<'_, Delivery> {
    fn drop(&mut self) {
        if self.slot.take().is_some() {
            self.line.made.fetch_sub(1, Ordering::SeqCst);
            self.line.wake_one();
        }
    }
}

/// A settlement counted among those waiting for a slot while it lives.
struct Waiting<'line>(&'line AtomicUsize);

impl<'line> Waiting<'line> {
    fn enter(waiting: &'line AtomicUsize) -> Self {
        waiting.fetch_add(1, Ordering::SeqCst);
        Self(waiting)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn pump_stopped() -> AckError {
    AckError::Broker(Box::from("the subscription's pump task has shut down"))
}

#[cfg(test)]
mod tests {
    use futures::future::{join_all, poll_immediate};

    use super::*;

    const ONE: NonZeroUsize = NonZeroUsize::MIN;

    /// A pump that answers every settlement with the delivery it names: an even delivery is
    /// accepted and an odd one refused with its own number, so a handle that got another
    /// handle's answer sees the wrong outcome.
    fn answer_by_number(cmd: SettleCmd<u32>) {
        let answer = if cmd.delivery.is_multiple_of(2) {
            Ok(())
        } else {
            Err(AckError::Broker(Box::from(cmd.delivery.to_string())))
        };
        cmd.reply.send(answer);
    }

    fn check(delivery: u32, answer: &Answer) {
        match answer {
            Ok(()) => assert!(
                delivery.is_multiple_of(2),
                "delivery {delivery} was accepted"
            ),
            Err(AckError::Broker(reason)) => {
                assert_eq!(reason.to_string(), delivery.to_string());
            }
            Err(other) => panic!("delivery {delivery} got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_settled_answer_slot_is_reused() {
        let (line, mut commands) = SettleLine::<u32>::open(ONE);
        let pump = tokio::spawn(async move {
            while let Some(cmd) = commands.recv().await {
                answer_by_number(cmd);
            }
        });

        for delivery in 0..4 {
            check(delivery, &line.settle(delivery, SettleKind::Accept).await);
            assert_eq!(
                line.idle_slots(),
                1,
                "one settlement at a time needs one slot, whatever the number of deliveries"
            );
        }

        drop(line);
        pump.await.expect("the pump ends with the line");
    }

    /// Settlements from many tasks at once, answered by the pump in an order of its own, over
    /// rounds that reuse the slots of the rounds before: each handle gets the answer to its own
    /// settlement.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_settlements_each_get_their_own_answer() {
        const AT_ONCE: u32 = 64;
        let (line, mut commands) = SettleLine::<u32>::open(NonZeroUsize::new(64).unwrap());
        let pump = tokio::spawn(async move {
            let mut waiting = Vec::new();
            while let Some(cmd) = commands.recv().await {
                waiting.push(cmd);
                if waiting.len() == AT_ONCE as usize {
                    // Last asked, first answered: the answers do not follow the commands.
                    while let Some(cmd) = waiting.pop() {
                        answer_by_number(cmd);
                    }
                }
            }
        });

        for round in 0..8 {
            let settlements = (0..AT_ONCE).map(|n| {
                let line = Arc::clone(&line);
                let delivery = round * AT_ONCE + n;
                tokio::spawn(async move {
                    let answer = line.settle(delivery, SettleKind::Accept).await;
                    check(delivery, &answer);
                })
            });
            for settled in join_all(settlements).await {
                settled.expect("every settlement got its own answer");
            }
        }
        assert!(
            line.idle_slots() <= AT_ONCE as usize,
            "the slots never outnumber the settlements that were in flight at once"
        );

        drop(line);
        pump.await.expect("the pump ends with the line");
    }

    /// More settlements at once than the credit: no more slots exist than the credit allows,
    /// and every settlement still gets its own answer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_slots_never_outnumber_the_credit() {
        const CREDIT: usize = 2;
        let (line, mut commands) = SettleLine::<u32>::open(NonZeroUsize::new(CREDIT).unwrap());
        let pump = tokio::spawn(async move {
            let mut waiting = Vec::new();
            while let Some(cmd) = commands.recv().await {
                waiting.push(cmd);
                if waiting.len() == CREDIT {
                    while let Some(cmd) = waiting.pop() {
                        answer_by_number(cmd);
                    }
                }
            }
        });

        let settlements = (0..16).map(|delivery| {
            let line = Arc::clone(&line);
            tokio::spawn(async move {
                check(delivery, &line.settle(delivery, SettleKind::Accept).await);
            })
        });
        for settled in join_all(settlements).await {
            settled.expect("every settlement got its own answer");
        }
        assert_eq!(
            line.idle_slots(),
            CREDIT,
            "the credit's worth of slots, and no more"
        );

        drop(line);
        pump.await.expect("the pump ends with the line");
    }

    /// A settlement beyond the credit waits for a slot to come free instead of making one, and
    /// goes out as soon as one does.
    #[tokio::test]
    async fn a_settlement_beyond_the_credit_waits_for_a_free_slot() {
        let (line, mut commands) = SettleLine::<u32>::open(ONE);
        let mut first = pin!(line.settle(0, SettleKind::Accept));
        assert!(poll_immediate(&mut first).await.is_none());
        let held = commands
            .recv()
            .await
            .expect("the first command reached the pump");

        let mut second = pin!(line.settle(2, SettleKind::Accept));
        assert!(poll_immediate(&mut second).await.is_none());
        assert!(
            poll_immediate(commands.recv()).await.is_none(),
            "the second settlement waits for the only slot"
        );

        answer_by_number(held);
        check(0, &first.await);
        assert!(poll_immediate(&mut second).await.is_none());
        answer_by_number(
            poll_immediate(commands.recv())
                .await
                .flatten()
                .expect("the freed slot let the second settlement out"),
        );
        check(2, &second.await);
        assert_eq!(line.idle_slots(), 1);
    }

    /// A settlement dropped while it waits for a slot leaves nothing behind: the slot the first
    /// one frees serves the next settlement.
    #[tokio::test]
    async fn a_settlement_dropped_while_it_waits_for_a_slot_leaves_the_line_working() {
        let (line, mut commands) = SettleLine::<u32>::open(ONE);
        let mut first = pin!(line.settle(0, SettleKind::Accept));
        assert!(poll_immediate(&mut first).await.is_none());
        let held = commands
            .recv()
            .await
            .expect("the first command reached the pump");
        {
            let mut dropped = pin!(line.settle(1, SettleKind::Accept));
            assert!(poll_immediate(&mut dropped).await.is_none());
        }

        answer_by_number(held);
        check(0, &first.await);
        let mut third = pin!(line.settle(4, SettleKind::Accept));
        assert!(poll_immediate(&mut third).await.is_none());
        let cmd = poll_immediate(commands.recv())
            .await
            .flatten()
            .expect("the third settlement took the freed slot at once");
        assert_eq!(cmd.delivery, 4, "the dropped settlement sent nothing");
        answer_by_number(cmd);
        check(4, &third.await);
        assert_eq!(line.idle_slots(), 1);
    }

    /// A handle whose settle future is dropped while the pump holds its command: the pump answers
    /// into a slot nobody reads, and the settlements after it are answered as before.
    #[tokio::test]
    async fn a_settlement_abandoned_before_its_answer_leaves_the_line_working() {
        let (line, mut commands) = SettleLine::<u32>::open(ONE);
        {
            let mut abandoned = pin!(line.settle(1, SettleKind::Reject));
            assert!(poll_immediate(&mut abandoned).await.is_none());
        }
        let orphan = commands.recv().await.expect("the command reached the pump");
        assert_eq!((orphan.delivery, orphan.kind), (1, SettleKind::Reject));
        orphan.reply.send(Ok(()));
        assert_eq!(
            line.idle_slots(),
            0,
            "the abandoned slot left with its settlement instead of serving the next one"
        );

        let mut next = pin!(line.settle(2, SettleKind::Modify));
        assert!(poll_immediate(&mut next).await.is_none());
        let cmd = poll_immediate(commands.recv())
            .await
            .flatten()
            .expect("the abandoned slot's place serves the next settlement at once");
        assert_eq!((cmd.delivery, cmd.kind), (2, SettleKind::Modify));
        answer_by_number(cmd);
        check(2, &next.await);
        assert_eq!(line.idle_slots(), 1);
    }

    /// A command the pump drops without answering (it stopped on shutdown) reports the stop
    /// instead of leaving the handle waiting.
    #[tokio::test]
    async fn a_command_dropped_unanswered_reports_the_stopped_pump() {
        let (line, mut commands) = SettleLine::<u32>::open(ONE);
        let mut settling = pin!(line.settle(7, SettleKind::Accept));
        assert!(poll_immediate(&mut settling).await.is_none());
        drop(commands.recv().await.expect("the command reached the pump"));

        assert!(matches!(settling.await, Err(AckError::Broker(_))));
        // The slot that saw the stop serves the next settlement.
        assert_eq!(line.idle_slots(), 1);
        let mut after = pin!(line.settle(8, SettleKind::Accept));
        assert!(poll_immediate(&mut after).await.is_none());
        answer_by_number(
            commands
                .recv()
                .await
                .expect("the next command reached the pump"),
        );
        check(8, &after.await);
        assert_eq!(line.idle_slots(), 1, "the next settlement reused the slot");
    }

    #[tokio::test]
    async fn a_settlement_after_the_pump_is_gone_reports_it() {
        let (line, commands) = SettleLine::<u32>::open(ONE);
        drop(commands);

        assert!(matches!(
            line.settle(3, SettleKind::Accept).await,
            Err(AckError::Broker(_))
        ));
    }

    /// Handles dropped without settling send nothing, and once the last of them and the pump's
    /// own reference are gone the pump's drain sees the line close.
    #[tokio::test]
    async fn the_line_closes_when_the_last_unsettled_handle_drops() {
        let (line, mut commands) = SettleLine::<u32>::open(ONE);
        let handles: Vec<_> = (0..3).map(|_| Arc::clone(&line)).collect();
        drop(line);

        for handle in handles {
            assert!(
                poll_immediate(commands.recv()).await.is_none(),
                "the line stays open while a handle may still settle"
            );
            drop(handle);
        }
        assert!(commands.recv().await.is_none(), "the line closed");
    }
}
