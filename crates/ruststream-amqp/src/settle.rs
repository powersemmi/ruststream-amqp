//! How a live delivery settles: the handle ships a disposition to its subscription's pump task and
//! waits for the answer the client gives.
//!
//! The client settles through the receiver the pump owns, so a handle cannot dispose of its
//! delivery itself. Every handle of a subscription shares one [`SettleLine`]: the command channel
//! to the pump and the answer slots the pump writes into. A slot belongs to the subscription and
//! is reused, so settling a delivery allocates nothing once the subscription has answered as many
//! settlements at once as it ever will.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use fe2o3_amqp::link::delivery::DeliveryInfo;
use multishot::{Receiver, Sender};
use ruststream::AckError;
use tokio::sync::mpsc;

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
    /// The answer slots no settlement is using. A settlement takes one and puts it back once it
    /// has read its answer, so the line holds as many slots as it ever had settlements in flight
    /// at once and allocates only to grow to that number.
    ///
    /// The lock guards a push or a pop, never an await. It replaces the allocation and the
    /// reference counting of a fresh channel per settlement, and the pump serializes the
    /// settlements of a subscription anyway, so the handles never wait on one another here
    /// longer than they wait for the pump.
    idle: Mutex<Vec<Receiver<Answer>>>,
}

impl<Delivery> fmt::Debug for SettleLine<Delivery> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SettleLine").finish_non_exhaustive()
    }
}

impl<Delivery> SettleLine<Delivery> {
    /// A line for a new subscription, and the end its pump reads commands from.
    pub(crate) fn open() -> (Arc<Self>, SettleCommands<Delivery>) {
        let (commands, received) = mpsc::unbounded_channel();
        let line = Self {
            commands,
            idle: Mutex::new(Vec::new()),
        };
        (Arc::new(line), received)
    }

    /// Asks the pump to settle `delivery` and waits for the client's answer.
    ///
    /// # Errors
    ///
    /// The client's refusal, or [`AckError::Broker`] when the pump has stopped: the subscription
    /// ended or the connection shut down, and the settlement can no longer reach the peer.
    pub(crate) async fn settle(&self, delivery: Delivery, kind: SettleKind) -> Answer {
        let (mut slot, reply) = self.take_slot();
        self.commands
            .send(SettleCmd {
                delivery,
                kind,
                reply: Reply(reply),
            })
            .map_err(|_| pump_stopped())?;
        // A settle future dropped while it waits here takes its slot along: the pump may still
        // write into it, so it must not serve another settlement.
        let answer = slot.recv().await;
        self.idle_slots_mut().push(slot);
        answer.map_err(|_| pump_stopped())?
    }

    /// An idle slot and the sender its answer arrives through, or a new pair when every slot is
    /// in use.
    fn take_slot(&self) -> (Receiver<Answer>, Sender<Answer>) {
        let idle = self.idle_slots_mut().pop();
        // A slot goes back only after its answer was read, so its sender is always available
        // again; the new pair stands in for a slot that would say otherwise.
        idle.and_then(|mut slot| slot.sender().map(|reply| (slot, reply)))
            .unwrap_or_else(|| {
                let (reply, slot) = multishot::channel();
                (slot, reply)
            })
    }

    fn idle_slots_mut(&self) -> MutexGuard<'_, Vec<Receiver<Answer>>> {
        // A push or a pop cannot leave the list half-changed, so a panic elsewhere while the
        // lock was held does not make the slots unusable.
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How many answer slots wait for their next settlement.
    #[cfg(test)]
    fn idle_slots(&self) -> usize {
        self.idle_slots_mut().len()
    }
}

fn pump_stopped() -> AckError {
    AckError::Broker(Box::from("the subscription's pump task has shut down"))
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::future::{join_all, poll_immediate};

    use super::*;

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
        let (line, mut commands) = SettleLine::<u32>::open();
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
        let (line, mut commands) = SettleLine::<u32>::open();
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

    /// A handle whose settle future is dropped while the pump holds its command: the pump answers
    /// into a slot nobody reads, and the settlements after it are answered as before.
    #[tokio::test]
    async fn a_settlement_abandoned_before_its_answer_leaves_the_line_working() {
        let (line, mut commands) = SettleLine::<u32>::open();
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
        let cmd = commands
            .recv()
            .await
            .expect("the next command reached the pump");
        assert_eq!((cmd.delivery, cmd.kind), (2, SettleKind::Modify));
        answer_by_number(cmd);
        check(2, &next.await);
        assert_eq!(line.idle_slots(), 1);
    }

    /// A command the pump drops without answering (it stopped on shutdown) reports the stop
    /// instead of leaving the handle waiting.
    #[tokio::test]
    async fn a_command_dropped_unanswered_reports_the_stopped_pump() {
        let (line, mut commands) = SettleLine::<u32>::open();
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
        let (line, commands) = SettleLine::<u32>::open();
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
        let (line, mut commands) = SettleLine::<u32>::open();
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
