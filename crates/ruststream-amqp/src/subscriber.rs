//! [`AmqpSubscriber`]: a stream of deliveries backed by a pump task.
//!
//! The client's `Receiver` requires `&mut self` to receive while dispositions take `&self`, and
//! an ack token must be `Send + 'static`. The crate therefore owns a pump task per subscription:
//! it drives `recv`, forwards deliveries into a bounded channel (back-pressure), and applies
//! settlement commands shipped back from message handles. The subscriber itself is a plain
//! bounded-channel consumer, which keeps `stream` cancel-safe and re-enterable, under the
//! framework's client-side buffer that turns it into a [`BatchSubscriber`].

use std::num::NonZeroUsize;

use futures::Stream;

use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::link::{Receiver as FeReceiver, RecvError};
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp_types::messaging::{Body, Modified};
use fe2o3_amqp_types::primitives::Value;
use ruststream::{AckError, BatchSubscriber, BufferedSubscriber, Subscriber};
use tokio::sync::{mpsc, watch};

use crate::address::AmqpAddress;
use crate::broker::{AmqpCore, PumpGuard, is_at_most_once, source_for};
use crate::error::{AmqpError, box_err};
use crate::message::{
    AmqpMessage, SettleCmd, SettleKind, SettleSender, headers_from_amqp, payload_from_body,
};

/// A subscription to one `AMQP` address; yields [`AmqpMessage`]s one at a time, or in batches.
///
/// Dropping the subscriber stops the pump task, detaches the link, and ends the subscription's
/// session. A subscription still open when the connected broker shuts down is ended by the
/// shutdown, before the connection closes: its stream yields the deliveries it had already
/// received and then ends, and settling one of those reports an error.
pub struct AmqpSubscriber {
    address: String,
    deliveries: BufferedSubscriber<Deliveries>,
}

impl std::fmt::Debug for AmqpSubscriber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpSubscriber")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl AmqpSubscriber {
    /// The address this subscription consumes from.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    pub(crate) async fn attach(
        core: &AmqpCore,
        mut session: SessionHandle<()>,
        address: AmqpAddress,
        guard: PumpGuard,
    ) -> Result<Self, AmqpError> {
        let at_most_once = is_at_most_once(address.settle_value());
        let credit = address.credit_value();
        let receiver = FeReceiver::builder()
            .name(core.link_name("receiver"))
            .source(source_for(&address))
            .auto_accept(at_most_once)
            .credit_mode(CreditMode::Auto(credit))
            .attach(&mut session)
            .await
            .map_err(|e| AmqpError::Attach {
                address: address.address().to_owned(),
                source: box_err(e),
            })?;

        // The delivery channel bounds in-flight conversions at the link credit, so the pump
        // never buffers beyond what the broker was allowed to send. The credit is non-zero by
        // construction, so it is already a legal channel capacity.
        let (out_tx, out_rx) = mpsc::channel(credit as usize);
        let (settle_tx, settle_rx) = mpsc::unbounded_channel();
        let addr = address.address().to_owned();
        tokio::spawn(pump(Pump {
            session,
            receiver,
            out: out_tx,
            settle_tx,
            settle_rx,
            address: addr.clone(),
            at_most_once,
            guard,
        }));

        Ok(Self {
            address: addr,
            deliveries: BufferedSubscriber::new(Deliveries { rx: out_rx })
                .max_wait(address.batch_wait_value()),
        })
    }
}

impl Subscriber for AmqpSubscriber {
    type Message = AmqpMessage;
    type Error = AmqpError;

    fn stream(&mut self) -> impl Stream<Item = Result<AmqpMessage, AmqpError>> + Send + '_ {
        self.deliveries.stream()
    }
}

/// `AMQP` 1.0 has no batch pull: a transfer carries one message and credit is flow control, not a
/// batch size. The batches are therefore assembled on the client, by the framework's own buffer,
/// so a batch never carries more than the size the registration named. What this crate chooses is
/// the deadline that closes a partial one, which rides the descriptor as
/// [`AmqpAddress::batch_wait`](crate::AmqpAddress::batch_wait).
impl BatchSubscriber for AmqpSubscriber {
    type Batch = Vec<AmqpMessage>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        self.deliveries.batches(size)
    }
}

/// The consuming end of the pump channel: one delivery per stream item.
struct Deliveries {
    rx: mpsc::Receiver<Result<AmqpMessage, AmqpError>>,
}

impl Subscriber for Deliveries {
    type Message = AmqpMessage;
    type Error = AmqpError;

    fn stream(&mut self) -> impl Stream<Item = Result<AmqpMessage, AmqpError>> + Send + '_ {
        // Poll the channel in place rather than wrapping it in an owning stream, so `stream`
        // can be called again after the returned stream is dropped (the runtime and the
        // conformance helpers re-enter it per call).
        futures::stream::poll_fn(move |cx| self.rx.poll_recv(cx))
    }
}

struct Pump {
    /// Owned for the lifetime of the subscription: dropping a session handle detaches every
    /// link on it, so the pump keeps it alive until teardown.
    session: SessionHandle<()>,
    receiver: FeReceiver,
    out: mpsc::Sender<Result<AmqpMessage, AmqpError>>,
    /// Held to mint per-message settle handles; dropped before the drain phase so the settle
    /// channel can close once every outstanding message handle is gone.
    settle_tx: SettleSender,
    settle_rx: mpsc::UnboundedReceiver<SettleCmd>,
    address: String,
    at_most_once: bool,
    /// Carries the connection's stop signal in, and its drop tells the connection this pump's
    /// session has ended. Held to the last line of the pump.
    guard: PumpGuard,
}

/// A `RecvError` that poisons only one delivery; the link keeps going.
///
/// An oversized delivery is one of them: the client has already rejected it with
/// `amqp:link:message-size-exceeded` and left the link attached.
fn is_per_message(err: &RecvError) -> bool {
    matches!(
        err,
        RecvError::MessageDecode(_)
            | RecvError::DeliveryIdIsNone
            | RecvError::DeliveryTagIsNone
            | RecvError::InconsistentFieldInMultiFrameDelivery
            | RecvError::MessageSizeExceeded(_)
    )
}

async fn pump(mut p: Pump) {
    let mut pending: Option<AmqpMessage> = None;
    let fatal = loop {
        if let Some(msg) = pending.take() {
            // A delivery is waiting for channel capacity; keep settling while it waits so an
            // unpolled stream can never wedge in-flight acks.
            tokio::select! {
                biased;
                cmd = p.settle_rx.recv() => {
                    if let Some(cmd) = cmd {
                        apply(&p.receiver, cmd).await;
                    }
                    pending = Some(msg);
                }
                permit = p.out.reserve() => match permit {
                    Ok(permit) => permit.send(Ok(msg)),
                    Err(_) => break false, // subscriber dropped
                },
                () = stopped(&mut p.guard.stop) => break false, // shutdown
            }
        } else {
            tokio::select! {
                biased;
                cmd = p.settle_rx.recv() => {
                    if let Some(cmd) = cmd {
                        apply(&p.receiver, cmd).await;
                    }
                }
                () = p.out.closed() => break false, // subscriber dropped
                () = stopped(&mut p.guard.stop) => break false, // shutdown
                delivery = p.receiver.recv::<Body<Value>>() => match delivery {
                    Ok(delivery) => {
                        let (info, message) = delivery.into_parts();
                        let headers = headers_from_amqp(&message);
                        // Absent where the peer sent no header section, which is what a message
                        // this crate published looks like; see `AmqpMessage::redelivery_count`.
                        let delivery_count = message.header.as_ref().map(|h| h.delivery_count);
                        match payload_from_body(message.body, &p.address) {
                            Ok(payload) => {
                                pending = Some(if p.at_most_once {
                                    AmqpMessage::settled(payload, headers, delivery_count)
                                } else {
                                    AmqpMessage::unsettled(
                                        payload,
                                        headers,
                                        delivery_count,
                                        p.settle_tx.clone(),
                                        info,
                                    )
                                });
                            }
                            Err(err) => {
                                // Terminal for this delivery: without a byte form the handler
                                // can never process it, so reject rather than redeliver forever.
                                if !p.at_most_once {
                                    let _ = p.receiver.reject(info, None).await;
                                }
                                if !deliver(&p.out, &mut p.guard.stop, Err(err)).await {
                                    break false;
                                }
                            }
                        }
                    }
                    Err(err) if is_per_message(&err) => {
                        let item = Err(AmqpError::Receive {
                            address: p.address.clone(),
                            source: box_err(err),
                        });
                        if !deliver(&p.out, &mut p.guard.stop, item).await {
                            break false;
                        }
                    }
                    Err(err) => {
                        let error = Err(AmqpError::Receive {
                            address: p.address.clone(),
                            source: box_err(err),
                        });
                        deliver(&p.out, &mut p.guard.stop, error).await;
                        break true;
                    }
                },
            }
        }
    };

    // Outstanding message handles may still settle; serve them until every clone of the settle
    // sender is gone, or until the connection shuts down, after which a settlement could not
    // reach the peer anyway and reports through AckError instead. On a fatal link error the
    // dispositions fail and report the same way. The delivery channel goes first: deliveries it
    // still holds for a dropped subscriber hold settle handles of their own.
    drop(p.out);
    drop(p.settle_tx);
    loop {
        tokio::select! {
            biased;
            cmd = p.settle_rx.recv() => match cmd {
                Some(cmd) => apply(&p.receiver, cmd).await,
                None => break,
            },
            () = stopped(&mut p.guard.stop) => break,
        }
    }

    if !fatal && let Err((_, err)) = p.receiver.detach().await {
        tracing::debug!(address = %p.address, error = %err, "amqp receiver detach failed");
    }
    if let Err(err) = p.session.end().await {
        tracing::debug!(address = %p.address, error = %err, "amqp session end failed");
    }
    // Only now may the connection close.
    drop(p.guard);
}

/// Resolves once the connection shuts down, or once it is gone altogether.
/// Hands `item` to the subscriber, waiting for channel capacity only until shutdown: a
/// subscriber that stopped polling a full channel must not hold the connection open. `false`
/// when the subscriber is gone or the connection is shutting down.
async fn deliver(
    out: &mpsc::Sender<Result<AmqpMessage, AmqpError>>,
    stop: &mut watch::Receiver<bool>,
    item: Result<AmqpMessage, AmqpError>,
) -> bool {
    tokio::select! {
        biased;
        () = stopped(stop) => false,
        sent = out.send(item) => sent.is_ok(),
    }
}

async fn stopped(stop: &mut watch::Receiver<bool>) {
    // An error means the connection's state was dropped, which ends the pump as surely as a
    // shutdown does. The value is dropped here rather than returned: it borrows the channel and
    // cannot cross a task boundary.
    let _ = stop.wait_for(|stopped| *stopped).await.is_ok();
}

async fn apply(receiver: &FeReceiver, cmd: SettleCmd) {
    let result = match cmd.kind {
        SettleKind::Accept => receiver.accept(cmd.info).await,
        SettleKind::Modify => {
            receiver
                .modify(
                    cmd.info,
                    Modified {
                        delivery_failed: Some(true),
                        // The delivery is to come back to this subscription: a retry that
                        // excluded its own consumer would strand a single-consumer service.
                        undeliverable_here: Some(false),
                        message_annotations: None,
                    },
                )
                .await
        }
        SettleKind::Reject => receiver.reject(cmd.info, None).await,
    };
    let _ = cmd
        .done
        .send(result.map_err(|e| AckError::Broker(box_err(e))));
}

#[cfg(test)]
mod tests {
    use fe2o3_amqp::link::{MessageSizeExceeded, RecvError};

    use super::is_per_message;

    /// The client rejects a delivery larger than the link's `max-message-size` itself, with
    /// `amqp:link:message-size-exceeded`, and keeps the link attached. The subscription reports
    /// that delivery and keeps receiving; ending it would stop a service over one message.
    #[test]
    fn an_oversized_delivery_does_not_end_the_subscription() {
        let oversized = RecvError::MessageSizeExceeded(MessageSizeExceeded {
            size: 2048,
            max_size: 1024,
        });

        assert!(is_per_message(&oversized));
    }

    /// A peer that sends past the credit it was given breaks the link's flow control, which no
    /// single delivery explains, so the subscription ends.
    #[test]
    fn a_transfer_beyond_the_credit_ends_the_subscription() {
        assert!(!is_per_message(&RecvError::TransferLimitExceeded));
    }
}
