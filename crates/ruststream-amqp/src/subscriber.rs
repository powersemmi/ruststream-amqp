//! [`AmqpSubscriber`]: a stream of deliveries backed by a pump task.
//!
//! The client's `Receiver` requires `&mut self` to receive while dispositions take `&self`, and
//! an ack token must be `Send + 'static`. The crate therefore owns a pump task per subscription:
//! it drives `recv`, forwards deliveries into a bounded channel (back-pressure), and applies
//! settlement commands shipped back from message handles. The subscriber itself is a plain
//! bounded-channel consumer, which keeps `stream` cancel-safe and re-enterable.

use futures::Stream;

use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::link::{Receiver as FeReceiver, RecvError};
use fe2o3_amqp::session::SessionHandle;
use fe2o3_amqp_types::messaging::Body;
use fe2o3_amqp_types::primitives::Value;
use ruststream::{AckError, Subscriber};
use tokio::sync::mpsc;

use crate::address::AmqpAddress;
use crate::broker::{AmqpCore, is_at_most_once, source_for};
use crate::error::{AmqpError, box_err};
use crate::message::{
    AmqpMessage, SettleCmd, SettleKind, SettleSender, headers_from_amqp, payload_from_body,
};

/// A subscription to one `AMQP` address; yields [`AmqpMessage`]s.
///
/// Dropping the subscriber stops the pump task, detaches the link, and ends the subscription's
/// session.
pub struct AmqpSubscriber {
    address: String,
    rx: mpsc::Receiver<Result<AmqpMessage, AmqpError>>,
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
        // never buffers beyond what the broker was allowed to send.
        let (out_tx, out_rx) = mpsc::channel(credit.max(1) as usize);
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
        }));

        Ok(Self {
            address: addr,
            rx: out_rx,
        })
    }
}

impl Subscriber for AmqpSubscriber {
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
}

/// A `RecvError` that poisons only one delivery; the link keeps going.
fn is_per_message(err: &RecvError) -> bool {
    matches!(
        err,
        RecvError::MessageDecode(_)
            | RecvError::DeliveryIdIsNone
            | RecvError::DeliveryTagIsNone
            | RecvError::InconsistentFieldInMultiFrameDelivery
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
                delivery = p.receiver.recv::<Body<Value>>() => match delivery {
                    Ok(delivery) => {
                        let (info, message) = delivery.into_parts();
                        let headers = headers_from_amqp(&message);
                        match payload_from_body(message.body, &p.address) {
                            Ok(payload) => {
                                pending = Some(if p.at_most_once {
                                    AmqpMessage::settled(payload, headers)
                                } else {
                                    AmqpMessage::unsettled(
                                        payload,
                                        headers,
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
                                if p.out.send(Err(err)).await.is_err() {
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
                        if p.out.send(item).await.is_err() {
                            break false;
                        }
                    }
                    Err(err) => {
                        let _ = p.out.send(Err(AmqpError::Receive {
                            address: p.address.clone(),
                            source: box_err(err),
                        }))
                        .await;
                        break true;
                    }
                },
            }
        }
    };

    // Outstanding message handles may still settle; serve them until every clone of the settle
    // sender is gone. On a fatal link error the dispositions fail and report through AckError.
    drop(p.settle_tx);
    while let Some(cmd) = p.settle_rx.recv().await {
        apply(&p.receiver, cmd).await;
    }

    if !fatal && let Err((_, err)) = p.receiver.detach().await {
        tracing::debug!(address = %p.address, error = %err, "amqp receiver detach failed");
    }
    if let Err(err) = p.session.end().await {
        tracing::debug!(address = %p.address, error = %err, "amqp session end failed");
    }
}

async fn apply(receiver: &FeReceiver, cmd: SettleCmd) {
    let result = match cmd.kind {
        SettleKind::Accept => receiver.accept(cmd.info).await,
        SettleKind::Release => receiver.release(cmd.info).await,
        SettleKind::Reject => receiver.reject(cmd.info, None).await,
    };
    let _ = cmd
        .done
        .send(result.map_err(|e| AckError::Broker(box_err(e))));
}
