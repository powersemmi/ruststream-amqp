//! The live publishers of the in-process broker, one per production policy.
//!
//! The split mirrors the real broker exactly: [`AmqpPublish`](crate::AmqpPublish) pairs into
//! [`AmqpTestPublisher`], which carries no transactional surface at all, and
//! [`AmqpTransactionalPublish`](crate::AmqpTransactionalPublish) pairs into
//! [`AmqpTestTxnPublisher`]. A mount that compiles against the stand-in therefore compiles
//! against a server too, which a single publisher carrying both surfaces would not guarantee.

use std::future::{Future, ready};
use std::sync::Arc;
#[cfg(feature = "transaction")]
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
#[cfg(feature = "transaction")]
use ruststream::TransactionalPublisher;
use ruststream::{HeaderMap, IncomingMessage, OutgoingMessage, Publisher, RequestReply};

use crate::address::Routing;
use crate::error::AmqpError;
use crate::testing::broker::TestState;
use crate::testing::subscriber::AmqpTestMessage;

/// Publisher for the in-process broker, the live form of [`AmqpPublish`](crate::AmqpPublish).
///
/// # Examples
///
/// ```
/// use ruststream_amqp::prelude::*;
/// use ruststream_amqp::testing::AmqpTestBroker;
/// use serde::Serialize;
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "orders")]
/// struct Order {
///     id: u64,
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let broker = AmqpTestBroker::new().connect().await?;
/// broker.publisher().message(&Order { id: 1 }).publish().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct AmqpTestPublisher {
    state: Arc<TestState>,
}

impl AmqpTestPublisher {
    pub(crate) fn new(state: Arc<TestState>) -> Self {
        Self { state }
    }
}

impl Publisher for AmqpTestPublisher {
    type Error = AmqpError;

    /// The real publisher's settings type, so a mount that compiles here compiles against a
    /// server: empty on both.
    type Options = ();

    /// Routes `msg` to every subscription on its address.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] once the broker has shut down, which is what the real
    /// publisher reports for a handle that outlived its connection.
    fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        _options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        if let Err(err) = self.state.ensure_live() {
            return ready(Err(err));
        }
        self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        );
        ready(Ok(()))
    }
}

/// Request/reply in process: the same correlation the real publisher performs, over the router.
///
/// The stand-in mints the private reply address the request advertises, which is the peer's job on
/// a server (a dynamic terminus). Everything a responder observes is therefore the same: it reads
/// `reply-to`, echoes `correlation-id`, and publishes the answer to that address. A reply carrying
/// a different correlation id is discarded and the wait continues, so a late answer to an earlier
/// request cannot resolve this one.
///
/// What it does not reproduce: a server refuses or dead-letters a request sent to an address
/// nothing consumes, whereas here the publish always lands and the caller learns of it only as
/// [`AmqpError::RequestTimeout`]. Whether a deployment's broker answers at all is a live-suite
/// question; see `tests/integration_amqp.rs` and the request/reply conformance suite.
impl RequestReply for AmqpTestPublisher {
    type Reply = AmqpTestMessage;

    fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> impl Future<Output = Result<Self::Reply, Self::Error>> + Send {
        let state = Arc::clone(&self.state);
        let address = msg.name().to_owned();
        let payload = Bytes::copy_from_slice(msg.payload());
        let mut headers = msg.headers().clone();
        async move {
            state.ensure_live()?;
            let reply_to = state.next_reply_address();
            let correlation_id = format!("{reply_to}-corr");
            // The real publisher overwrites both properties as well: the reply address is the one
            // it owns, and the correlation id is what it will match on.
            headers.insert("reply-to", Bytes::from(reply_to.clone()));
            headers.insert("correlation-id", Bytes::from(correlation_id.clone()));

            // The private reply address carries one consumer, this request: anycast, so a second
            // request in flight cannot be handed a copy of this one's answer.
            let (id, _requeue, mut rx) = state.router.subscribe(reply_to, Routing::Anycast);
            state.publish(&address, payload, headers);

            let reply = tokio::time::timeout(timeout, async {
                loop {
                    let delivery = rx.recv().await?;
                    // Every delivery becomes a message handle before it is judged, so a discarded
                    // one still balances the harness's in-flight count when it drops.
                    let reply = AmqpTestMessage::settled(delivery, state.coordinator().cloned());
                    if reply.headers().correlation_id() == Some(correlation_id.as_str()) {
                        return Some(reply);
                    }
                }
            })
            .await
            .ok()
            .flatten();

            // Mirrors detaching the reply link: the address stops accepting once the exchange is
            // over. Whatever raced the deadline into the channel is drained through the same
            // message handle, so the count stays balanced either way.
            state.router.unsubscribe(id);
            while let Ok(raced) = rx.try_recv() {
                drop(AmqpTestMessage::settled(
                    raced,
                    state.coordinator().cloned(),
                ));
            }

            reply.ok_or(AmqpError::RequestTimeout)
        }
    }
}

/// One publish held back until the transaction commits.
#[cfg(feature = "transaction")]
type Buffered = (String, Bytes, HeaderMap);

/// Transactional publisher for the in-process broker, the live form of
/// [`AmqpTransactionalPublish`](crate::AmqpTransactionalPublish).
///
/// Reproduces what the protocol's transactional posting is observable as: publishes between
/// `begin_transaction` and `commit` reach no subscriber, `abort` discards them, and the handle
/// carries at most one transaction, so a second `begin_transaction` errors and leaves the open one
/// untouched. What it cannot reproduce is atomicity across a crash - the buffer lives in this
/// process, not on a broker - so a durability claim still belongs in the live suite.
///
/// # Examples
///
/// ```
/// use ruststream_amqp::prelude::*;
/// use ruststream_amqp::testing::AmqpTestBroker;
/// use serde::Serialize;
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "invoices")]
/// struct Invoice {
///     id: u64,
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let broker = AmqpTestBroker::new().connect().await?;
/// let publisher = broker.transactional_publisher();
/// publisher.begin_transaction().await?;
/// publisher.message(&Invoice { id: 1 }).publish().await?;
/// publisher.commit().await?;
/// # Ok(())
/// # }
/// ```
// Deliberately not `Clone`, as the real transactional publisher is not: a handle carries at most
// one transaction, and a second handle to the same one would be a second way to settle it.
#[cfg(feature = "transaction")]
#[derive(Debug)]
pub struct AmqpTestTxnPublisher {
    state: Arc<TestState>,
    /// `Some` while a transaction is open.
    txn: Mutex<Option<Vec<Buffered>>>,
}

#[cfg(feature = "transaction")]
impl AmqpTestTxnPublisher {
    pub(crate) fn new(state: Arc<TestState>) -> Self {
        Self {
            state,
            txn: Mutex::new(None),
        }
    }

    fn buffer(&self) -> MutexGuard<'_, Option<Vec<Buffered>>> {
        self.txn
            .lock()
            .expect("amqp test transaction mutex poisoned")
    }
}

#[cfg(feature = "transaction")]
impl Publisher for AmqpTestTxnPublisher {
    type Error = AmqpError;

    /// The real transactional publisher's settings type: empty on both.
    type Options = ();

    /// Buffers `msg` while a transaction is open, and routes it straight away otherwise - which is
    /// what the real publisher does with a post outside a transaction.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::NotConnected`] once the broker has shut down.
    fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        _options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        if let Err(err) = self.state.ensure_live() {
            return ready(Err(err));
        }
        let payload = Bytes::copy_from_slice(msg.payload());
        let mut buffer = self.buffer();
        if let Some(open) = buffer.as_mut() {
            open.push((msg.name().to_owned(), payload, msg.headers().clone()));
        } else {
            drop(buffer);
            self.state
                .publish(msg.name(), payload, msg.headers().clone());
        }
        ready(Ok(()))
    }
}

#[cfg(feature = "transaction")]
impl TransactionalPublisher for AmqpTestTxnPublisher {
    /// Opens the buffering transaction.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when one is already open, which is left untouched.
    fn begin_transaction(&self) -> impl Future<Output = Result<(), Self::Error>> {
        let mut buffer = self.buffer();
        let already_open = buffer.is_some();
        if !already_open {
            *buffer = Some(Vec::new());
        }
        drop(buffer);
        if already_open {
            return ready(Err(AmqpError::Transaction(
                "a transaction is already open on this publisher".into(),
            )));
        }
        ready(Ok(()))
    }

    /// Routes everything buffered, in publish order.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when no transaction is open, and
    /// [`AmqpError::NotConnected`] once the broker has shut down - a discharge cannot reach a
    /// closed transport either. The transaction is consumed in both cases, as it is on the real
    /// publisher, so the handle never wedges.
    fn commit(&self) -> impl Future<Output = Result<(), Self::Error>> {
        let Some(buffered) = self.buffer().take() else {
            return ready(Err(AmqpError::Transaction(
                "no transaction is open on this publisher".into(),
            )));
        };
        if let Err(err) = self.state.ensure_live() {
            return ready(Err(err));
        }
        for (address, payload, headers) in buffered {
            self.state.publish(&address, payload, headers);
        }
        ready(Ok(()))
    }

    /// Discards everything buffered.
    ///
    /// # Errors
    ///
    /// Returns [`AmqpError::Transaction`] when no transaction is open.
    fn abort(&self) -> impl Future<Output = Result<(), Self::Error>> {
        if self.buffer().take().is_none() {
            return ready(Err(AmqpError::Transaction(
                "no transaction is open on this publisher".into(),
            )));
        }
        ready(Ok(()))
    }
}
