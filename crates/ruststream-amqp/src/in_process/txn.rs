//! Transactional posting on the in-process transport.

use std::sync::{Arc, Mutex, MutexGuard};

use ruststream::{OutgoingFor, Take};

use super::bus::Bus;
use super::router::Delivery;
use super::{ensure_node, frame};
use crate::error::AmqpError;

/// One publish held back until its transaction commits.
type Buffered = (String, Delivery);

/// The transactional publisher's state on the in-process transport: the publishes of the open
/// transaction, held back until the commit.
///
/// Nothing between `begin_transaction` and `commit` reaches a subscriber, `abort` discards it, and
/// the handle carries at most one transaction. The buffer lives in this process, so atomicity
/// across a crash is the server's to show.
#[derive(Debug)]
pub(crate) struct TxnBuffer {
    bus: Arc<Bus>,
    /// `Some` while a transaction is open.
    open: Mutex<Option<Vec<Buffered>>>,
}

impl TxnBuffer {
    pub(crate) fn new(bus: Arc<Bus>) -> Self {
        Self {
            bus,
            open: Mutex::new(None),
        }
    }

    fn open(&self) -> MutexGuard<'_, Option<Vec<Buffered>>> {
        self.open
            .lock()
            .expect("amqp in-process transaction mutex poisoned")
    }

    /// Buffers `msg` while a transaction is open, and routes it at once otherwise, as the live
    /// publisher posts outside a transaction.
    pub(crate) fn publish(&self, msg: OutgoingFor<'_, Take>) -> Result<(), AmqpError> {
        self.bus.ensure_live()?;
        let address = msg.name();
        ensure_node(address)?;
        let delivery = frame(msg);
        let mut open = self.open();
        if let Some(buffer) = open.as_mut() {
            buffer.push((address.to_owned(), delivery));
        } else {
            drop(open);
            self.bus.route(address, &delivery)?;
        }
        Ok(())
    }

    /// Opens an owned transaction on the same connection: a buffer of its own, which nothing
    /// shares with this handle or with another owned transaction.
    pub(crate) fn open_owned(&self) -> Result<OwnedBuffer, AmqpError> {
        self.bus.ensure_live()?;
        Ok(OwnedBuffer {
            bus: Arc::clone(&self.bus),
            buffered: Vec::new(),
        })
    }

    pub(crate) fn begin(&self) -> Result<(), AmqpError> {
        self.bus.ensure_live()?;
        let mut open = self.open();
        if open.is_some() {
            return Err(AmqpError::Transaction(
                "a transaction is already open on this publisher".into(),
            ));
        }
        *open = Some(Vec::new());
        drop(open);
        Ok(())
    }

    /// Takes the open transaction, which a commit and an abort both consume, whatever they report
    /// after it: the handle never wedges.
    fn take(&self) -> Result<Vec<Buffered>, AmqpError> {
        self.open().take().ok_or_else(|| {
            AmqpError::Transaction("no transaction is open on this publisher".into())
        })
    }

    pub(crate) fn commit(&self) -> Result<(), AmqpError> {
        let buffered = self.take()?;
        // The discharge cannot reach a closed connection, as on the live publisher.
        self.bus.ensure_live()?;
        self.bus.route_all(&buffered)
    }

    pub(crate) fn abort(&self) -> Result<(), AmqpError> {
        drop(self.take()?);
        self.bus.ensure_live()
    }
}

/// An owned transaction's state on the in-process transport: its publishes, held back until its
/// commit.
///
/// Every owned transaction has a buffer of its own, so any number of them are open on one
/// publisher at once and settling one leaves the others whole.
#[derive(Debug)]
pub(crate) struct OwnedBuffer {
    bus: Arc<Bus>,
    buffered: Vec<Buffered>,
}

impl OwnedBuffer {
    /// How many publishes the transaction holds, for the warning a dropped one logs.
    pub(crate) const fn len(&self) -> usize {
        self.buffered.len()
    }

    pub(crate) fn publish(&mut self, msg: OutgoingFor<'_, Take>) -> Result<(), AmqpError> {
        self.bus.ensure_live()?;
        let address = msg.name();
        ensure_node(address)?;
        let delivery = frame(msg);
        self.buffered.push((address.to_owned(), delivery));
        Ok(())
    }

    pub(crate) fn commit(self) -> Result<(), AmqpError> {
        // The discharge cannot reach a closed connection, as on the live transaction.
        self.bus.ensure_live()?;
        self.bus.route_all(&self.buffered)
    }

    pub(crate) fn abort(self) -> Result<(), AmqpError> {
        self.bus.ensure_live()
    }
}
