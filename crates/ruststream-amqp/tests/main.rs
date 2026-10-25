//! The integration suite, one test binary: each file under `tests/` is a module of it.

mod live;

mod asyncapi;
mod batches;
mod conformance_amqp;
mod integration_amqp;
mod live_descriptor;
mod live_request_reply;
mod live_retry;
mod live_runtime;
mod live_shutdown;
mod owned_transactions;
mod prelude_vocabularies;
mod publishing;
mod reply_destination;
mod retry_redelivery;
mod sources;
