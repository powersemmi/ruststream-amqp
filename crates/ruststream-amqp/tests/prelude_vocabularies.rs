//! Pins the two vocabularies the crate prelude carries apart.
//!
//! A handler body bounds its slots with the broker capability traits; a routes file attaches
//! policies under uniform, prefix-free names. The names must not collide, and the alias must keep
//! resolving to the policy through the glob even while the framework ships a same-named item of
//! its own - which is exactly what a silent glob shadow would hide.

use ruststream_amqp::prelude::*;

/// The handler vocabulary: a slot bound names the capability, never a publisher type. The leading
/// underscore keeps a never-called compile-only pin out of the dead-code lint.
fn _slot_bound<T: Publisher>() {}

#[cfg(feature = "transaction")]
fn _transactional_slot_bound<T: TransactionalPublisher>() {}

fn _request_slot_bound<T: RequestReply>() {}

#[test]
fn the_mount_vocabulary_names_the_policies_without_the_broker_prefix() {
    // The value form a mount site writes: `.out(Reply, Publish)`.
    let _: Publish = Publish;

    #[cfg(feature = "transaction")]
    let _: TransactionalPublish = TransactionalPublish;
}

/// The glob is the one import of this crate a service writes, so the surface a declaration names
/// comes with it: the broker and its authentication profile, the descriptor and its delivery
/// guarantee, and the error startup and publishing report.
#[test]
fn the_glob_carries_the_surface_a_declaration_names() {
    let broker = AmqpBroker::new("amqp://localhost:5672").sasl(Sasl::plain("svc", "secret"));
    let source = AmqpAddress::topic("events").settle(Settle::AtMostOnce);
    let reported: Result<(), AmqpError> = Ok(());

    let _ = (broker, source, reported);
}

#[test]
fn the_prefixed_originals_stay_reachable_through_the_glob() {
    // What a file globbing two broker preludes writes to say which `Publish` it means.
    let _: AmqpPublish = AmqpPublish;

    #[cfg(feature = "transaction")]
    let _: AmqpTransactionalPublish = AmqpTransactionalPublish;
}
