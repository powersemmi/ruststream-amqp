// The harness macros generate the group module, its items and the paths between them, and a
// benchmark function takes its setup value by value because the harness owns the drop; the
// crate's lints are written for the library surface, not for generated benchmark scaffolding.
#![allow(
    missing_docs,
    unused_qualifications,
    unreachable_pub,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value
)]
//! Consuming in batches of 64: the crate assembles the batch on the client, hands the handler a
//! slice, and the runtime settles every delivery in it.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_amqp::prelude::*;

#[subscriber(AmqpAddress::queue("orders"))]
async fn consume(orders: &[Order], ctx: &mut Context<'_, (), Latch>) -> HandlerOutcome {
    for order in orders {
        black_box((order.id, order.quantity));
        ctx.state().arrived();
    }
    HandlerOutcome::ack()
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(consume.batch(nonzero!(64)));
    })
}

// Two allocations per delivery are the framework's test hooks, which copy every payload of a
// batch for the harness's record whether a harness runs or not, and three more come per batch.
// A thousand deliveries do not divide into whole batches, so the floor is stated over a thousand.
#[library_benchmark(config = common::config_every(2_048, 1_000, 29))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = batch_group; benchmarks = service);
main!(library_benchmark_groups = batch_group);
