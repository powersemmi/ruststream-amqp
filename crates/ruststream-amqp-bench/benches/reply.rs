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
//! Replying: the handler returns a value, the runtime encodes it and hands it to the publisher
//! this crate's default policy, `AmqpPublish`, pairs into, which sends it to the address the reply
//! type declares and waits for the broker to accept it.

mod common;

use std::hint::black_box;

use common::{Latch, MESSAGES, Order, Pending};
use gungraun::{library_benchmark, library_benchmark_group, main};
use ruststream_amqp::prelude::*;
use serde::Serialize;

/// A reply with a destination of its own: the mount site adds nothing to it.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
}

#[subscriber(AmqpAddress::queue(common::input()), publish)]
async fn confirm(order: &Order, ctx: &mut Context<'_, (), Latch>) -> Confirmation {
    ctx.state().arrived();
    Confirmation {
        id: black_box(order.id),
    }
}

fn app(messages: usize) -> Pending {
    common::pending(messages, |b| {
        b.include(confirm);
    })
}

// The longest run allocated 86,471 to 86,475 blocks over five runs of this tree.
#[library_benchmark(config = common::config(common::floor(86_475)))]
#[bench::first(app(1))]
#[bench::base(app(MESSAGES))]
#[bench::twice(app(2 * MESSAGES))]
fn service(app: Pending) {
    common::start_and_drain(app);
}

library_benchmark_group!(name = reply_group; benchmarks = service);
main!(library_benchmark_groups = reply_group);
