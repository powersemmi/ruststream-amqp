//! Where a reply lands on this broker: the destination its own type declares, or the one the
//! mount site supplies when the type declares none.
//!
//! Both resolutions are the framework's, but the address a reply actually leaves on is the
//! broker's, so this reads it back off the in-process broker's publish log rather than trusting
//! the mount site.

#![cfg(feature = "testing")]

use ruststream::testing::TestApp;
use ruststream_amqp::prelude::*;
use ruststream_amqp::testing::AmqpTestBroker;
use serde::{Deserialize, Serialize};

/// The request both responders answer. The derive names no destination, so each injection below
/// says which address it goes to.
#[derive(Debug, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// A reply that fixes its own destination, so the mount site adds nothing to it.
///
/// No reply here shares a field set with the request: the assertions read a decoded payload, so
/// two types that decode into one another would let an assertion aimed at the wrong address pass.
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
    paid: bool,
}

/// A reply that declares no destination, so the mount site supplies one.
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize, Outgoing)]
struct Confirmation {
    order_id: u64,
    accepted: bool,
}

#[subscriber("receipt-requests", publish)]
async fn issue_receipt(order: &Order) -> Receipt {
    Receipt {
        order_id: order.id,
        paid: true,
    }
}

#[subscriber("confirmation-requests", publish("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        order_id: order.id,
        accepted: true,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_lands_where_its_own_type_declares() {
    let app =
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(AmqpTestBroker::new(), |b| {
            b.include(issue_receipt);
        });
    let app = TestApp::start(app).await.expect("startup failed");

    // Awaiting the injection drives the reaction to a standstill, so the assertions below read a
    // settled service.
    app.broker::<AmqpTestBroker>()
        .message(&Order { id: 7 })
        .to("receipt-requests")
        .publish()
        .await
        .expect("publish failed");

    app.broker::<AmqpTestBroker>()
        .subscriber("receipt-requests")
        .assert_called_once();
    app.broker::<AmqpTestBroker>()
        .published::<Receipt>("receipts")
        .assert_called_once()
        .with(&Receipt {
            order_id: 7,
            paid: true,
        });

    app.shutdown().await.expect("shutdown failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reply_that_declares_no_name_lands_where_the_mount_site_says() {
    let app =
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(AmqpTestBroker::new(), |b| {
            b.include(confirm);
        });
    let app = TestApp::start(app).await.expect("startup failed");

    app.broker::<AmqpTestBroker>()
        .message(&Order { id: 11 })
        .to("confirmation-requests")
        .publish()
        .await
        .expect("publish failed");

    app.broker::<AmqpTestBroker>()
        .subscriber("confirmation-requests")
        .assert_called_once();
    app.broker::<AmqpTestBroker>()
        .published::<Confirmation>("confirmations")
        .assert_called_once()
        .with(&Confirmation {
            order_id: 11,
            accepted: true,
        });

    app.shutdown().await.expect("shutdown failed");
}
