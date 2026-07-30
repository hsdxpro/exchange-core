//! The published watermark: what a recovery says again, and what it does not.
//!
//! A leader that dies between acknowledging a command and publishing its
//! outcome used to leave the client holding an ack it could only resolve by
//! asking. The watermark bounds that: the venue journals a marker meaning
//! "everything before me was handed to the feed", and a recovery keeps the
//! private outcomes it regenerates past the last marker, to be handed to each
//! account when it reconnects.
//!
//! These tests drive the journal directly -- write commands and markers, drop
//! the venue, recover a new one over the same storage -- which is exactly the
//! shape of a promotion: the state survives, the outcomes in flight do not.

mod common;

use bx_journal::MemoryLog;
use bx_pipeline::{Exchange, limit_order};
use bx_protocol::{Command, EventKind, Side};
use common::{SYMBOL, funded, instruments};

#[test]
fn a_watermark_is_a_no_op_with_no_events() {
    let mut exchange = funded();
    let before = exchange.snapshot();
    let mut marker = Command::watermark();
    let events = exchange.submit(&mut marker).unwrap();
    assert!(
        events.is_empty(),
        "a venue-internal marker leaked events to subscribers: {events:?}"
    );
    let mut after = exchange.snapshot();
    // The marker is journalled, so the position moved; nothing else may have.
    after.sequence = before.sequence;
    assert_eq!(after, before, "a watermark changed venue state");
}

#[test]
fn recovery_keeps_only_the_outcomes_past_the_last_watermark() {
    let mut exchange = funded();
    // Told before the marker: the old venue already published these.
    let mut early = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    exchange.submit(&mut early).unwrap();
    let mut marker = Command::watermark();
    exchange.submit(&mut marker).unwrap();
    // Acked after the marker: the outcomes that die with a leader.
    let mut late = limit_order(1, SYMBOL, 2, Side::Bid, 10_060, 1);
    exchange.submit(&mut late).unwrap();
    let mut other = limit_order(2, SYMBOL, 3, Side::Ask, 10_100, 1);
    exchange.submit(&mut other).unwrap();

    let storage = exchange.into_storage();
    let mut recovered = Exchange::new(storage, instruments()).unwrap();
    recovered.recover().unwrap();

    let account_1 = recovered
        .take_pending_outcomes(1)
        .expect("account 1 was owed outcomes and got nothing");
    assert!(
        account_1.iter().all(|e| e.order_id != 1),
        "order 1 was published before the watermark and must not be redelivered: {account_1:?}"
    );
    assert!(
        account_1
            .iter()
            .any(|e| e.order_id == 2 && e.kind == EventKind::Resting as u8),
        "account 1 was never told order 2 rested: {account_1:?}"
    );
    assert!(
        account_1.iter().all(|e| e.sequence == 0),
        "redelivered outcomes must carry sequence zero, not a position in a channel that restarted"
    );
    assert!(
        account_1
            .iter()
            .all(|e| e.kind != EventKind::Received as u8),
        "the ack was delivered before it was durable - redelivering it reports every order twice"
    );

    let account_2 = recovered
        .take_pending_outcomes(2)
        .expect("account 2 was owed outcomes and got nothing");
    assert!(
        account_2.iter().any(|e| e.order_id == 3),
        "account 2's outcome went to somebody else or nowhere: {account_2:?}"
    );
    assert!(
        account_2.iter().all(|e| e.order_id != 2),
        "account 1's outcome leaked into account 2's redelivery: {account_2:?}"
    );
}

#[test]
fn draining_is_once_only() {
    let mut exchange = funded();
    let mut marker = Command::watermark();
    exchange.submit(&mut marker).unwrap();
    let mut order = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    exchange.submit(&mut order).unwrap();

    let storage = exchange.into_storage();
    let mut recovered = Exchange::new(storage, instruments()).unwrap();
    recovered.recover().unwrap();

    assert!(recovered.take_pending_outcomes(1).is_some());
    assert!(
        recovered.take_pending_outcomes(1).is_none(),
        "a second session for the same account would be told the same outcomes twice"
    );
}

#[test]
fn a_recovery_with_no_watermark_keeps_everything() {
    // A journal that never carried a marker makes no claim about what was
    // published, so the safe reading is: nothing was.
    let mut exchange = funded();
    let mut order = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    exchange.submit(&mut order).unwrap();

    let storage = exchange.into_storage();
    let mut recovered = Exchange::new(storage, instruments()).unwrap();
    recovered.recover().unwrap();
    assert!(
        recovered.take_pending_outcomes(1).is_some(),
        "with no watermark, every outcome is potentially untold and must be kept"
    );
}

#[test]
fn a_watermark_survives_the_chain() {
    // The marker is an ordinary journalled record: it must fold into the chain
    // like everything else, or two venues disagreeing about markers would
    // disagree about heads.
    // Chain on before the first record: it cannot be retrofitted, and the
    // journal refuses the attempt -- that guard has its own tests.
    let mut exchange = Exchange::new(MemoryLog::new(), instruments()).unwrap();
    exchange.set_chaining(true);
    exchange.set_chain_interval(1);
    exchange.deposit(1, 2, u64::MAX).unwrap();
    let mut order = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    exchange.submit(&mut order).unwrap();
    let before = exchange.chain_head();
    let mut marker = Command::watermark();
    exchange.submit(&mut marker).unwrap();
    assert_ne!(
        exchange.chain_head(),
        before,
        "a journalled record left the chain head unchanged"
    );
}

#[test]
fn a_watermark_clears_outcomes_sequenced_before_it_in_the_same_batch() {
    // Why the gateway injects the marker ahead of the commands a pass reads,
    // rather than appending it to the group.
    //
    // The pipeline's rule is positional and has to be: everything before the
    // marker is claimed as told. So a marker committed *after* commands in the
    // same group claims their outcomes -- and those outcomes reach a socket
    // later in the same pass, so a venue dying in between would drop exactly
    // the events the client never received. This test states the pipeline
    // behaviour that makes the gateway's ordering load-bearing.
    let mut exchange = funded();
    let mut batch = [
        limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1),
        Command::watermark(),
    ];
    exchange.submit_batch(&mut batch).unwrap();

    let storage = exchange.into_storage();
    let mut recovered = Exchange::new(storage, instruments()).unwrap();
    recovered.recover().unwrap();
    assert!(
        recovered.take_pending_outcomes(1).is_none(),
        "an outcome sequenced before the marker survived it, so the marker \
         means something other than what it says"
    );

    // The other order, which is the one the gateway produces: the marker first,
    // claiming only what came before, and the order's outcome kept.
    let mut exchange = funded();
    let mut batch = [
        Command::watermark(),
        limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1),
    ];
    exchange.submit_batch(&mut batch).unwrap();

    let storage = exchange.into_storage();
    let mut recovered = Exchange::new(storage, instruments()).unwrap();
    recovered.recover().unwrap();
    assert!(
        recovered.take_pending_outcomes(1).is_some(),
        "a marker sequenced ahead of a command swallowed that command's outcome"
    );
}
