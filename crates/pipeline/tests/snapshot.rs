//! Snapshot and recovery tests.
//!
//! The property that matters is equivalence: restoring a snapshot and replaying
//! the journal after it must produce exactly the state a full replay from zero
//! produces. If those two ever disagree, the snapshot is silently corrupting the
//! venue while looking like a successful recovery, which is the worst failure
//! this component can have.
//!
//! "Exactly" means order-level, not aggregate. Two books with identical depth
//! can have different queues, and the queue decides who gets filled.

mod common;

use bx_journal::MemoryLog;
use bx_pipeline::book::Resting;
use bx_pipeline::instrument::AssetId;
use bx_pipeline::snapshot::Snapshot;
use bx_pipeline::{Exchange, accounting_violations, limit_order, market_order};
use bx_protocol::{AccountId, EventKind, RejectReason, Side};
use common::{
    ACCOUNTS, BTC, START_BTC, START_USD, SYMBOL, TraderPopulation, USD, funded, instruments,
};

/// Every resting order, in queue order. This is the comparison that catches a
/// reordered book; depth alone would not.
fn resting(exchange: &Exchange<MemoryLog>) -> Vec<Resting> {
    let mut out = Vec::new();
    exchange
        .book(SYMBOL)
        .unwrap()
        .for_each_resting(|order| out.push(order));
    out
}

fn holdings(exchange: &Exchange<MemoryLog>) -> Vec<(AccountId, AssetId, u128, u128)> {
    let mut out = Vec::new();
    for account in ACCOUNTS {
        for asset in [BTC, USD] {
            let balance = exchange.accounts().balance(account, asset);
            out.push((account, asset, balance.free, balance.reserved));
        }
    }
    out
}

#[test]
fn a_snapshot_plus_the_journal_after_it_equals_a_full_replay() {
    for seed in [1_u64, 7, 99, 2_026] {
        let mut exchange = funded();
        let mut traders = TraderPopulation::new(seed);
        let mut resting_ids = Vec::new();

        // Trade, snapshot mid-session, then keep trading.
        for _ in 0..600 {
            let mut command = traders.act(&mut resting_ids);
            exchange.submit(&mut command).unwrap();
        }
        let snapshot = exchange.snapshot();
        assert!(
            !snapshot.orders.is_empty(),
            "seed {seed}: snapshot captured no resting orders"
        );
        for _ in 0..600 {
            let mut command = traders.act(&mut resting_ids);
            exchange.submit(&mut command).unwrap();
        }

        let total = exchange.next_sequence();
        let storage = exchange.into_storage();

        // Path A: replay everything from zero. Deposits are journalled, so
        // nothing is re-applied by hand.
        let mut full = Exchange::new(storage, instruments()).unwrap();
        assert_eq!(full.recover().unwrap(), total);
        let full_book = resting(&full);
        let full_holdings = holdings(&full);
        let full_open = full.open_orders();

        // Path B: restore the snapshot and replay only what came after it. No
        // deposits: the snapshot carries the balances.
        let mut partial = Exchange::new(full.into_storage(), instruments()).unwrap();
        let replayed = partial.recover_from(&snapshot).unwrap();

        assert_eq!(
            replayed,
            total - snapshot.sequence,
            "seed {seed}: replayed the wrong slice of the journal"
        );
        assert!(
            replayed < total,
            "seed {seed}: the snapshot saved no replay at all"
        );
        assert_eq!(
            resting(&partial),
            full_book,
            "seed {seed}: recovered book differs, order by order"
        );
        assert_eq!(
            holdings(&partial),
            full_holdings,
            "seed {seed}: recovered balances differ"
        );
        assert_eq!(
            partial.open_orders(),
            full_open,
            "seed {seed}: recovered a different number of holds"
        );
        assert_eq!(accounting_violations(), 0, "seed {seed}");
    }
}

#[test]
fn a_snapshot_preserves_queue_priority_not_merely_depth() {
    let mut exchange = funded();
    // Three accounts join the same price level in a known order.
    for (account, order_id) in [(1_u64, 101_u64), (2, 102), (3, 103)] {
        let mut command = limit_order(account, SYMBOL, order_id, Side::Ask, 10_100, 5);
        exchange.submit(&mut command).unwrap();
    }

    let snapshot = exchange.snapshot();
    let before = resting(&exchange);
    assert_eq!(
        before.iter().map(|o| o.order).collect::<Vec<_>>(),
        vec![101, 102, 103],
        "the level was not in arrival order to begin with"
    );

    let mut restored = Exchange::new(exchange.into_storage(), instruments()).unwrap();
    restored.restore(&snapshot);
    assert_eq!(resting(&restored), before, "queue order changed");

    // The real test of priority: a taker must hit 101 first.
    let mut taker = market_order(4, SYMBOL, 201, Side::Bid, 5);
    // The taker's own fills. Both sides of a trade are told now, so filtering
    // on the kind alone collects the maker's copy as well and reads its
    // counterparty -- the taker -- as though it were another maker hit.
    let fills: Vec<u64> = restored
        .submit(&mut taker)
        .unwrap()
        .iter()
        .filter(|e| e.kind == EventKind::Filled as u8 && e.account == 4)
        .map(|e| e.counterparty_order_id)
        .collect();
    assert_eq!(fills, vec![101], "the restored queue filled out of order");
}

#[test]
fn a_snapshot_restores_balances_without_any_deposits() {
    let mut exchange = funded();
    let mut command = limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5);
    exchange.submit(&mut command).unwrap();
    let held = exchange.accounts().balance(1, USD).reserved;
    assert!(held > 0, "the order reserved nothing");

    let snapshot = exchange.snapshot();
    let mut restored = Exchange::new(exchange.into_storage(), instruments()).unwrap();
    restored.restore(&snapshot);

    assert_eq!(restored.accounts().balance(1, USD).reserved, held);
    assert_eq!(
        restored.accounts().balance(1, USD).total(),
        u128::from(START_USD),
        "restored the wrong total"
    );
    assert_eq!(
        restored.accounts().balance(1, BTC).total(),
        u128::from(START_BTC)
    );
    assert_eq!(
        restored.accounts().total_supply(USD),
        u128::from(START_USD) * 8
    );
}

#[test]
fn a_restored_order_can_still_be_cancelled_and_releases_its_hold() {
    let mut exchange = funded();
    let mut command = limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5);
    exchange.submit(&mut command).unwrap();
    let snapshot = exchange.snapshot();

    let mut restored = Exchange::new(exchange.into_storage(), instruments()).unwrap();
    restored.restore(&snapshot);

    let mut request = common::cancel(1, 101);
    let events = restored.submit(&mut request).unwrap().to_vec();
    assert!(
        events.iter().any(|e| e.kind == EventKind::Canceled as u8),
        "a restored order could not be cancelled: {events:?}"
    );
    assert_eq!(
        restored.accounts().balance(1, USD).reserved,
        0,
        "cancelling a restored order did not release its hold"
    );
    assert_eq!(
        restored.accounts().balance(1, USD).free,
        u128::from(START_USD)
    );
}

#[test]
fn a_snapshot_survives_a_round_trip_through_a_real_file() {
    let mut exchange = funded();
    let mut traders = TraderPopulation::new(31);
    let mut ids = Vec::new();
    for _ in 0..200 {
        let mut command = traders.act(&mut ids);
        exchange.submit(&mut command).unwrap();
    }
    let snapshot = exchange.snapshot();

    let path = std::env::temp_dir().join(format!("bx-snapshot-{}.snap", std::process::id()));
    let _ = std::fs::remove_file(&path);
    snapshot
        .write_to(&mut std::fs::File::create(&path).unwrap())
        .unwrap();
    let read_back = Snapshot::read_from(&mut std::fs::File::open(&path).unwrap()).unwrap();
    let _ = std::fs::remove_file(&path);

    assert_eq!(
        read_back, snapshot,
        "the file did not survive the round trip"
    );

    // And it still restores a book identical to the one it came from.
    let before = resting(&exchange);
    let mut restored = Exchange::new(exchange.into_storage(), instruments()).unwrap();
    restored.restore(&read_back);
    assert_eq!(resting(&restored), before);
}

#[test]
fn an_empty_venue_snapshots_and_restores_cleanly() {
    let exchange = funded();
    let snapshot = exchange.snapshot();
    assert!(snapshot.orders.is_empty(), "no orders were placed");
    // Not zero: funding the accounts is itself journalled, two deposits for
    // each of the eight accounts.
    assert_eq!(snapshot.sequence, 16);
    assert_eq!(snapshot.balances.len(), 16);

    let mut restored = Exchange::new(exchange.into_storage(), instruments()).unwrap();
    assert_eq!(restored.recover_from(&snapshot).unwrap(), 0);
    assert!(
        restored
            .book(SYMBOL)
            .unwrap()
            .depth(Side::Bid, 10)
            .is_empty()
    );
    assert_eq!(
        restored.accounts().total_supply(USD),
        u128::from(START_USD) * 8
    );
}

/// The case a retrying client hits, and the reason order IDs must increase.
///
/// A client sends an order and loses its connection before the acknowledgement
/// arrives. It cannot tell whether the order landed, so it resends. If the
/// first attempt had already *filled*, nothing resting held that ID any more,
/// and the resend was accepted and filled a second time -- one intent, two
/// trades, and no way for the client to have avoided it.
#[test]
fn an_order_id_that_already_traded_cannot_be_used_again() {
    let mut exchange = funded();

    // Account 2 rests, account 1 takes it. Order 500 fills completely and is
    // gone from the book.
    let mut maker = limit_order(2, SYMBOL, 400, Side::Ask, 10_100, 5);
    exchange.submit(&mut maker).unwrap();
    let mut taker = limit_order(1, SYMBOL, 500, Side::Bid, 10_100, 5);
    let events = exchange.submit(&mut taker).unwrap().to_vec();
    assert!(
        events.iter().any(|e| e.kind == EventKind::Filled as u8),
        "the order was meant to trade"
    );
    assert!(
        exchange.open_orders_for(1, SYMBOL).is_empty(),
        "the taker should hold nothing after filling"
    );

    // The retry. Same ID, same intent.
    let mut retry = limit_order(1, SYMBOL, 500, Side::Bid, 10_100, 5);
    let events = exchange.submit(&mut retry).unwrap().to_vec();
    let rejected: Vec<_> = events
        .iter()
        .filter(|e| e.kind == EventKind::Rejected as u8)
        .collect();
    assert_eq!(rejected.len(), 1, "the retry was not refused: {events:?}");
    assert_eq!(
        rejected[0].reject_reason(),
        Some(RejectReason::OrderIdNotIncreasing),
        "refused, but for a reason that does not tell the client its first \
         attempt had landed"
    );
    assert!(
        !events.iter().any(|e| e.kind == EventKind::Filled as u8),
        "the retry traded a second time"
    );
}

/// A lower ID is refused too, not merely an equal one.
#[test]
fn an_order_id_below_the_highest_used_is_refused() {
    let mut exchange = funded();
    let mut first = limit_order(1, SYMBOL, 900, Side::Bid, 10_050, 1);
    exchange.submit(&mut first).unwrap();

    // Below the mark, and the order that set it is gone from the picture as far
    // as these IDs are concerned.
    for id in [1, 899] {
        let mut command = limit_order(1, SYMBOL, id, Side::Bid, 10_050, 1);
        let events = exchange.submit(&mut command).unwrap().to_vec();
        assert!(
            events.iter().any(|e| e.kind == EventKind::Rejected as u8
                && e.reject_reason() == Some(RejectReason::OrderIdNotIncreasing)),
            "order ID {id} was accepted after 900"
        );
    }

    // 900 itself is still resting, so it is refused as a live duplicate. Both
    // refusals are correct and they say different things: one means the ID is in
    // use right now, the other that it was used and is finished.
    let mut same = limit_order(1, SYMBOL, 900, Side::Bid, 10_050, 1);
    let events = exchange.submit(&mut same).unwrap().to_vec();
    assert!(
        events.iter().any(|e| e.kind == EventKind::Rejected as u8
            && e.reject_reason() == Some(RejectReason::DuplicateOrderId)),
        "a still-resting ID should be refused as a duplicate: {events:?}"
    );

    // And the next one above it still works, so the rule bounds nothing else.
    let mut ok = limit_order(1, SYMBOL, 901, Side::Bid, 10_050, 1);
    let events = exchange.submit(&mut ok).unwrap().to_vec();
    assert!(
        !events.iter().any(|e| e.kind == EventKind::Rejected as u8),
        "901 should have been accepted"
    );
}

/// A rejected order leaves its ID free, because nothing happened.
#[test]
fn an_id_rejected_before_acceptance_may_be_sent_again() {
    let mut exchange = funded();

    // Quantity zero is refused before the mark is taken.
    let mut bad = limit_order(1, SYMBOL, 700, Side::Bid, 10_050, 0);
    let events = exchange.submit(&mut bad).unwrap().to_vec();
    assert!(
        events.iter().any(|e| e.kind == EventKind::Rejected as u8),
        "a zero quantity should be refused"
    );

    // Corrected and resent under the same ID.
    let mut good = limit_order(1, SYMBOL, 700, Side::Bid, 10_050, 1);
    let events = exchange.submit(&mut good).unwrap().to_vec();
    assert!(
        !events.iter().any(|e| e.kind == EventKind::Rejected as u8),
        "the corrected order was refused: {events:?}"
    );
}

/// The mark has to survive a snapshot, or recovery reopens the hole.
///
/// Recovery from a snapshot replays only the journal *after* it. A mark rebuilt
/// from that alone forgets every ID used before, so the venue comes back
/// accepting orders it has already traded -- the failure this rule exists to
/// prevent, reintroduced by the recovery path.
#[test]
fn the_highest_order_id_survives_a_snapshot_and_replay() {
    let mut exchange = funded();
    let mut command = limit_order(1, SYMBOL, 4_000, Side::Bid, 10_050, 1);
    exchange.submit(&mut command).unwrap();
    let mut cancel = common::cancel(1, 4_000);
    exchange.submit(&mut cancel).unwrap();

    let snapshot = exchange.snapshot();
    assert_eq!(
        snapshot.order_id_marks.len(),
        1,
        "the snapshot carries no mark for the account that traded"
    );

    // Round-tripped through bytes, not just cloned: the field has to be in the
    // file, not merely in the struct.
    let mut bytes = Vec::new();
    snapshot.write_to(&mut bytes).unwrap();
    let reread = Snapshot::read_from(&mut bytes.as_slice()).unwrap();
    assert_eq!(
        reread, snapshot,
        "the snapshot did not survive its own format"
    );

    let mut restored = Exchange::new(exchange.into_storage(), instruments()).unwrap();
    restored.recover_from(&reread).unwrap();

    let mut replay = limit_order(1, SYMBOL, 4_000, Side::Bid, 10_050, 1);
    let events = restored.submit(&mut replay).unwrap().to_vec();
    assert!(
        events.iter().any(|e| e.kind == EventKind::Rejected as u8
            && e.reject_reason() == Some(RejectReason::OrderIdNotIncreasing)),
        "a recovered venue accepted an order ID it had already used"
    );
}

/// A full replay from zero reaches the same marks as snapshot plus replay.
///
/// The equivalence this file exists to defend, extended to the new state.
#[test]
fn full_replay_and_snapshot_replay_agree_on_the_marks() {
    let mut exchange = funded();
    for (account, id) in [(1_u64, 10_u64), (2, 20), (1, 30), (3, 40), (2, 50)] {
        let mut command = limit_order(account, SYMBOL, id, Side::Bid, 10_050, 1);
        exchange.submit(&mut command).unwrap();
    }
    let snapshot = exchange.snapshot();
    let storage = exchange.into_storage();

    // One log, two recoveries off it: replay reads, it does not consume.
    let mut from_snapshot = Exchange::new(storage, instruments()).unwrap();
    from_snapshot.recover_from(&snapshot).unwrap();
    let marks_from_snapshot = from_snapshot.snapshot().order_id_marks;

    let mut from_zero = Exchange::new(from_snapshot.into_storage(), instruments()).unwrap();
    from_zero.recover().unwrap();

    assert_eq!(
        marks_from_snapshot,
        from_zero.snapshot().order_id_marks,
        "the two recovery paths disagree on which IDs have been used"
    );
}
