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
use bx_protocol::{AccountId, EventKind, Side};
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
    let fills: Vec<u64> = restored
        .submit(&mut taker)
        .unwrap()
        .iter()
        .filter(|e| e.kind == EventKind::Filled as u8)
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
