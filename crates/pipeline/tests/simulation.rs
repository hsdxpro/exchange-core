//! Deterministic simulation: a venue crashed over and over, on purpose.
//!
//! Every failure the venue must survive is injected from a seed, so a failure
//! here is reproducible from one number rather than being a flake someone
//! re-runs until it passes. The seed is printed in every assertion message.
//!
//! What is being checked after each crash is not "did it start up" but the two
//! things a client actually relies on:
//!
//! - **Everything acknowledged survives.** State after recovery equals state at
//!   the last commit, order for order.
//! - **Nothing unacknowledged survives.** A command that was applied in memory
//!   but never committed must be gone, because the client was never told about
//!   it and the venue must not invent it.
//!
//! Value conservation and the accounting-violation counter are asserted the
//! whole way through, because a crash is exactly when a half-applied trade would
//! creep in.

mod common;

use bx_journal::MemoryLog;
use bx_pipeline::book::Resting;
use bx_pipeline::{Exchange, accounting_violations};
use bx_protocol::Side;
use common::{BTC, SYMBOL, TraderPopulation, USD, funded, instruments};

/// The venue's committed state, as a client would be entitled to see it again.
#[derive(Debug, Eq, PartialEq)]
struct Committed {
    orders: Vec<Resting>,
    sequence: u64,
    usd: u128,
    btc: u128,
    open: usize,
}

fn observe(exchange: &Exchange<MemoryLog>) -> Committed {
    let mut orders = Vec::new();
    exchange
        .book(SYMBOL)
        .unwrap()
        .for_each_resting(|order| orders.push(order));
    Committed {
        orders,
        sequence: exchange.next_sequence(),
        usd: exchange.accounts().total_supply(USD),
        btc: exchange.accounts().total_supply(BTC),
        open: exchange.open_orders(),
    }
}

/// Takes the log, loses everything unsynced, and replays it into a fresh venue.
fn crash_and_recover(exchange: Exchange<MemoryLog>) -> Exchange<MemoryLog> {
    let mut storage = exchange.into_storage();
    storage.crash();
    let mut recovered = Exchange::new(storage, instruments()).unwrap();
    recovered.recover().unwrap();
    recovered
}

#[test]
fn a_venue_crashed_repeatedly_never_loses_an_acknowledged_command() {
    for seed in [1_u64, 7, 42, 99, 2_026, 31_337] {
        let mut exchange = funded();
        let mut traders = TraderPopulation::new(seed);
        let mut resting = Vec::new();

        // The venue is funded through committed deposits, so this is already a
        // valid recovery point.
        let mut committed = observe(&exchange);
        let mut crashes = 0;
        let mut abandoned_groups = 0;

        for _ in 0..300 {
            // A group of whatever size the seed says, applied but not yet
            // durable.
            let group = 1 + traders.next() % 12;
            for _ in 0..group {
                let mut command = traders.act(&mut resting);
                exchange.enqueue(&mut command).unwrap();
            }

            // Three quarters of groups are committed. The rest are abandoned by
            // a crash, which is the case that matters.
            if !traders.next().is_multiple_of(4) {
                exchange.commit().unwrap();
                committed = observe(&exchange);
            } else {
                abandoned_groups += 1;
            }

            if traders.next().is_multiple_of(5) {
                exchange = crash_and_recover(exchange);
                crashes += 1;
                assert_eq!(
                    observe(&exchange),
                    committed,
                    "seed {seed}: recovery did not reproduce the last committed state"
                );
            }
        }

        // Finally, crash without committing whatever is outstanding.
        exchange = crash_and_recover(exchange);
        assert_eq!(
            observe(&exchange),
            committed,
            "seed {seed}: the final recovery diverged"
        );

        assert!(crashes > 10, "seed {seed}: only {crashes} crashes injected");
        assert!(
            abandoned_groups > 10,
            "seed {seed}: only {abandoned_groups} groups were left uncommitted, \
             so the interesting case was barely exercised"
        );
        assert!(
            !committed.orders.is_empty(),
            "seed {seed}: the session never rested an order"
        );
        assert_eq!(accounting_violations(), 0, "seed {seed}");
    }
}

#[test]
fn a_torn_write_is_recovered_from_and_trading_continues() {
    // A crash between the write and the flush leaves half a record. It is the
    // *last* thing in the log by definition -- the process is dead, so nothing
    // follows it -- and the venue must cut it off on restart, carry on trading,
    // and still be coherent on the restart after that.
    //
    // Deposits are journalled, so the first ACCOUNTS * 2 appends are those. The
    // tear is placed on the final command of each session.
    let deposits = common::ACCOUNTS.count() * 2;

    for commands in [1_usize, 5, 40, 97] {
        let tear_at = deposits + commands;
        let mut exchange =
            Exchange::new(MemoryLog::new().tearing_append(tear_at), instruments()).unwrap();
        for account in common::ACCOUNTS {
            exchange.deposit(account, USD, common::START_USD).unwrap();
            exchange.deposit(account, BTC, common::START_BTC).unwrap();
        }

        let mut traders = TraderPopulation::new(u64::try_from(commands).unwrap());
        let mut resting = Vec::new();
        for _ in 0..commands {
            let mut command = traders.act(&mut resting);
            exchange.submit(&mut command).unwrap();
        }

        // Restart over the torn log. The half record is dropped, so one command
        // fewer than was sent comes back.
        let storage = exchange.into_storage();
        let mut recovered = Exchange::new(storage, instruments()).unwrap();
        let replayed = recovered
            .recover()
            .unwrap_or_else(|e| panic!("{commands} commands: recovery refused to start: {e}"));
        assert_eq!(
            replayed as usize,
            deposits + commands - 1,
            "{commands} commands: recovery kept or lost the wrong records"
        );

        // And it still trades. The log being writable after the tear was cut off
        // is the part that used to be broken: an append landing after a torn
        // record made every later record unreachable.
        let before = recovered.next_sequence();
        let mut command = traders.act(&mut resting);
        recovered.submit(&mut command).unwrap();
        assert_eq!(
            recovered.next_sequence(),
            before + 1,
            "{commands} commands: could not append after recovering"
        );

        // One more restart, to prove the log is coherent end to end and not just
        // readable up to the point of the tear.
        let storage = recovered.into_storage();
        let mut again = Exchange::new(storage, instruments()).unwrap();
        let replayed_again = again
            .recover()
            .unwrap_or_else(|e| panic!("{commands} commands: second recovery failed: {e}"));
        assert_eq!(
            replayed_again as usize,
            deposits + commands,
            "{commands} commands: the record written after recovery was lost"
        );
        assert_eq!(accounting_violations(), 0, "{commands} commands");
    }
}

#[test]
fn a_storage_failure_is_reported_and_never_silently_swallowed() {
    // The device stops accepting writes part way through a session. Every
    // command after that must fail loudly; a venue that reported success would
    // be acknowledging orders it cannot keep.
    let mut exchange = Exchange::new(MemoryLog::new().failing_after(40), instruments()).unwrap();
    let mut failures = 0;
    let mut accepted = 0;

    let mut traders = TraderPopulation::new(5);
    let mut resting = Vec::new();
    for _ in 0..120 {
        let mut command = traders.act(&mut resting);
        if exchange.submit(&mut command).is_err() {
            failures += 1;
        } else {
            accepted += 1;
        }
    }

    assert_eq!(accepted, 40, "the device accepted more than it should have");
    assert!(failures > 0, "a dead device reported success");
    assert_eq!(accounting_violations(), 0);
}

/// The disk fills. The venue refuses to acknowledge, and recovery discards
/// exactly the group that was applied in memory but never made durable.
///
/// This is the failure the shipped storage actually has. `FileLog` buffers
/// appends and touches the device only at sync, so on a real file ENOSPC
/// surfaces in the sync and never in the append -- and the test above, which
/// fails appends, exercises a path the deployed venue cannot reach.
///
/// The dangerous moment is precise: `enqueue` has already applied the group to
/// the books when `commit`'s sync fails, so the venue's memory is ahead of its
/// journal. Everything rests on two behaviours -- the failed commit releases no
/// events, and the process fail-stops so the divergent memory is discarded and
/// replay rebuilds from the last durable group. A shadow venue fed only the
/// commands that were actually committed defines, independently, what the
/// recovered state must be.
#[test]
fn a_full_disk_refuses_acknowledgement_and_recovery_discards_the_divergence() {
    // Funding costs 16 syncs (8 accounts, 2 deposits each); the disk fills 25
    // trading commands later.
    let deposits = 16;
    let mut primary = Exchange::new(
        MemoryLog::new().failing_sync_after(deposits + 25),
        instruments(),
    )
    .unwrap();
    let mut shadow = funded();
    common::fund(&mut primary);

    let mut traders = TraderPopulation::new(4242);
    let mut resting = Vec::new();
    let mut committed = 0;
    let mut refused = false;
    for _ in 0..100 {
        let mut command = traders.act(&mut resting);
        let mut twin = command;
        if primary.submit(&mut command).is_ok() {
            committed += 1;
            shadow
                .submit(&mut twin)
                .expect("the shadow's clean storage refused a command");
        } else {
            // The venue fail-stops here: its memory holds a group the journal
            // does not, and serving on would acknowledge against state that a
            // restart cannot reproduce.
            refused = true;
            break;
        }
    }
    assert!(refused, "the disk never filled, so nothing was tested");
    assert_eq!(committed, 25, "the failure did not land where it was aimed");

    // The operator frees space and restarts. Everything committed survives;
    // the group that was applied but never synced does not exist.
    let mut storage = primary.into_storage();
    storage.crash();
    storage.repair();
    let mut recovered = Exchange::new(storage, instruments()).unwrap();
    recovered.recover().unwrap();

    assert_eq!(
        observe(&recovered),
        observe(&shadow),
        "recovery after a full disk does not match a venue that only ever \
         saw the committed commands"
    );
    assert_eq!(accounting_violations(), 0);

    // And it is a venue again, not a wreck: the repaired disk takes new orders.
    let mut command = traders.act(&mut resting);
    recovered
        .submit(&mut command)
        .expect("the recovered venue refuses orders after the disk was freed");
}

#[test]
fn recovery_is_reproducible_from_the_same_journal() {
    // Determinism is what makes replay a recovery mechanism rather than an
    // approximation. The same log replayed twice must give the same venue, down
    // to queue order.
    let mut exchange = funded();
    let mut traders = TraderPopulation::new(777);
    let mut resting = Vec::new();
    for _ in 0..500 {
        let mut command = traders.act(&mut resting);
        exchange.submit(&mut command).unwrap();
    }

    let storage = exchange.into_storage();
    let mut first = Exchange::new(storage, instruments()).unwrap();
    first.recover().unwrap();
    let once = observe(&first);

    let storage = first.into_storage();
    let mut second = Exchange::new(storage, instruments()).unwrap();
    second.recover().unwrap();

    assert_eq!(
        observe(&second),
        once,
        "replaying the same journal twice produced different venues"
    );
    assert!(!once.orders.is_empty());
    assert_eq!(
        first_side_count(&once, Side::Bid) + first_side_count(&once, Side::Ask),
        once.orders.len()
    );
}

fn first_side_count(state: &Committed, side: Side) -> usize {
    state.orders.iter().filter(|o| o.side == side).count()
}
