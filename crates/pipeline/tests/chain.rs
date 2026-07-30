//! The verifiable chain over the sequenced stream.
//!
//! The venue publishes a head that commits to every command it has accepted, in
//! order. A client holding the stream can recompute that head and see for itself
//! that its order was included where it was told and that nothing was slipped in
//! front of it. "Did the sequencer front-run me" otherwise takes trust, and this
//! is the one question a venue cannot answer by asserting an answer.
//!
//! So these tests are written from the client's side wherever they can be: they
//! recompute heads with the same public function a client would use, rather than
//! comparing the venue against itself.

mod common;

use bx_journal::{CHAIN_LEN, EMPTY_CHAIN, MemoryLog, chain_next};
use bx_pipeline::snapshot::Snapshot;
use bx_pipeline::{Exchange, limit_order};
use bx_protocol::{Command, Side};
use common::{SYMBOL, funded, instruments};
use zerocopy::IntoBytes;

/// A chaining venue that seals after every record.
///
/// The default interval is 1,024, which is the right trade for a venue and the
/// wrong one for a test: exercising a boundary would mean a thousand orders per
/// assertion. An interval of one puts a boundary everywhere, so these tests are
/// about what the chain commits to rather than about counting to 1,024. The
/// default is exercised separately below.
fn chaining() -> Exchange<MemoryLog> {
    chaining_with_interval(1)
}

/// Chaining from the journal's first record, then funded.
///
/// The order matters and is enforced: chaining cannot be switched on over a
/// journal that already holds records, because the head would cover only what
/// follows while a replay would cover everything. Funding is journalled, so a
/// venue funded first is already too late.
fn chaining_with_interval(interval: u64) -> Exchange<MemoryLog> {
    let mut exchange = common::venue();
    exchange.set_chaining(true);
    exchange.set_chain_interval(interval);
    common::fund(&mut exchange);
    exchange
}

/// Off unless asked for, because it is not free.
#[test]
fn the_chain_is_absent_until_it_is_turned_on() {
    let mut exchange = funded();
    let mut command = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    exchange.submit(&mut command).unwrap();
    assert_eq!(
        exchange.chain_head(),
        EMPTY_CHAIN,
        "a venue that was never asked to chain produced a head"
    );
}

#[test]
fn the_head_moves_with_every_record_and_never_repeats() {
    let mut exchange = chaining();
    let mut seen = std::collections::HashSet::new();
    seen.insert(exchange.chain_head());

    for id in 1..=20_u64 {
        let mut command = limit_order(1, SYMBOL, id, Side::Bid, 10_050, 1);
        exchange.submit(&mut command).unwrap();
        assert!(
            seen.insert(exchange.chain_head()),
            "the head repeated after command {id}, so it commits to nothing"
        );
    }
}

/// The client's side: recompute the head from the commands and compare.
///
/// This is the whole point. If the venue's head cannot be reproduced from the
/// stream by somebody else, publishing it proves nothing.
#[test]
fn a_client_can_recompute_the_head_from_the_commands_it_saw() {
    let mut exchange = chaining();
    // Funding is journalled and chained, so the client starts from where that
    // left the head rather than from nothing.
    let mut recomputed = exchange.chain_head();

    for id in 1..=10_u64 {
        let mut command = limit_order(1, SYMBOL, id, Side::Bid, 10_050 + id as i64, 1);
        exchange.submit(&mut command).unwrap();
        // `submit` stamps the sequence, so the command now holds exactly the
        // bytes the journal appended. The interval is one here, so every record
        // is its own boundary.
        recomputed = chain_next(&recomputed, [command.as_bytes()]);
        assert_eq!(
            exchange.chain_head(),
            recomputed,
            "the venue's head and the client's disagree after command {id}"
        );
    }
}

/// One group of many records: the head covers all of them, in order.
/// A whole interval's records are covered by the head sealed at its end.
#[test]
fn an_interval_is_covered_by_one_head_over_its_records_in_order() {
    let mut exchange = chaining_with_interval(8);

    let before = exchange.chain_head();
    let mut batch: Vec<Command> = (1..=8_u64)
        .map(|id| limit_order(1, SYMBOL, id, Side::Bid, 10_050 + id as i64, 1))
        .collect();
    exchange.submit_batch(&mut batch).unwrap();

    let expected = chain_next(&before, batch.iter().map(IntoBytes::as_bytes));
    assert_eq!(
        exchange.chain_head(),
        expected,
        "an interval's head is not the chain over its records"
    );
}

/// The head does not move inside an interval, and does at its boundary.
///
/// Sealing costs; the interval is what spreads that cost. A head that advanced
/// per record would mean the interval was being ignored.
#[test]
fn the_head_holds_still_inside_an_interval_and_moves_at_the_boundary() {
    let mut exchange = chaining_with_interval(4);
    let start = exchange.chain_head();

    // The journal already holds the funding deposits, so boundaries are counted
    // from wherever those left it rather than from zero.
    let mut moved_at = Vec::new();
    let mut previous = start;
    for id in 1..=16_u64 {
        let mut command = limit_order(1, SYMBOL, id, Side::Bid, 10_050, 1);
        exchange.submit(&mut command).unwrap();
        let head = exchange.chain_head();
        if head != previous {
            moved_at.push(command.sequence);
            previous = head;
        }
    }

    assert!(!moved_at.is_empty(), "the head never advanced");
    for sequence in &moved_at {
        assert!(
            (sequence + 1).is_multiple_of(4),
            "the head advanced at sequence {sequence}, which is not an interval              boundary"
        );
    }
}

/// And the shipped default works, without a test-sized interval.
#[test]
fn the_default_interval_seals_after_its_own_number_of_records() {
    let mut exchange = common::venue();
    exchange.set_chaining(true);
    common::fund(&mut exchange);
    assert_eq!(exchange.chain_interval(), 1_024);

    let before = exchange.chain_head();
    let mut batch: Vec<Command> = (1..=1_100_u64)
        .map(|id| limit_order(1, SYMBOL, id, Side::Bid, 10_050, 1))
        .collect();
    exchange.submit_batch(&mut batch).unwrap();
    assert_ne!(
        exchange.chain_head(),
        before,
        "past a thousand records the default interval sealed nothing"
    );
}

/// Swapping two commands changes the head. This is the property that makes it a
/// commitment to *order* rather than to contents.
#[test]
fn reordering_two_commands_changes_the_head() {
    let mut first = chaining();
    let mut second = chaining();

    let mut a = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    let mut b = limit_order(1, SYMBOL, 2, Side::Bid, 10_060, 1);
    first.submit(&mut a).unwrap();
    first.submit(&mut b).unwrap();

    // The other venue takes them the other way round. Sequence numbers differ,
    // which is itself part of what the head commits to -- a reorder is not a
    // relabelling.
    let mut b2 = limit_order(1, SYMBOL, 2, Side::Bid, 10_060, 1);
    let mut a2 = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    second.submit(&mut b2).unwrap();
    second.submit(&mut a2).unwrap();

    assert_ne!(
        first.chain_head(),
        second.chain_head(),
        "two different orderings produced the same head, so the chain does not \
         commit to order"
    );
}

/// Inserting a command changes every head after it.
#[test]
fn inserting_a_command_changes_the_head() {
    let mut honest = chaining();
    let mut tampered = chaining();

    for id in [1_u64, 2] {
        let mut command = limit_order(1, SYMBOL, id, Side::Bid, 10_050, 1);
        honest.submit(&mut command).unwrap();
    }

    let mut extra = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    tampered.submit(&mut extra).unwrap();
    let mut slipped = limit_order(2, SYMBOL, 99, Side::Bid, 10_070, 1);
    tampered.submit(&mut slipped).unwrap();
    let mut command = limit_order(1, SYMBOL, 2, Side::Bid, 10_050, 1);
    tampered.submit(&mut command).unwrap();

    assert_ne!(
        honest.chain_head(),
        tampered.chain_head(),
        "an inserted command left the head unchanged"
    );
}

/// The same commands produce the same head on a different venue.
///
/// Determinism, applied to the chain: a head is a function of the stream and
/// nothing else, or a replica and a leader would publish different ones.
#[test]
fn two_venues_given_the_same_stream_agree_on_the_head() {
    let mut left = chaining();
    let mut right = chaining();
    for id in 1..=15_u64 {
        let price = 10_000 + (id as i64 * 7) % 900;
        let mut one = limit_order(1, SYMBOL, id, Side::Bid, price, 1);
        let mut two = limit_order(1, SYMBOL, id, Side::Bid, price, 1);
        left.submit(&mut one).unwrap();
        right.submit(&mut two).unwrap();
    }
    assert_eq!(left.chain_head(), right.chain_head());
    assert_ne!(left.chain_head(), EMPTY_CHAIN);
}

/// Recovery reaches the head it is recovering from, by either route.
///
/// The failure this guards is subtle and would be invisible: a recovered venue
/// that publishes a head committing to a *suffix* of the stream. Every client
/// checking it would disagree, and neither side could see why.
#[test]
fn both_recovery_paths_reach_the_same_head() {
    let mut exchange = chaining();
    for id in 1..=12_u64 {
        let mut command = limit_order(1, SYMBOL, id, Side::Bid, 10_050, 1);
        exchange.submit(&mut command).unwrap();
    }
    let live = exchange.chain_head();
    let snapshot = exchange.snapshot();
    assert_eq!(snapshot.chain_head, live, "the snapshot lost the head");

    // Through the file format, not just the struct.
    let mut bytes = Vec::new();
    snapshot.write_to(&mut bytes).unwrap();
    let reread = Snapshot::read_from(&mut bytes.as_slice()).unwrap();
    assert_eq!(reread.chain_head, live);

    let mut from_snapshot = Exchange::new(exchange.into_storage(), instruments()).unwrap();
    from_snapshot.set_chaining(true);
    from_snapshot.set_chain_interval(1);
    from_snapshot.recover_from(&reread).unwrap();
    assert_eq!(
        from_snapshot.chain_head(),
        live,
        "snapshot recovery published a head the live venue never had"
    );

    let mut from_zero = Exchange::new(from_snapshot.into_storage(), instruments()).unwrap();
    from_zero.set_chaining(true);
    from_zero.set_chain_interval(1);
    from_zero.recover().unwrap();
    assert_eq!(
        from_zero.chain_head(),
        live,
        "a full replay reached a different head from the venue it replayed"
    );
}

/// A head is a full digest, not a truncation.
#[test]
fn a_head_is_a_full_digest() {
    assert_eq!(CHAIN_LEN, 32);
    let mut exchange = chaining();
    let mut command = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    exchange.submit(&mut command).unwrap();
    let head = exchange.chain_head();
    assert_ne!(head, EMPTY_CHAIN);
    assert!(
        head.iter().any(|byte| *byte != 0),
        "a head of zeroes is not a digest"
    );
}
