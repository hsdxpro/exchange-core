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
use bx_protocol::{Command, EventKind, Side, checkpoint_message};
use common::{SYMBOL, funded, instruments};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
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
            "the head advanced at sequence {sequence}, which is not an interval \
             boundary"
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

// ----------------------------------------------------- publishing the head

/// The head reaches clients, not just the venue's own API.
///
/// A chain nobody is told about proves nothing. This is the difference between
/// having the property and shipping it.
#[test]
fn a_checkpoint_is_published_when_the_head_seals() {
    let mut exchange = chaining_with_interval(4);
    let mut published = Vec::new();

    for id in 1..=16_u64 {
        let mut command = limit_order(1, SYMBOL, id, Side::Bid, 10_050, 1);
        let events = exchange.submit(&mut command).unwrap();
        for event in events {
            if event.kind == EventKind::Checkpoint as u8 {
                published.push(*event);
            }
        }
    }

    assert!(
        !published.is_empty(),
        "the head sealed but no checkpoint was published"
    );
    // The last one carries the head the venue now holds.
    let last = published.last().unwrap();
    assert_eq!(
        last.chain_head(),
        exchange.chain_head(),
        "the published head is not the venue's"
    );
    assert!(
        last.cause_sequence > 0,
        "a checkpoint that names no sequence commits to nothing in particular"
    );
}

/// Nothing is published when chaining is off.
#[test]
fn no_checkpoints_without_chaining() {
    let mut exchange = funded();
    for id in 1..=8_u64 {
        let mut command = limit_order(1, SYMBOL, id, Side::Bid, 10_050, 1);
        let events = exchange.submit(&mut command).unwrap();
        assert!(
            !events.iter().any(|e| e.kind == EventKind::Checkpoint as u8),
            "a venue with chaining off published a checkpoint"
        );
    }
}

/// A checkpoint carries a full digest through the record and back.
#[test]
fn a_checkpoint_round_trips_its_head() {
    let mut head = [0_u8; 32];
    for (index, byte) in head.iter_mut().enumerate() {
        *byte = u8::try_from(index).expect("32 fits") ^ 0x5a;
    }
    let event = bx_protocol::Event::checkpoint(4_096, &head);
    assert_eq!(event.kind(), Some(EventKind::Checkpoint));
    assert_eq!(event.cause_sequence, 4_096);
    assert_eq!(
        event.chain_head(),
        head,
        "the head did not survive the record"
    );
}

/// The published head is the one a client recomputes from the stream.
///
/// End to end for the property: the venue publishes, the client recalculates
/// from the commands it saw, and the two agree.
#[test]
fn a_client_can_check_a_published_checkpoint_against_the_stream() {
    const INTERVAL: u64 = 4;
    let mut exchange = chaining_with_interval(INTERVAL);
    let mut recomputed = exchange.chain_head();
    let mut latest = None;

    // A verifier has to fold a whole interval's records into one digest, the way
    // the venue does. Calling the extend function per record instead finalises
    // per record, which is a *different* chain -- correct arithmetic over the
    // wrong boundaries, and it disagrees with the venue for a reason that looks
    // like a bug in the venue. Hence submitting an interval at a time here.
    for round in 0..2_u64 {
        let mut batch: Vec<Command> = (0..INTERVAL)
            .map(|i| {
                let id = round * INTERVAL + i + 1;
                limit_order(1, SYMBOL, id, Side::Bid, 10_050 + id as i64, 1)
            })
            .collect();
        let events = exchange.submit_batch(&mut batch).unwrap().to_vec();
        recomputed = chain_next(&recomputed, batch.iter().map(IntoBytes::as_bytes));
        for event in &events {
            if event.kind == EventKind::Checkpoint as u8 {
                latest = Some(*event);
            }
        }
    }

    let checkpoint = latest.expect("no checkpoint was published");
    assert_eq!(
        checkpoint.chain_head(),
        recomputed,
        "the venue published a head the stream does not produce"
    );
    assert_eq!(checkpoint.chain_head(), exchange.chain_head());
}

/// A snapshot taken between two boundaries still recovers to the right head.
///
/// The case the other recovery test missed, because at an interval of one every
/// record is a boundary and the two coincide. With a wider interval a snapshot
/// lands mid-interval: its head covers only up to the previous boundary, and the
/// records after that were folded into a digest nothing persists. Replaying from
/// the snapshot alone leaves them out of the chain for good, and the venue then
/// publishes a head no client can reproduce — silent divergence in the one
/// feature whose whole point is not needing to be trusted.
#[test]
fn a_snapshot_taken_mid_interval_recovers_to_the_same_head() {
    const INTERVAL: u64 = 8;
    let mut exchange = chaining_with_interval(INTERVAL);

    // Enough to sit part way through an interval rather than on a boundary.
    for id in 1..=13_u64 {
        let mut command = limit_order(1, SYMBOL, id, Side::Bid, 10_050, 1);
        exchange.submit(&mut command).unwrap();
    }
    let snapshot = exchange.snapshot();
    assert!(
        snapshot.chain_sealed_at < snapshot.sequence,
        "this test needs a snapshot taken between boundaries, but it landed on \
         one: sealed at {} of {}",
        snapshot.chain_sealed_at,
        snapshot.sequence
    );

    // Carry on past the next boundary, so a head that lost records diverges.
    for id in 14..=30_u64 {
        let mut command = limit_order(1, SYMBOL, id, Side::Bid, 10_050, 1);
        exchange.submit(&mut command).unwrap();
    }
    let live = exchange.chain_head();
    let live_at = exchange.chain_sealed_at();

    // Recover from the mid-interval snapshot, then apply the same tail.
    let storage = exchange.into_storage();
    let mut restored = Exchange::new(storage, instruments()).unwrap();
    restored.set_chaining(true);
    restored.set_chain_interval(INTERVAL);
    restored.recover_from(&snapshot).unwrap();

    assert_eq!(
        restored.chain_head(),
        live,
        "a mid-interval snapshot recovered to a head the venue never had"
    );
    assert_eq!(restored.chain_sealed_at(), live_at);
}

/// A published head names what it covers, not the venue's newest sequence.
///
/// Between boundaries the two differ. Naming the newest would claim coverage of
/// records the head does not commit to, and a client folding those would
/// disagree with a venue that had done nothing wrong.
#[test]
fn a_checkpoint_names_only_what_its_head_covers() {
    const INTERVAL: u64 = 4;
    let mut exchange = chaining_with_interval(INTERVAL);
    let mut latest = None;

    for id in 1..=20_u64 {
        let mut command = limit_order(1, SYMBOL, id, Side::Bid, 10_050, 1);
        let events = exchange.submit(&mut command).unwrap().to_vec();
        for event in &events {
            if event.kind == EventKind::Checkpoint as u8 {
                latest = Some(*event);
            }
        }
        // Whatever the venue has published, it never claims more than it covers.
        if let Some(checkpoint) = latest {
            assert!(
                checkpoint.cause_sequence <= exchange.chain_sealed_at(),
                "a checkpoint claimed coverage to {} while the head covers {}",
                checkpoint.cause_sequence,
                exchange.chain_sealed_at()
            );
            assert!(
                checkpoint.cause_sequence.is_multiple_of(INTERVAL),
                "a head was sealed off a boundary, at {}",
                checkpoint.cause_sequence
            );
        }
    }

    let checkpoint = latest.expect("nothing was published");
    assert_eq!(
        checkpoint.cause_sequence,
        exchange.chain_sealed_at(),
        "the newest checkpoint disagrees with the head it was published for"
    );
    assert_eq!(checkpoint.chain_head(), exchange.chain_head());
}

/// A chain switched on over an existing journal must not be usable until
/// something has replayed that journal.
///
/// The dangerous shape: open a log holding a thousand records, enable chaining
/// (legal, because *this* journal has appended nothing yet -- that is how a
/// recovering node enables it before replaying), then append without ever
/// recovering. The first boundary seals a head covering only the new records
/// while naming a sequence that spans all of them, so the venue publishes a
/// commitment over history it never hashed. A client folding the stream would
/// disagree, and the venue would look like it was lying.
///
/// Enabling before a replay is legitimate and has to stay legal, so the refusal
/// belongs at the append.
#[test]
#[should_panic(expected = "nothing has replayed")]
fn a_chain_enabled_over_an_unreplayed_journal_refuses_to_append() {
    let mut exchange = funded();
    let mut command = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    exchange.submit(&mut command).unwrap();
    let storage = exchange.into_storage();

    // A fresh venue over a journal that already holds records, chaining on, and
    // no recovery in between.
    let mut careless = Exchange::new(storage, instruments()).unwrap();
    careless.set_chaining(true);
    careless.set_chain_interval(4);
    let mut order = limit_order(1, SYMBOL, 99, Side::Bid, 10_050, 1);
    let _ = careless.submit(&mut order);
}

/// And the legitimate flow still works: enable, then recover, then trade.
#[test]
fn a_chain_enabled_before_a_replay_is_fine() {
    let mut exchange = chaining_with_interval(4);
    for id in 1..=10_u64 {
        let mut command = limit_order(1, SYMBOL, id, Side::Bid, 10_050, 1);
        exchange.submit(&mut command).unwrap();
    }
    let live = exchange.chain_head();

    let mut restored = Exchange::new(exchange.into_storage(), instruments()).unwrap();
    restored.set_chaining(true);
    restored.set_chain_interval(4);
    restored.recover().unwrap();
    assert_eq!(restored.chain_head(), live, "a full replay diverged");

    // And it may append afterwards.
    let mut more = limit_order(1, SYMBOL, 11, Side::Bid, 10_050, 1);
    restored.submit(&mut more).unwrap();
}

// ------------------------------------------------ the venue's own signature

/// A key whose private half exists only here. Signing keys never live in the
/// repository; this is a test fixture, and a venue's real one is loaded from a
/// file the operator holds.
fn venue_key() -> SigningKey {
    SigningKey::from_bytes(&[0x5a; 32])
}

fn signed_chaining() -> Exchange<MemoryLog> {
    let mut exchange = chaining();
    exchange.set_chain_key(venue_key());
    exchange
}

/// Reassembles the signature a checkpoint was published with, the way a client
/// reading the channel must: two halves, in arrival order, after the head.
fn published_signature(events: &[bx_protocol::Event]) -> Option<(u64, [u8; 32], [u8; 64])> {
    let head_at = events
        .iter()
        .position(|e| e.kind == EventKind::Checkpoint as u8)?;
    let first = events
        .iter()
        .position(|e| e.kind == EventKind::CheckpointSignature as u8)?;
    let second = events
        .iter()
        .position(|e| e.kind == EventKind::CheckpointSignatureContinued as u8)?;
    let mut signature = [0_u8; 64];
    signature[..32].copy_from_slice(&events[first].chain_head());
    signature[32..].copy_from_slice(&events[second].chain_head());
    Some((
        events[head_at].cause_sequence,
        events[head_at].chain_head(),
        signature,
    ))
}

/// What the gateway's logon asks a client to sign, spelled out rather than
/// imported: this crate must not depend on the gateway to make a point about
/// domain separation.
fn logon_message(nonce: &[u8; 32]) -> Vec<u8> {
    let mut message = Vec::new();
    message.extend_from_slice(bx_protocol::AUTH_DOMAIN);
    message.extend_from_slice(nonce);
    message
}

#[test]
fn a_client_can_verify_the_venue_signed_the_head_it_published() {
    let mut exchange = signed_chaining();
    let mut command = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    let events = exchange.submit(&mut command).unwrap().to_vec();

    let (sealed_at, head, signature) =
        published_signature(&events).expect("no signature was published with the head");
    // Only the public half, which is all a client is ever given.
    let verifying = VerifyingKey::from_bytes(&venue_key().verifying_key().to_bytes()).unwrap();
    verifying
        .verify_strict(
            &checkpoint_message(sealed_at, &head),
            &Signature::from_bytes(&signature),
        )
        .expect("the venue's signature over its own head did not verify");
    assert_eq!(
        exchange.chain_public_key(),
        Some(verifying.to_bytes()),
        "the venue reports a public key other than the one it signed with"
    );
}

#[test]
fn the_signature_arrives_as_two_records_directly_after_the_head() {
    let mut exchange = signed_chaining();
    let mut command = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    let events = exchange.submit(&mut command).unwrap();

    let checkpoint: Vec<&bx_protocol::Event> = events
        .iter()
        .filter(|e| e.kind >= EventKind::Checkpoint as u8)
        .collect();
    let kinds: Vec<u8> = checkpoint.iter().map(|e| e.kind).collect();
    assert_eq!(
        kinds,
        vec![
            EventKind::Checkpoint as u8,
            EventKind::CheckpointSignature as u8,
            EventKind::CheckpointSignatureContinued as u8,
        ],
        "a client reassembling halves in arrival order would build the wrong signature"
    );
    // Contiguous channel sequences, so a subscriber cannot mistake the pair for
    // a gap and ask for a resume it does not need.
    for pair in checkpoint.windows(2) {
        assert_eq!(
            pair[1].sequence,
            pair[0].sequence + 1,
            "the checkpoint channel numbered its own records with a hole"
        );
    }
}

#[test]
fn a_signature_does_not_verify_against_a_head_it_did_not_cover() {
    let mut exchange = signed_chaining();
    let mut first = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    let early = exchange.submit(&mut first).unwrap().to_vec();
    let mut second = limit_order(1, SYMBOL, 2, Side::Bid, 10_060, 1);
    let later = exchange.submit(&mut second).unwrap().to_vec();

    let (_, _, early_signature) = published_signature(&early).unwrap();
    let (later_at, later_head, _) = published_signature(&later).unwrap();
    // The venue's own earlier signature, replayed against a later head. Refused
    // because the sequence is inside the signed message, not merely beside it.
    assert!(
        venue_key()
            .verifying_key()
            .verify_strict(
                &checkpoint_message(later_at, &later_head),
                &Signature::from_bytes(&early_signature),
            )
            .is_err(),
        "a past commitment verified as a description of the present"
    );
}

#[test]
fn a_logon_signature_is_not_a_commitment_to_the_stream() {
    // The domains are separate so one key cannot produce a signature valid in
    // both places. Signing a logon challenge whose bytes are chosen to look like
    // a checkpoint must not yield a usable commitment.
    let key = venue_key();
    let mut exchange = signed_chaining();
    let mut command = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    let events = exchange.submit(&mut command).unwrap().to_vec();
    let (sealed_at, head, _) = published_signature(&events).unwrap();

    let as_logon = key.sign(&logon_message(&head));
    assert!(
        key.verifying_key()
            .verify_strict(&checkpoint_message(sealed_at, &head), &as_logon)
            .is_err(),
        "a logon signature verified as a chain commitment"
    );
}

#[test]
fn a_venue_holding_no_key_still_publishes_a_head() {
    // The chain is worth folding without a signature; it just cannot survive a
    // venue that rewrites its own history. Losing the head too would be strictly
    // worse.
    let mut exchange = chaining();
    let mut command = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    let events = exchange.submit(&mut command).unwrap();
    assert!(
        events.iter().any(|e| e.kind == EventKind::Checkpoint as u8),
        "an unsigned venue stopped publishing heads"
    );
    assert!(
        !events
            .iter()
            .any(|e| e.kind == EventKind::CheckpointSignature as u8),
        "a venue with no key published a signature anyway"
    );
    assert_eq!(exchange.chain_public_key(), None);
}

#[test]
fn a_replay_on_a_node_holding_the_same_key_reproduces_the_signature() {
    // Ed25519 is deterministic, which is what lets the signature live in the
    // event stream at all: a promoted node replaying the journal has to produce
    // the same records, or recovery would not be a function of the log.
    let mut exchange = signed_chaining();
    let mut command = limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1);
    let events = exchange.submit(&mut command).unwrap().to_vec();
    let (sealed_at, head, signature) = published_signature(&events).unwrap();

    let again = venue_key().sign(&checkpoint_message(sealed_at, &head));
    assert_eq!(
        again.to_bytes(),
        signature,
        "the same key over the same head produced two different signatures"
    );
}
