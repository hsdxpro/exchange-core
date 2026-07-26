//! Snapshots taken by a running venue, and used by a restart.
//!
//! Driven by calling `poll` and `snapshot_if_due` directly rather than through a
//! socket. Sockets are covered elsewhere; what matters here is that the file on
//! disk is complete, that a restart uses it, and that a restart which uses it
//! ends up in exactly the state a full replay would.

use bx_gateway::tcp::{Server, SnapshotPolicy};
use bx_journal::FileLog;
use bx_pipeline::instrument::{Instrument, Instruments};
use bx_pipeline::limit_order;
use bx_protocol::{Command, Side, Ticks};
use std::path::{Path, PathBuf};
use std::time::Duration;

const BTC: u32 = 1;
const USD: u32 = 2;
const SYMBOL: u32 = 1;
const FLOOR: Ticks = 10_000;
const RETAINED: usize = 4_096;
const MAX_SESSIONS: usize = 1_024;
const MAX_RECORDS: usize = 256;

fn instruments() -> Instruments {
    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(SYMBOL, BTC, USD, FLOOR, 1_000_000, 65_536));
    instruments
}

/// A scratch directory that cleans up after itself.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("bx-snap-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn venue(journal: &Path) -> Server<FileLog> {
    Server::bind(
        "127.0.0.1:0",
        FileLog::open(journal).unwrap(),
        instruments(),
        RETAINED,
        MAX_RECORDS,
        MAX_SESSIONS,
    )
    .unwrap()
}

fn orders(from: u64, count: u64) -> Vec<Command> {
    (0..count)
        .map(|i| {
            limit_order(
                1,
                SYMBOL,
                from + i,
                Side::Bid,
                FLOOR + 1_000 + ((from + i) % 4_000) as Ticks,
                1,
            )
        })
        .collect()
}

#[test]
fn a_policy_turns_a_recovery_target_into_a_command_count() {
    // 6.5M commands a second replayed, two seconds of downtime allowed.
    let policy = SnapshotPolicy::from_recovery_target(6_500_000, Duration::from_secs(2));
    assert_eq!(policy.interval(), 13_000_000);

    // A tighter target snapshots more often, which is the whole trade.
    let tighter = SnapshotPolicy::from_recovery_target(6_500_000, Duration::from_millis(100));
    assert!(tighter.interval() < policy.interval());
}

#[test]
#[should_panic(expected = "replay rate must be positive")]
fn a_policy_refuses_a_replay_rate_of_zero() {
    let _ = SnapshotPolicy::from_recovery_target(0, Duration::from_secs(1));
}

#[test]
fn nothing_is_written_until_a_snapshot_is_due() {
    let scratch = Scratch::new("not-yet");
    let path = scratch.file("state.snap");
    let mut server = venue(&scratch.file("journal.log"));
    server.venue_mut().deposit(1, USD, u64::MAX / 4).unwrap();
    // Due only after 1,000 commands.
    server.snapshot_to(
        SnapshotPolicy::from_recovery_target(1_000, Duration::from_secs(1)),
        path.clone(),
    );

    let mut batch = orders(1, 10);
    server.venue_mut().accept(&mut batch).unwrap();
    assert_eq!(server.snapshot_if_due().unwrap(), None);
    assert!(!path.exists(), "wrote a snapshot before one was due");
}

#[test]
fn a_restart_from_a_snapshot_lands_where_a_full_replay_would() {
    let scratch = Scratch::new("equivalent");
    let journal = scratch.file("journal.log");
    let path = scratch.file("state.snap");

    // Trade, snapshot part way, then trade some more.
    let expected = {
        let mut server = venue(&journal);
        server.venue_mut().deposit(1, USD, u64::MAX / 4).unwrap();
        server.snapshot_to(
            SnapshotPolicy::from_recovery_target(100, Duration::from_secs(1)),
            path.clone(),
        );

        let mut batch = orders(1, 500);
        server.venue_mut().accept(&mut batch).unwrap();
        let covered = server
            .snapshot_if_due()
            .unwrap()
            .expect("a snapshot was due and was not taken");
        assert!(path.exists(), "the snapshot file was not written");
        assert!(covered > 0);

        let mut more = orders(1_000, 200);
        server.venue_mut().accept(&mut more).unwrap();
        server
            .venue()
            .book(SYMBOL)
            .unwrap()
            .depth(Side::Bid, 10_000)
    };

    // Restart using the snapshot.
    let from_snapshot = {
        let mut server = venue(&journal);
        let replayed = server.recover(Some(&path)).unwrap();
        assert!(
            replayed > 0 && replayed < 701,
            "replayed {replayed} records, so the snapshot saved nothing"
        );
        server
            .venue()
            .book(SYMBOL)
            .unwrap()
            .depth(Side::Bid, 10_000)
    };

    // Restart ignoring it.
    let from_journal = {
        let mut server = venue(&journal);
        server.recover(None).unwrap();
        server
            .venue()
            .book(SYMBOL)
            .unwrap()
            .depth(Side::Bid, 10_000)
    };

    assert_eq!(
        from_snapshot, expected,
        "recovering from the snapshot lost or invented orders"
    );
    assert_eq!(
        from_snapshot, from_journal,
        "the snapshot path and the full replay disagree, so one of them is wrong"
    );
}

#[test]
fn a_corrupt_snapshot_is_reported_rather_than_quietly_ignored() {
    let scratch = Scratch::new("corrupt");
    let journal = scratch.file("journal.log");
    let path = scratch.file("state.snap");
    {
        let mut server = venue(&journal);
        server.venue_mut().deposit(1, USD, u64::MAX / 4).unwrap();
        let mut batch = orders(1, 10);
        server.venue_mut().accept(&mut batch).unwrap();
    }
    std::fs::write(&path, b"this is not a snapshot at all").unwrap();

    let mut server = venue(&journal);
    assert!(
        server.recover(Some(&path)).is_err(),
        "a corrupt snapshot was skipped, which would hide it for as long as the \
         venue kept running"
    );
}

#[test]
fn a_missing_snapshot_falls_back_to_replaying_everything() {
    let scratch = Scratch::new("missing");
    let journal = scratch.file("journal.log");
    let expected = {
        let mut server = venue(&journal);
        server.venue_mut().deposit(1, USD, u64::MAX / 4).unwrap();
        let mut batch = orders(1, 40);
        server.venue_mut().accept(&mut batch).unwrap();
        server
            .venue()
            .book(SYMBOL)
            .unwrap()
            .depth(Side::Bid, 10_000)
    };

    let mut server = venue(&journal);
    let replayed = server.recover(Some(&scratch.file("absent.snap"))).unwrap();
    assert_eq!(replayed, 41, "expected the deposit and every order");
    assert_eq!(
        server
            .venue()
            .book(SYMBOL)
            .unwrap()
            .depth(Side::Bid, 10_000),
        expected
    );
}

#[test]
fn a_snapshot_is_replaced_atomically_so_a_crash_leaves_the_old_one() {
    let scratch = Scratch::new("atomic");
    let journal = scratch.file("journal.log");
    let path = scratch.file("state.snap");
    let mut server = venue(&journal);
    server.venue_mut().deposit(1, USD, u64::MAX / 4).unwrap();
    server.snapshot_to(
        SnapshotPolicy::from_recovery_target(50, Duration::from_secs(1)),
        path.clone(),
    );

    let mut first = orders(1, 60);
    server.venue_mut().accept(&mut first).unwrap();
    let first_at = server.snapshot_if_due().unwrap().unwrap();

    let mut second = orders(1_000, 60);
    server.venue_mut().accept(&mut second).unwrap();
    let second_at = server.snapshot_if_due().unwrap().unwrap();
    assert!(
        second_at > first_at,
        "the second snapshot covered no new work"
    );

    // The staging file never survives a successful write.
    assert!(
        !path.with_extension("writing").exists(),
        "a half-written snapshot was left behind"
    );
    assert!(path.exists());
}
