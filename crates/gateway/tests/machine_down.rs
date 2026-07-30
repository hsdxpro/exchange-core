//! Machines dying, as processes actually die.
//!
//! `failover.rs` covers the leader being replaced by a different node.
//! `simulation.rs` crashes a venue in-process, from a seed. What neither covers
//! is the two most ordinary failures a deployment sees:
//!
//! - A **standalone venue** whose process is killed mid-trading and restarted on
//!   the same machine. No cluster, no promotion: recovery is the journal file
//!   and nothing else. This is every single-node deployment's crash story, and
//!   no test ran it against the shipped binary until this one.
//! - A **follower** dying while the leader keeps trading, then coming back.
//!   The unit tests prove readmission logic against an in-memory log; this
//!   proves the running system does it across real sockets with real files,
//!   and that the leader never stopped acknowledging while the follower was
//!   gone.
//!
//! Ports are fixed and unique across the test files (75xx here), because two
//! tests binding one port fail in a way that looks like the venue refusing
//! connections.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bx_gateway::codec::{FRAME_LEN, encode};
use bx_pipeline::{limit_order, subscribe};
use bx_protocol::{ChannelKind, Event, EventKind, Side};
use zerocopy::FromBytes;

/// Bytes in one journal record, and in the file header before them.
const RECORD_LEN: u64 = 64;
const MAGIC_LEN: u64 = 8;

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("bx-down-{}-{name}", std::process::id()));
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

/// A spawned process, killed when dropped, output drained for as long as it
/// lives so the child never blocks on a full pipe.
struct Process {
    child: Child,
    seen: Arc<Mutex<Vec<String>>>,
}

impl Process {
    fn start(binary: &str, args: &[&str]) -> Self {
        let mut child = Command::new(binary)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|e| panic!("could not start {binary}: {e}"));

        let stdout = child.stdout.take().expect("stdout was not captured");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let collected = Arc::clone(&seen);
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                collected.lock().unwrap().push(line);
            }
        });
        Self { child, seen }
    }

    fn wait_for(&self, needle: &str, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if self
                .seen
                .lock()
                .unwrap()
                .iter()
                .any(|line| line.contains(needle))
            {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }

    /// Kills the process without any shutdown courtesy, which is the point:
    /// a machine losing power does not flush buffers on the way down.
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Waits for the process to exit on its own, returning whether it failed.
    fn exited_with_failure(&mut self, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if let Ok(Some(status)) = self.child.try_wait() {
                return !status.success();
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        false
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Records a log file holds, past its header.
fn records(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |m| m.len().saturating_sub(MAGIC_LEN) / RECORD_LEN)
}

fn connect(address: &str) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match TcpStream::connect(address) {
            Ok(stream) => {
                stream.set_nodelay(true).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                return stream;
            }
            Err(e) if Instant::now() >= deadline => {
                panic!("could not reach the venue at {address}: {e}")
            }
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
}

/// Sends resting bids numbered from `first` and returns how many the venue
/// acknowledged. Prices step across 4,000 ticks so everything rests.
fn trade_from(stream: &mut TcpStream, first: u64, count: u64) -> u64 {
    let mut bytes = Vec::new();
    for i in 0..count {
        encode(
            &limit_order(
                1,
                1,
                first + i,
                Side::Bid,
                10_000 + ((first + i) % 4_000) as i64,
                1,
            ),
            &mut bytes,
        );
    }
    stream.write_all(&bytes).unwrap();

    let mut scratch = vec![0_u8; FRAME_LEN * 256];
    let mut acknowledged = 0;
    let mut answered = 0;
    let mut partial = 0;
    while answered < count {
        let Ok(read) = stream.read(&mut scratch[partial..]) else {
            break;
        };
        if read == 0 {
            break;
        }
        let filled = partial + read;
        let whole = filled / FRAME_LEN;
        for index in 0..whole {
            let start = index * FRAME_LEN;
            if let Ok(event) = Event::read_from_bytes(&scratch[start..start + FRAME_LEN]) {
                if event.kind == EventKind::Received as u8 {
                    acknowledged += 1;
                    answered += 1;
                } else if event.kind == EventKind::Rejected as u8 {
                    answered += 1;
                }
            }
        }
        scratch.copy_within(whole * FRAME_LEN..filled, 0);
        partial = filled - whole * FRAME_LEN;
    }
    acknowledged
}

/// One order, one group, one answer. Returns whether it was acknowledged.
///
/// Groups are what drive the replication loop, so a test that needs many groups
/// -- readmission fires every 256 -- has to send orders one at a time rather
/// than in a batch the venue would apply as one group.
fn one_order(stream: &mut TcpStream, scratch: &mut [u8], held: &mut usize, id: u64) -> bool {
    let mut bytes = Vec::new();
    encode(
        &limit_order(1, 1, id, Side::Bid, 10_000 + (id % 4_000) as i64, 1),
        &mut bytes,
    );
    stream.write_all(&bytes).unwrap();
    loop {
        let Ok(read) = stream.read(&mut scratch[*held..]) else {
            return false;
        };
        if read == 0 {
            return false;
        }
        let filled = *held + read;
        let whole = filled / FRAME_LEN;
        let mut outcome = None;
        for index in 0..whole {
            let start = index * FRAME_LEN;
            if let Ok(event) = Event::read_from_bytes(&scratch[start..start + FRAME_LEN]) {
                if event.kind == EventKind::Received as u8 {
                    outcome = Some(true);
                } else if event.kind == EventKind::Rejected as u8 && outcome.is_none() {
                    outcome = Some(false);
                }
            }
        }
        scratch.copy_within(whole * FRAME_LEN..filled, 0);
        *held = filled - whole * FRAME_LEN;
        if let Some(acknowledged) = outcome {
            return acknowledged;
        }
    }
}

/// Price levels the venue states when a client starts following the book.
fn book_levels(stream: &mut TcpStream) -> usize {
    let mut bytes = Vec::new();
    encode(&subscribe(1, 1, ChannelKind::Book), &mut bytes);
    stream.write_all(&bytes).unwrap();

    let mut scratch = vec![0_u8; FRAME_LEN * 512];
    let mut filled = 0;
    let mut levels = 0;
    stream
        .set_read_timeout(Some(Duration::from_millis(400)))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let Ok(read) = stream.read(&mut scratch[filled..]) else {
            break;
        };
        if read == 0 {
            break;
        }
        filled += read;
        let whole = filled / FRAME_LEN;
        for index in 0..whole {
            let start = index * FRAME_LEN;
            if let Ok(event) = Event::read_from_bytes(&scratch[start..start + FRAME_LEN])
                && event.kind == EventKind::BookSnapshot as u8
            {
                levels += 1;
            }
        }
        scratch.copy_within(whole * FRAME_LEN..filled, 0);
        filled -= whole * FRAME_LEN;
        if filled + FRAME_LEN > scratch.len() {
            filled = 0;
        }
    }
    levels
}

fn standalone_config(path: &Path, journal: &Path) -> PathBuf {
    let text = format!(
        "listen = 127.0.0.1:0\n\
         journal = {}\n\
         target_recovery_ms = 2000\n\
         replay_rate = 7600000\n\
         retained_per_channel = 4096\n\
         max_records_per_session = 256\n\
         max_sessions = 64\n\
         ack_timeout_ms = 1000\n\
         max_feed_memory_mb = 64\n\
         authentication = open\n\
         \n\
         [instrument]\n\
         symbol = 1\n\
         base = 1\n\
         quote = 2\n\
         floor_ticks = 10000\n\
         max_quantity = 1000000\n\
         max_open_orders = 100000\n",
        journal.display()
    );
    std::fs::write(path, text).unwrap();
    path.to_path_buf()
}

fn replicated_config(path: &Path, journal: &Path, followers: &[&str]) -> PathBuf {
    let replicas: String = followers
        .iter()
        .map(|address| format!("replica = {address}\n"))
        .collect();
    let text = format!(
        "listen = 127.0.0.1:0\n\
         journal = {}\n\
         target_recovery_ms = 2000\n\
         replay_rate = 7600000\n\
         retained_per_channel = 4096\n\
         max_records_per_session = 256\n\
         max_sessions = 64\n\
         ack_timeout_ms = 1000\n\
         term = 1\n\
         max_feed_memory_mb = 64\n\
         authentication = open\n\
         {replicas}\
         \n\
         [instrument]\n\
         symbol = 1\n\
         base = 1\n\
         quote = 2\n\
         floor_ticks = 10000\n\
         max_quantity = 1000000\n\
         max_open_orders = 100000\n",
        journal.display()
    );
    std::fs::write(path, text).unwrap();
    path.to_path_buf()
}

/// A single machine crashes and comes back. Everything a client was told
/// survives; nothing a client was not told appears.
///
/// This is the deployment with no cluster, where the journal file is the only
/// durability there is. The venue is killed the way power loss kills it -- no
/// shutdown path runs -- restarted with the same configuration, and then asked
/// three questions a real client would ask: is my resting order still there
/// (a duplicate ID must be refused), is the book intact (the stated snapshot
/// holds the levels), and are you actually serving (new orders acknowledged).
#[test]
fn a_standalone_venue_killed_mid_trading_recovers_everything_on_restart() {
    const ORDERS: u64 = 2_000;
    let scratch = Scratch::new("standalone");
    let journal = scratch.file("venue.log");
    let config = standalone_config(&scratch.file("venue.conf"), &journal);

    let mut venue = Process::start(
        env!("CARGO_BIN_EXE_venue"),
        &[
            "--config",
            config.to_str().unwrap(),
            "--listen",
            "127.0.0.1:7501",
        ],
    );
    assert!(
        venue.wait_for("listening", Duration::from_secs(60)),
        "the venue never came up"
    );

    let mut client = connect("127.0.0.1:7501");
    let acknowledged = trade_from(&mut client, 1, ORDERS);
    assert_eq!(
        acknowledged, ORDERS,
        "the venue did not acknowledge every order, so the rest proves nothing"
    );
    drop(client);

    // Die the way machines die.
    venue.kill();
    let on_disk = records(&journal);
    assert!(
        on_disk >= ORDERS,
        "the journal holds {on_disk} records for {ORDERS} acknowledged orders; \
         acknowledgements were sent for commands that were never durable"
    );

    // Same machine, same file, restarted.
    let restarted = Process::start(
        env!("CARGO_BIN_EXE_venue"),
        &[
            "--config",
            config.to_str().unwrap(),
            "--listen",
            "127.0.0.1:7501",
        ],
    );
    assert!(
        restarted.wait_for("recovered", Duration::from_secs(20)),
        "the restarted venue never reported recovering its journal"
    );
    assert!(
        restarted.wait_for("listening", Duration::from_secs(60)),
        "the restarted venue never started serving"
    );

    // The book, not the bytes: the stated snapshot must hold the levels the
    // dead process was resting. 2,000 orders across 2,000 distinct prices.
    let mut reader = connect("127.0.0.1:7501");
    let levels = book_levels(&mut reader);
    assert!(
        levels >= 1_900,
        "the restarted venue states {levels} price levels for {ORDERS} resting \
         orders; it recovered the journal and lost the book"
    );

    // An ID acknowledged before the crash must be refused after it. If this is
    // accepted, the venue forgot the order it acknowledged -- which is exactly
    // the lie a client cannot detect on its own.
    let mut client = connect("127.0.0.1:7501");
    let mut scratch_buf = vec![0_u8; FRAME_LEN * 256];
    let mut held = 0;
    assert!(
        !one_order(&mut client, &mut scratch_buf, &mut held, 1),
        "a duplicate of a pre-crash order was accepted, so the acknowledged \
         order it duplicates is gone"
    );

    // And it is a venue, not a museum: fresh orders still trade.
    assert!(
        one_order(&mut client, &mut scratch_buf, &mut held, 500_000),
        "the restarted venue refuses new orders"
    );
}

/// A follower dies; the leader keeps acknowledging. The follower comes back;
/// it is readmitted and backfilled to exactly the leader's log.
///
/// The quorum is sized for this: three nodes, so one follower confirming is a
/// majority with the leader. The dangerous outcomes on each side of the
/// restart are different. While the follower is down, the venue stalling would
/// mean one machine's failure stopped trading -- the thing replication exists
/// to prevent. After it returns, staying behind forever would mean the cluster
/// is quietly one failure from data loss while reporting itself healthy.
#[test]
fn a_dead_follower_neither_stops_the_leader_nor_stays_gone() {
    let scratch = Scratch::new("follower");
    let a_log = scratch.file("a.log");
    let b_log = scratch.file("b.log");
    let leader_log = scratch.file("leader.log");

    let mut replica_a = Process::start(
        env!("CARGO_BIN_EXE_replica"),
        &["127.0.0.1:7502", "--file", a_log.to_str().unwrap()],
    );
    let replica_b = Process::start(
        env!("CARGO_BIN_EXE_replica"),
        &["127.0.0.1:7503", "--file", b_log.to_str().unwrap()],
    );
    assert!(
        replica_a.wait_for("replica listening", Duration::from_secs(10))
            && replica_b.wait_for("replica listening", Duration::from_secs(10)),
        "the followers never came up"
    );

    let config = replicated_config(
        &scratch.file("leader.conf"),
        &leader_log,
        &["127.0.0.1:7502", "127.0.0.1:7503"],
    );
    let leader = Process::start(
        env!("CARGO_BIN_EXE_venue"),
        &[
            "--config",
            config.to_str().unwrap(),
            "--listen",
            "127.0.0.1:7504",
        ],
    );
    assert!(
        leader.wait_for("listening", Duration::from_secs(60)),
        "the leader never came up"
    );

    let mut client = connect("127.0.0.1:7504");
    let mut scratch_buf = vec![0_u8; FRAME_LEN * 256];
    let mut held = 0;

    // Healthy cluster first, so a failure later is attributable.
    for id in 1..=100_u64 {
        assert!(
            one_order(&mut client, &mut scratch_buf, &mut held, id),
            "order {id} was not acknowledged by a healthy cluster"
        );
    }

    // One machine down.
    replica_a.kill();

    // The leader must keep acknowledging on the surviving majority, through the
    // moment it discovers the death (one group eats the ack timeout) and every
    // group after. 300 groups also crosses the readmission cadence (256), so
    // the leader is provably attempting to bring the dead node back while it is
    // still dead -- and surviving that attempt failing.
    for id in 101..=400_u64 {
        assert!(
            one_order(&mut client, &mut scratch_buf, &mut held, id),
            "order {id} was not acknowledged while one of two followers was down; \
             a single machine's failure stopped the venue"
        );
    }

    // The machine comes back, same address, same file.
    let replica_a_again = Process::start(
        env!("CARGO_BIN_EXE_replica"),
        &["127.0.0.1:7502", "--file", a_log.to_str().unwrap()],
    );
    assert!(
        replica_a_again.wait_for("replica listening", Duration::from_secs(10)),
        "the restarted follower never came up"
    );

    // Trade until the leader's next readmission sweep finds it, rejoins it, and
    // backfills the gap. The sweep runs every 256 groups, so 300 more single-
    // order groups guarantees one; the poll below allows the backfill itself
    // time to land.
    for id in 401..=700_u64 {
        assert!(
            one_order(&mut client, &mut scratch_buf, &mut held, id),
            "order {id} was not acknowledged after the follower returned"
        );
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    let target = records(&leader_log);
    assert!(target >= 700, "the leader's own log is short: {target}");
    loop {
        let caught_up = records(&a_log);
        if caught_up >= target {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the returned follower holds {caught_up} of the leader's {target} \
             records; it was never backfilled, so the cluster is one failure \
             from losing acknowledged orders while calling itself healthy"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // The healthy follower was never disturbed: it holds everything too.
    assert!(
        records(&b_log) >= target,
        "the follower that never failed is missing records"
    );
}

/// A journal that rotted on disk stops the venue before it serves anyone.
///
/// The journal is the only source of truth, so a venue that starts over a
/// corrupt one is a venue whose books silently disagree with what clients were
/// told before the corruption. Refusing to start is the only honest answer,
/// and it must happen at startup -- an operator restarting after a disk scare
/// finds out now, not when replay produces a wrong book under load.
///
/// A torn *trailing* write is different and must keep working: that is the
/// ordinary residue of a crash, and `simulation.rs` proves it recovers. This
/// is corruption in the middle, which no crash produces and only a bad disk
/// or a bad restore can.
#[test]
fn a_corrupt_journal_stops_the_venue_before_it_serves() {
    const ORDERS: u64 = 200;
    let scratch = Scratch::new("corrupt");
    let journal = scratch.file("venue.log");
    let config = standalone_config(&scratch.file("venue.conf"), &journal);

    let mut venue = Process::start(
        env!("CARGO_BIN_EXE_venue"),
        &[
            "--config",
            config.to_str().unwrap(),
            "--listen",
            "127.0.0.1:7505",
        ],
    );
    assert!(
        venue.wait_for("listening", Duration::from_secs(60)),
        "the venue never came up"
    );
    let mut client = connect("127.0.0.1:7505");
    assert_eq!(trade_from(&mut client, 1, ORDERS), ORDERS);
    drop(client);
    venue.kill();

    // Rot a record in the middle: its discriminants become values no version
    // defines. Not the tail -- a torn tail is legitimate.
    let mut bytes = std::fs::read(&journal).unwrap();
    let middle = (MAGIC_LEN + (ORDERS / 2) * RECORD_LEN) as usize;
    for byte in &mut bytes[middle..middle + 8] {
        *byte = 0xFF;
    }
    std::fs::write(&journal, &bytes).unwrap();

    let mut restarted = Process::start(
        env!("CARGO_BIN_EXE_venue"),
        &[
            "--config",
            config.to_str().unwrap(),
            "--listen",
            "127.0.0.1:7505",
        ],
    );
    assert!(
        restarted.exited_with_failure(Duration::from_secs(20)),
        "the venue started over a corrupt journal instead of refusing; whatever \
         it is serving cannot match what clients were told before the corruption"
    );
    assert!(
        !restarted.wait_for("listening", Duration::from_secs(1)),
        "the venue listened before noticing its journal is corrupt"
    );
}

/// A client whose outcome events died with the venue is told them on
/// reconnect, without asking.
///
/// The ack means "received and durable" and the outcome follows as its own
/// event, so a venue that dies in between leaves a client holding an ack it
/// used to only be able to resolve by querying. The watermark closes that: the
/// venue journals "everything before me was handed to the feed", recovery
/// regenerates the outcomes past the last marker, and the first session to act
/// for the account is handed them, sequence zero, before anything else.
#[test]
fn a_reconnecting_client_is_told_the_outcomes_that_died_with_the_venue() {
    const ORDERS: u64 = 100;
    let scratch = Scratch::new("watermark");
    let journal = scratch.file("venue.log");
    let config = standalone_config(&scratch.file("venue.conf"), &journal);

    let mut venue = Process::start(
        env!("CARGO_BIN_EXE_venue"),
        &[
            "--config",
            config.to_str().unwrap(),
            "--listen",
            "127.0.0.1:7507",
        ],
    );
    assert!(
        venue.wait_for("listening", Duration::from_secs(60)),
        "the venue never came up"
    );
    let mut client = connect("127.0.0.1:7507");
    let acknowledged = trade_from(&mut client, 1, ORDERS);
    assert_eq!(acknowledged, ORDERS, "not every order was acknowledged");
    drop(client);
    venue.kill();

    let restarted = Process::start(
        env!("CARGO_BIN_EXE_venue"),
        &[
            "--config",
            config.to_str().unwrap(),
            "--listen",
            "127.0.0.1:7507",
        ],
    );
    assert!(
        restarted.wait_for("listening", Duration::from_secs(60)),
        "the restarted venue never started serving"
    );

    // Same account reconnects and sends one fresh order -- its first command,
    // which is what attaches the account. It must be handed the recovered
    // outcomes it was never told, marked as redelivery by sequence zero. It
    // never sends QueryOpenOrders.
    let mut client = connect("127.0.0.1:7507");
    let mut bytes = Vec::new();
    encode(
        &limit_order(1, 1, 500_000, Side::Bid, 10_000, 1),
        &mut bytes,
    );
    client.write_all(&bytes).unwrap();

    let mut scratch_buf = vec![0_u8; FRAME_LEN * 512];
    let mut held = 0;
    let mut redelivered = 0_u64;
    let mut fresh_acknowledged = false;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && !(redelivered > 0 && fresh_acknowledged) {
        let Ok(read) = client.read(&mut scratch_buf[held..]) else {
            break;
        };
        if read == 0 {
            break;
        }
        let filled = held + read;
        let whole = filled / FRAME_LEN;
        for index in 0..whole {
            let start = index * FRAME_LEN;
            if let Ok(event) = Event::read_from_bytes(&scratch_buf[start..start + FRAME_LEN]) {
                if event.kind == EventKind::Resting as u8
                    && event.sequence == 0
                    && event.order_id <= ORDERS
                {
                    redelivered += 1;
                }
                if event.kind == EventKind::Received as u8 && event.order_id == 500_000 {
                    fresh_acknowledged = true;
                }
            }
        }
        scratch_buf.copy_within(whole * FRAME_LEN..filled, 0);
        held = filled - whole * FRAME_LEN;
    }
    assert!(
        redelivered > 0,
        "the reconnecting client was told nothing it was owed; \
         it would have had to query for outcomes the venue acknowledged"
    );
    assert!(fresh_acknowledged, "the venue stopped serving new orders");
}
