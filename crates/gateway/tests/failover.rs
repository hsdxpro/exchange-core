//! Failover, as real processes.
//!
//! Spawns the same `venue` and `replica` binaries a deployment runs, kills the
//! leader mid-session, promotes a node with an empty log at a higher term, and
//! checks it recovers everything the dead leader had acknowledged.
//!
//! This is the one property that cannot be tested in a single process. Every
//! other durability test shares a heap with the thing it is testing, so a leader
//! whose journal was never written to disk still looks correct — which is exactly
//! the bug this scenario found by hand: with replication enabled, both the
//! leader's file and the follower's held nothing but their eight magic bytes
//! after ten thousand acknowledged orders, because one buffered until `sync` and
//! neither ever synced.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Bytes in one journal record, and in the file header before them.
const RECORD_LEN: u64 = 64;
const MAGIC_LEN: u64 = 8;

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("bx-failover-{}-{name}", std::process::id()));
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

/// A spawned process: killed when the test ends, and with its output drained for
/// as long as it lives.
///
/// Draining matters. Reading a child's stdout and then dropping the reader closes
/// the pipe, and the child's next `println!` panics -- so a helper that waits for
/// a "ready" line and then lets go kills the process it just waited for. That is
/// how the follower in this test kept dying, which looked exactly like a
/// promotion failing to reach a majority.
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
        // Runs until the child closes its stdout, which keeps the pipe open for
        // the child's whole life.
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                collected.lock().unwrap().push(line);
            }
        });
        Self { child, seen }
    }

    /// Waits until the process has printed a line containing `needle`.
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

    fn stop(&mut self) {
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
        self.stop();
    }
}

/// A configuration file for one node: its own journal, one follower, one term.
fn write_config(path: &Path, journal: &Path, follower: &str, term: u64) -> PathBuf {
    let text = format!(
        "listen = 127.0.0.1:0\n\
         journal = {}\n\
         target_recovery_ms = 2000\n\
         replay_rate = 7600000\n\
         retained_per_channel = 4096\n\
         max_records_per_session = 256\n\
         max_sessions = 64\n\
         ack_timeout_ms = 1000\n\
         term = {term}\n\
         max_feed_memory_mb = 64\n\
         authentication = open\n\
         replica = {follower}\n\
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

/// Records a log file holds, past its header.
fn records(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |m| m.len().saturating_sub(MAGIC_LEN) / RECORD_LEN)
}

/// Sends `count` resting orders and waits until each is acknowledged, so the test
/// only measures what a client was actually told.
fn trade(address: &str, count: u64) -> u64 {
    use bx_gateway::codec::{FRAME_LEN, encode};
    use bx_pipeline::limit_order;
    use bx_protocol::{Event, EventKind, Side};
    use zerocopy::FromBytes;

    let mut stream = TcpStream::connect(address).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();

    let mut bytes = Vec::new();
    for i in 0..count {
        encode(
            &limit_order(1, 1, i + 1, Side::Bid, 10_000 + (i % 4_000) as i64, 1),
            &mut bytes,
        );
    }
    stream.write_all(&bytes).unwrap();

    let mut scratch = vec![0_u8; FRAME_LEN * 256];
    let mut acknowledged = 0;
    let mut partial = 0;
    while acknowledged < count {
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
            if let Ok(event) = Event::read_from_bytes(&scratch[start..start + FRAME_LEN])
                && event.kind == EventKind::Received as u8
            {
                acknowledged += 1;
            }
        }
        scratch.copy_within(whole * FRAME_LEN..filled, 0);
        partial = filled - whole * FRAME_LEN;
    }
    acknowledged
}

#[test]
fn a_promoted_node_recovers_what_the_dead_leader_acknowledged() {
    const ORDERS: u64 = 2_000;
    let scratch = Scratch::new("promote");
    let follower_journal = scratch.file("follower.log");
    let first_journal = scratch.file("leader1.log");
    let second_journal = scratch.file("leader2.log");

    // A follower with a durable log, so it outlives the leader.
    let replica = Process::start(
        env!("CARGO_BIN_EXE_replica"),
        &[
            "127.0.0.1:7401",
            "--file",
            follower_journal.to_str().unwrap(),
        ],
    );
    assert!(
        replica.wait_for("replica listening", Duration::from_secs(10)),
        "the follower never came up"
    );

    // The first leader, term 1.
    let first_config = write_config(
        &scratch.file("leader1.conf"),
        &first_journal,
        "127.0.0.1:7401",
        1,
    );
    let mut leader = Process::start(
        env!("CARGO_BIN_EXE_venue"),
        &[
            "--config",
            first_config.to_str().unwrap(),
            "--listen",
            "127.0.0.1:7400",
        ],
    );
    assert!(
        leader.wait_for("listening", Duration::from_secs(20)),
        "the first leader never came up"
    );

    let acknowledged = trade("127.0.0.1:7400", ORDERS);
    assert_eq!(
        acknowledged, ORDERS,
        "the leader did not acknowledge every order, so the rest of this proves nothing"
    );

    // Kill it, as a machine failure would.
    leader.stop();

    // Both copies exist on disk. This is the assertion that catches a leader
    // relying entirely on its followers, or a follower holding nothing but memory.
    let on_leader = records(&first_journal);
    let on_follower = records(&follower_journal);
    assert!(
        on_leader >= ORDERS,
        "the dead leader's own log holds {on_leader} records for {ORDERS} acknowledged orders"
    );
    assert_eq!(
        on_follower, on_leader,
        "the follower does not hold what the leader acknowledged"
    );

    // A different node, empty log, higher term.
    let second_config = write_config(
        &scratch.file("leader2.conf"),
        &second_journal,
        "127.0.0.1:7401",
        2,
    );
    assert_eq!(
        records(&second_journal),
        0,
        "the promoted node is not empty"
    );
    let promoted = Process::start(
        env!("CARGO_BIN_EXE_venue"),
        &[
            "--config",
            second_config.to_str().unwrap(),
            "--listen",
            "127.0.0.1:7402",
        ],
    );
    assert!(
        promoted.wait_for("listening", Duration::from_secs(40)),
        "the promoted node never started serving"
    );

    // It caught up before serving: everything the dead leader acknowledged.
    let recovered = records(&second_journal);
    assert!(
        recovered >= on_leader,
        "the promoted node serves with {recovered} records against the {on_leader} \
         that were acknowledged, so orders a client was told about are missing"
    );

    // And it is a working venue, not just a copy of a log.
    assert_eq!(
        trade("127.0.0.1:7402", 100),
        100,
        "the promoted node does not accept new orders"
    );
}

#[test]
fn a_node_that_cannot_reach_a_majority_refuses_to_start() {
    // It cannot know what it is missing, so serving would risk losing an order a
    // client was told about. Refusing is the only safe answer, and it has to be
    // the observable behaviour rather than a comment.
    let scratch = Scratch::new("no-majority");
    let config = write_config(
        &scratch.file("lonely.conf"),
        &scratch.file("lonely.log"),
        // Nothing is listening here.
        "127.0.0.1:7499",
        3,
    );
    let mut node = Process::start(
        env!("CARGO_BIN_EXE_venue"),
        &[
            "--config",
            config.to_str().unwrap(),
            "--listen",
            "127.0.0.1:7498",
        ],
    );
    let served = node.wait_for("listening", Duration::from_secs(5));
    assert!(
        !served,
        "a node served clients without reaching a majority of its cluster"
    );
    assert!(
        node.exited_with_failure(Duration::from_secs(30)),
        "it should have exited with a failure rather than lingering"
    );
}
