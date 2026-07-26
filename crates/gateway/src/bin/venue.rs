//! The venue, as a process.
//!
//! Run it, point clients at it. This is the same `Server` the integration tests
//! drive, with nothing added for production and nothing removed for testing —
//! which is the point of having it: a multi-process test exercises exactly the
//! binary a deployment would run.
//!
//! ```text
//! venue [address] [--file PATH] [--snapshot PATH]
//! ```
//!
//! With `--file` the journal is a real file and the venue survives a restart.
//! Without it the journal is in memory, which is what a throughput measurement
//! wants: an in-memory log measures the venue, a real file measures the disk.
//!
//! With `--snapshot` the venue writes its state periodically and a restart
//! starts from it instead of replaying the journal from the beginning. The
//! cadence comes from a recovery-time target rather than being picked, and the
//! journal stays authoritative: deleting the snapshot costs recovery time and
//! nothing else.

use bx_gateway::tcp::{Server, SnapshotPolicy};
use bx_journal::{FileLog, LogStorage, MemoryLog};
use bx_pipeline::instrument::{Instrument, Instruments};
use std::path::PathBuf;
use std::time::Duration;

const BTC: u32 = 1;
const USD: u32 = 2;
const SYMBOL: u32 = 1;
const FLOOR: i64 = 10_000;
const MAX_QUANTITY: u64 = 1_000_000;
const MAX_OPEN_ORDERS: u32 = 1_000_000;
const RETAINED_PER_CHANNEL: usize = 1 << 16;
const MAX_RECORDS_PER_SESSION: usize = 4_096;
/// Accounts credited at startup, so a client can trade without a funding API.
const ACCOUNTS: std::ops::RangeInclusive<u64> = 1..=16;
const STARTING_BALANCE: u64 = u64::MAX / 4;

/// Commands a second the venue replays at, from `cargo x latency`: 100,000
/// records in 13.1 ms. Re-measure on the machine that will run it -- the figure
/// depends on the traffic mix, and a snapshot cadence derived from someone
/// else's hardware is a guess wearing a number.
const REPLAY_RATE: u64 = 7_600_000;

/// How long a restart may take. The only knob here, and the one an operator
/// actually has an opinion about.
const TARGET_RECOVERY: Duration = Duration::from_secs(2);

fn instruments() -> Instruments {
    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(
        SYMBOL,
        BTC,
        USD,
        FLOOR,
        MAX_QUANTITY,
        MAX_OPEN_ORDERS,
    ));
    instruments
}

fn run<S: LogStorage>(
    address: &str,
    storage: S,
    fresh: bool,
    snapshot: Option<PathBuf>,
) -> std::io::Result<()> {
    let mut server = Server::bind(
        address,
        storage,
        instruments(),
        RETAINED_PER_CHANNEL,
        MAX_RECORDS_PER_SESSION,
    )?;

    if fresh {
        for account in ACCOUNTS {
            for asset in [USD, BTC] {
                server
                    .venue_mut()
                    .deposit(account, asset, STARTING_BALANCE)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
            }
        }
    } else {
        let replayed = server.recover(snapshot.as_deref())?;
        println!("recovered {replayed} commands");
    }

    if let Some(path) = snapshot {
        let policy = SnapshotPolicy::from_recovery_target(REPLAY_RATE, TARGET_RECOVERY);
        println!(
            "snapshotting every {} commands, for a {:?} recovery target",
            policy.interval(),
            TARGET_RECOVERY
        );
        server.snapshot_to(policy, path);
    }

    // Ready only once the port is bound and state is restored, so a parent
    // process can wait for this line instead of sleeping and hoping.
    println!("listening {}", server.address()?);
    loop {
        if let Err(e) = server.poll() {
            eprintln!("venue stopped: {e}");
            return Err(std::io::Error::other(e.to_string()));
        }
        // A failed snapshot is not fatal. The journal is still authoritative, so
        // it costs recovery time and nothing else -- stopping the venue over it
        // would turn a slow restart into an outage.
        if let Err(e) = server.snapshot_if_due() {
            eprintln!("snapshot failed, continuing: {e}");
        }
    }
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let address = args
        .first()
        .filter(|a| !a.starts_with("--"))
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:7070".to_string());

    let file = args
        .iter()
        .position(|a| a == "--file")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from);

    let snapshot = args
        .iter()
        .position(|a| a == "--snapshot")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from);

    match file {
        Some(path) => {
            let fresh = !path.exists();
            let log = FileLog::open(&path).map_err(|e| std::io::Error::other(e.to_string()))?;
            run(&address, log, fresh, snapshot)
        }
        None => run(&address, MemoryLog::new(), true, snapshot),
    }
}
