//! The venue, as a process.
//!
//! Run it, point clients at it. This is the same `Server` the integration tests
//! drive, with nothing added for production and nothing removed for testing —
//! which is the point of having it: a multi-process test exercises exactly the
//! binary a deployment would run.
//!
//! ```text
//! venue [--config PATH] [--listen ADDRESS]
//! ```
//!
//! Everything comes from the configuration file; `venue.conf` at the repo root
//! is an annotated example. `--listen` overrides the address in it, which is
//! what a test harness needs and the one setting worth overriding.
//!
//! With no `--config` the venue starts on an in-memory journal with a single
//! instrument. That is a measurement setup, not a deployment: an in-memory log
//! measures the venue, a real file measures the disk.

use bx_gateway::config::Config;
use bx_gateway::tcp::{Server, SnapshotPolicy};
use bx_journal::{FileLog, JournalError, LogStorage, MemoryLog, ReplicatedLog};
use bx_pipeline::instrument::{Instrument, Instruments};
use std::path::Path;
use std::time::Duration;

/// Accounts credited at startup, so a client can trade without a funding API.
/// A real venue funds through deposits from its own account service.
const ACCOUNTS: std::ops::RangeInclusive<u64> = 1..=16;
const STARTING_BALANCE: u64 = u64::MAX / 4;

/// What the venue runs as when no configuration file is given: one instrument,
/// journal in memory. Enough to point the load harness at, and not a deployment.
fn measurement_config() -> Config {
    const BTC: u32 = 1;
    const USD: u32 = 2;
    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(1, BTC, USD, 10_000, 1_000_000, 1_000_000));
    Config {
        listen: "127.0.0.1:7070".to_string(),
        journal: None,
        snapshot: None,
        target_recovery: Duration::from_secs(2),
        replay_rate: 7_600_000,
        retained_per_channel: 1 << 16,
        max_records_per_session: 4_096,
        max_sessions: 4_096,
        replicas: Vec::new(),
        ack_timeout: Duration::from_millis(250),
        term: 1,
        max_feed_memory: 64 * 1024 * 1024,
        instruments,
    }
}

fn listed(config: &Config) -> Instruments {
    let mut instruments = Instruments::new();
    for instrument in config.instruments.iter() {
        instruments.insert(*instrument);
    }
    instruments
}

fn run<S: LogStorage>(config: &Config, storage: S, fresh: bool) -> std::io::Result<()> {
    let mut server = Server::bind(
        &config.listen,
        storage,
        listed(config),
        config.retained_per_channel,
        config.max_records_per_session,
        config.max_sessions,
    )?;

    if fresh {
        for account in ACCOUNTS {
            for instrument in config.instruments.iter() {
                for asset in [instrument.base, instrument.quote] {
                    server
                        .venue_mut()
                        .deposit(account, asset, STARTING_BALANCE)
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                }
            }
        }
    } else {
        let replayed = server.recover(config.snapshot.as_deref())?;
        println!("recovered {replayed} commands");
    }

    if let Some(path) = config.snapshot.clone() {
        let policy =
            SnapshotPolicy::from_recovery_target(config.replay_rate, config.target_recovery);
        println!(
            "snapshotting every {} commands, for a {:?} recovery target",
            policy.interval(),
            config.target_recovery
        );
        server.snapshot_to(policy, path);
    }
    let symbols = config.instruments.iter().count();
    println!(
        "feed retains {} events a channel: {} MiB across {symbols} instrument(s)",
        config.retained_per_channel,
        bx_gateway::config::feed_memory(config.retained_per_channel, symbols) / 1024 / 1024
    );
    if !config.replicas.is_empty() {
        println!(
            "replicating to {} follower(s) as term {}",
            config.replicas.len(),
            config.term
        );
    }

    // Printed only once the port is bound and state is restored, so a parent
    // process can wait for this line instead of sleeping and hoping.
    println!("listening {}", server.address()?);
    loop {
        if let Err(e) = server.poll() {
            eprintln!("venue stopped: {e}");
            return Err(std::io::Error::other(e.to_string()));
        }
        // A failed snapshot is not fatal. The journal is still authoritative, so
        // it costs recovery time and nothing else; stopping the venue over it
        // would turn a slow restart into an outage.
        if let Err(e) = server.snapshot_if_due() {
            eprintln!("snapshot failed, continuing: {e}");
        }
    }
}

/// Opens the journal the configuration asks for, wrapped in replication when
/// followers are listed.
fn start(config: &Config) -> std::io::Result<()> {
    let other = |e: JournalError| std::io::Error::other(e.to_string());

    match config.journal.as_deref() {
        Some(path) => {
            let fresh = !path.exists();
            let log = FileLog::open(path).map_err(other)?;
            if config.replicas.is_empty() {
                run(config, log, fresh)
            } else {
                run(config, promoted(config, log)?, fresh)
            }
        }
        None if config.replicas.is_empty() => run(config, MemoryLog::new(), true),
        None => run(config, promoted(config, MemoryLog::new())?, true),
    }
}

/// Connects to the followers and brings this node's log up to the longest any
/// majority holds, before it serves anybody.
///
/// A group is acknowledged once a majority holds it and this contacts a majority,
/// so the two intersect: whatever a client was told is on at least one node that
/// answers, and the longest log recovers all of it. A node that cannot reach a
/// majority refuses to start rather than serving without knowing what it missed.
fn promoted<S: LogStorage>(config: &Config, storage: S) -> std::io::Result<ReplicatedLog<S>> {
    let mut log =
        ReplicatedLog::connect(storage, &config.replicas, config.ack_timeout, config.term)?;
    let recovered = log.catch_up()?;
    if recovered > 0 {
        println!("caught up {recovered} bytes from a follower before serving",);
    }
    Ok(log)
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flag = |name: &str| -> Option<String> {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };

    // A bad configuration stops the venue here rather than being half-applied.
    // The message names the line it is on.
    let mut config = match flag("--config") {
        Some(path) => {
            Config::read(Path::new(&path)).map_err(|e| std::io::Error::other(e.to_string()))?
        }
        None => {
            println!("no --config given: in-memory journal, one instrument");
            measurement_config()
        }
    };
    // A positional address, which is what the load harness passes.
    if let Some(address) = args.first().filter(|a| !a.starts_with("--")) {
        config.listen = address.clone();
    }
    if let Some(address) = flag("--listen") {
        config.listen = address;
    }

    start(&config)
}
