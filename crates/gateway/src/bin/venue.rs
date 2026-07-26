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

use bx_election::Leadership;
use bx_gateway::auth::{Credentials, Mode as AuthMode};
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

/// How long a promoted node keeps trying to reach a majority before giving up and
/// refusing to serve. Long enough to outlast a follower finishing with the leader
/// that just died.
const PROMOTION_WINDOW: Duration = Duration::from_secs(15);

/// Commands between metric reports. Counted rather than timed so an idle venue
/// says nothing and the loop never reads a clock on its account.
const REPORT_EVERY: u64 = 1_000_000;

/// How long a standby waits to be elected before saying so and waiting again.
/// Not a failure -- a standby that is never elected is a standby doing its job.
const ELECTION_PATIENCE: Duration = Duration::from_secs(30);

/// How long to wait before standing again after a setback.
///
/// Short enough that a real failover is not delayed by it, long enough that a
/// node which keeps failing to take up the leadership is not spinning a core.
const SETBACK: Duration = Duration::from_millis(500);

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
        // No election: one node, leading unconditionally. An election needs a
        // majority, and a majority of one is a formality with a cost.
        node_id: 1,
        peers: Vec::new(),
        leadership_state: None,
        max_feed_memory: 64 * 1024 * 1024,
        // Open, and said out loud at startup. This mode exists to point the load
        // harness at, and a measurement run has no secrets to distribute.
        authentication: AuthMode::Open,
        credentials: Credentials::new(),
        rate_limit: None,
        // Off for measurement: two wall-clock readings a pass would show up in
        // the very numbers this configuration exists to produce.
        timestamps: false,
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

fn run<S: LogStorage>(
    config: &Config,
    storage: S,
    fresh: bool,
    leadership: Option<&Leadership>,
) -> std::io::Result<()> {
    let mut server = Server::bind(
        &config.listen,
        storage,
        listed(config),
        config.retained_per_channel,
        config.max_records_per_session,
        config.max_sessions,
    )?;

    match config.authentication {
        AuthMode::Required => {
            server.require_authentication(config.credentials.clone());
            println!(
                "authentication required, {} account(s) credentialled",
                config.credentials.len()
            );
        }
        // Loud, because the difference between this and a venue that meant to be
        // open is a line in a file.
        AuthMode::Open => {
            println!("AUTHENTICATION OFF: any session may act for any account");
        }
    }
    server.stamp_times(config.timestamps);
    if config.timestamps {
        println!("stamping arrival and match times");
    }
    if let Some(limit) = config.rate_limit {
        server.rate_limit(limit);
        println!(
            "rate limit {}/sec per account, bursting to {}",
            limit.per_second(),
            limit.burst()
        );
    }

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
    let mut reported_at = 0_u64;
    loop {
        // Checked before every pass, because a deposed leader must stop taking
        // orders rather than discover on its next commit that a majority has
        // moved on without it. One relaxed load; it does not show up against a
        // pass measured in hundreds of nanoseconds.
        if leadership.is_some_and(|held| !held.is_leader()) {
            println!("no longer the leader; closing the door");
            return Ok(());
        }
        if let Err(e) = server.poll() {
            eprintln!("venue stopped: {e}");
            return Err(std::io::Error::other(e.to_string()));
        }
        // Reported against commands rather than a clock, so an idle venue stays
        // silent and a busy one reports often enough to be useful — and so the
        // loop never reads a clock it would not otherwise read.
        let commands = server.metrics().commands();
        if commands >= reported_at + REPORT_EVERY {
            reported_at = commands;
            println!("{}", server.metrics().report());
            let refused = server.venue().exchange().rejects_by_reason();
            if !refused.is_empty() {
                let named: Vec<String> = refused
                    .iter()
                    .take(5)
                    .map(|(reason, count)| format!("{reason}: {count}"))
                    .collect();
                println!("  rejects       {}", named.join(", "));
            }
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
fn start(config: &Config, term: u64, leadership: Option<&Leadership>) -> std::io::Result<()> {
    let other = |e: JournalError| std::io::Error::other(e.to_string());

    match config.journal.as_deref() {
        Some(path) => {
            let fresh = !path.exists();
            let log = FileLog::open(path).map_err(other)?;
            if config.replicas.is_empty() {
                run(config, log, fresh, leadership)
            } else {
                run(config, promoted(config, log, term)?, fresh, leadership)
            }
        }
        None if config.replicas.is_empty() => run(config, MemoryLog::new(), true, leadership),
        None => run(
            config,
            promoted(config, MemoryLog::new(), term)?,
            true,
            leadership,
        ),
    }
}

/// Serves for as long as this node is the leader, and no longer.
///
/// This is the loop that removes the person. Before it, a promotion meant
/// someone noticing a dead venue, editing a higher term into a file, and
/// starting a process. Now the node waits until the cluster elects it, proves it
/// can still reach a majority, catches its log up to what any majority holds,
/// serves — and stops the instant it is no longer the leader, because the node
/// that replaced it holds a higher term and every write this one made would be
/// refused anyway.
///
/// Everything is rebuilt on each promotion rather than kept warm. That is not
/// laziness: a node that has been out of the leadership does not know what it
/// missed, and the only honest way to find out is to catch up and replay. Doing
/// it any other way would mean serving from state whose provenance nobody can
/// state.
fn lead(config: &Config, leadership: &Leadership) -> std::io::Result<()> {
    loop {
        println!("standing for election as node {}", leadership.id());
        let Some(term) = leadership.await_leadership(ELECTION_PATIENCE) else {
            // Not an error. Another node holds it, and this one is a standby
            // doing exactly what a standby should: nothing.
            continue;
        };
        println!("elected leader for term {term}");

        // Proves the node can still reach a majority *before* it takes an
        // order, rather than discovering it could not on the first commit.
        if let Err(e) = leadership.announce() {
            eprintln!("elected but could not reach a majority to announce: {e}");
            // A pause, because this node may still hold the leadership: without
            // it the next `await_leadership` returns immediately and the loop
            // spins on a failing announce as fast as the machine allows, which
            // is a standby burning a core over a partition it cannot fix.
            std::thread::sleep(SETBACK);
            continue;
        }

        match start(config, term, Some(leadership)) {
            Ok(()) => println!("stepped down from term {term}; standing by"),
            Err(e) => {
                // A leader that cannot commit must stop leading, but the process
                // stays up: it is a perfectly good standby, and exiting would
                // shrink the cluster that has to elect its replacement.
                eprintln!("stopped serving term {term}: {e}");
            }
        }
        // Same reason: a failure that returns at once -- a port not yet released,
        // a follower still unreachable -- would otherwise be retried in a tight
        // loop rather than in a moment.
        std::thread::sleep(SETBACK);
    }
}

/// Connects to the followers and brings this node's log up to the longest any
/// majority holds, before it serves anybody.
///
/// A group is acknowledged once a majority holds it and this contacts a majority,
/// so the two intersect: whatever a client was told is on at least one node that
/// answers, and the longest log recovers all of it. A node that cannot reach a
/// majority refuses to start rather than serving without knowing what it missed.
fn promoted<S: LogStorage>(
    config: &Config,
    storage: S,
    term: u64,
) -> std::io::Result<ReplicatedLog<S>> {
    let mut log = ReplicatedLog::connect(storage, &config.replicas, config.ack_timeout, term)?;
    // Retried against a deadline: at the moment of a promotion a follower may
    // still be finishing with the leader that just died.
    let recovered = log.catch_up(PROMOTION_WINDOW)?;
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

    if config.peers.is_empty() {
        // No election configured: this node leads unconditionally, on the term
        // in the file. The single-node and measurement shape, and the one a
        // person still has to promote.
        return start(&config, config.term, None);
    }

    let state = config
        .leadership_state
        .clone()
        .ok_or_else(|| std::io::Error::other("peers are listed but leadership_state is not"))?;
    let leadership = Leadership::join(config.node_id, &config.peers, &state)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    println!(
        "joined a leadership cluster of {} as node {}",
        config.peers.len(),
        config.node_id
    );
    lead(&config, &leadership)
}
