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
use bx_gateway::auth::{self, Credentials, Mode as AuthMode};
use bx_gateway::config::Config;
use bx_gateway::expose::Exporter;
use bx_gateway::feed::Feed;
use bx_gateway::multicast::Multicast;
use bx_gateway::tcp::{Server, SnapshotPolicy};
use bx_journal::{FileLog, JournalError, LogStorage, MemoryLog, ReplicatedLog};
use bx_pipeline::instrument::{Instrument, Instruments};
use ed25519_dalek::SigningKey;
use std::path::Path;
use std::time::Duration;

/// Accounts credited at startup, so a client can trade without a funding API.
/// A real venue funds through deposits from its own account service.
/// Wide enough that the load client can give every one of its connections an
/// account to itself. Sharing one is not a saving: the private feed is
/// per-account, so two sessions on one account each receive the other's
/// acknowledgements and neither can tell which events answer its own orders.
const ACCOUNTS: std::ops::RangeInclusive<u64> = 1..=256;
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
        admin_account: None,
        chain: false,
        chain_interval: 0,
        chain_key_file: None,
        tls_listen: None,
        tls_cert_file: None,
        tls_key_file: None,
        metrics_listen: None,
        feed_listen: None,
        multicast: Vec::new(),
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

/// Loads the venue's chain signing key from the file that holds it.
///
/// A file rather than a configuration value, and read at startup rather than
/// embedded: a signing key in a config file is a signing key in the repository,
/// the image layer, and every backup of either. Nothing prints it -- the public
/// half is what an operator hands out, and it is derived here.
fn read_chain_key(path: &Path) -> std::io::Result<SigningKey> {
    let text = std::fs::read_to_string(path)?;
    let seed = auth::key_bytes_from_hex(&text).map_err(|why| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{}: {why}", path.display()),
        )
    })?;
    Ok(SigningKey::from_bytes(&seed))
}

/// The public half, for an operator to hand to clients.
fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
        out
    })
}

fn run<S: LogStorage>(
    config: &Config,
    storage: S,
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
    if let Some(admin) = config.admin_account {
        server.administrator(admin);
    }
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

    // Before recovering, because a chain has to cover the replay too: switched on
    // afterwards it would hold a head that accounts for nothing before it, which
    // the journal now refuses outright rather than publishing.
    if config.chain {
        server.venue_mut().exchange_mut().set_chaining(true);
        if config.chain_interval > 0 {
            server
                .venue_mut()
                .exchange_mut()
                .set_chain_interval(config.chain_interval);
        }
        match &config.chain_key_file {
            Some(path) => {
                let key = read_chain_key(path)?;
                let public = key.verifying_key().to_bytes();
                server.venue_mut().exchange_mut().set_chain_key(key);
                // The public half, so an operator can hand clients the value they
                // check against without going near the file.
                println!("chain on, signing as {}", hex(&public));
            }
            None => println!(
                "chain on, UNSIGNED: heads prove the venue agrees with itself, \
                 not that its history was never rewritten"
            ),
        }
    }

    if let (Some(address), Some(cert), Some(key)) = (
        &config.tls_listen,
        &config.tls_cert_file,
        &config.tls_key_file,
    ) {
        server.tls_listen(address, cert, key)?;
        println!("tls 1.3 listening on {address}; the raw listener stays for the cross-connect");
    }

    // Held for the life of the venue: dropping it stops the feed thread.
    // Bound, not used: it must outlive the trading loop, because dropping it
    // stops the thread that serves the feed.
    let mut published = None;
    let _feed = match &config.feed_listen {
        Some(address) => {
            let handoff = bx_gateway::handoff::Handoff::new();
            server.publish_to(handoff.clone());
            published = Some(handoff.clone());
            // The run identifier a receiver uses to tell "sequence 5 again"
            // from "sequence 5 still" across a promotion, since channel
            // numbering restarts when a venue does. Derived from the term and
            // the node, both of which a replaced leader cannot reuse.
            // Both halves masked to 32 bits rather than shifted and hoped over:
            // a node id past four billion would otherwise shift its own
            // identity out of the word and collide with another node's.
            let session = ((config.node_id & 0xFFFF_FFFF) << 32) | (config.term & 0xFFFF_FFFF);
            let groups = if config.multicast.is_empty() {
                None
            } else {
                let sender = Multicast::open(&config.multicast, session)?;
                println!("multicast to {}", config.multicast.join(", "));
                Some(sender)
            };
            let feed = Feed::start(
                address,
                handoff,
                config.retained_per_channel,
                config.retained_per_channel * std::mem::size_of::<bx_protocol::Event>(),
                groups,
            )?;
            println!("market data on {}, off the trading thread", feed.address());
            Some(feed)
        }
        None => None,
    };

    // Held for the life of the venue: dropping it stops the serving thread.
    let exporter = match &config.metrics_listen {
        Some(address) => {
            let exporter = Exporter::start(address)?;
            println!("metrics on http://{}", exporter.address());
            Some(exporter)
        }
        None => None,
    };

    // Recover first, always, and let the recovered state say whether this venue
    // has a history rather than asking whether a file happened to exist.
    //
    // Asking the file was wrong in the one case that matters most. A promoted
    // node opens a journal that did not exist a moment ago, then `catch_up`
    // fills it from a majority — so "the file was missing" was true and "this
    // venue is new" was false, and the node funded fresh accounts and served
    // with empty books while holding every record the dead leader had
    // acknowledged. It had recovered the data and thrown away the state.
    let replayed = server.recover(config.snapshot.as_deref())?;
    if server.venue().exchange().next_sequence() == 0 {
        // Nothing has ever happened here, by any route: no journal, no
        // snapshot, nothing caught up. Only then is this a new venue.
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
        println!(
            "recovered {replayed} commands, at sequence {}",
            server.venue().exchange().next_sequence()
        );
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
            // Published on the cadence the venue already reports on, so the
            // scrape endpoint costs no clock read and no work per request.
            if let Some(exporter) = &exporter {
                let mut text = server.metrics().prometheus();
                // Distribution has its own way of falling behind, and it is not
                // visible in the venue's counters at all.
                if let (Some(feed), Some(handoff)) = (&_feed, &published) {
                    text.push_str(&feed.prometheus(handoff));
                }
                exporter.publish(text);
            }
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
            let log = FileLog::open(path).map_err(other)?;
            if config.replicas.is_empty() {
                run(config, log, leadership)
            } else {
                run(config, promoted(config, log, term)?, leadership)
            }
        }
        None if config.replicas.is_empty() => run(config, MemoryLog::new(), leadership),
        None => run(
            config,
            promoted(config, MemoryLog::new(), term)?,
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
