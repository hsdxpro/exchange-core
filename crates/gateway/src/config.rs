//! Deployment configuration.
//!
//! Everything a deployment needs to change without a recompile: what to listen
//! on, where the journal and snapshot live, how long a restart may take, how
//! much feed to retain, and which instruments are listed.
//!
//! Parsed by hand rather than with `serde`. The format is `key = value` lines
//! and `[instrument]` blocks, which is a hundred lines of parser against a
//! dependency tree of a few hundred crates, and this project has exactly one
//! dependency. If the format ever needs to nest, that trade changes.
//!
//! Two rules the parser follows that matter more than the format:
//!
//! - **An unknown key is an error.** A misspelled key that is silently ignored
//!   means the venue runs with a default nobody chose, and the operator has no
//!   way to tell. That is a classic outage.
//! - **Every value is validated at startup.** A zero retention window or an
//!   empty instrument list should stop the venue before it accepts an order, not
//!   surface as strange behaviour under load.

use crate::auth::{self, Credentials, Mode};
use crate::limit::RateLimit;
use bx_pipeline::instrument::{Instrument, Instruments, MAX_OPEN_ORDERS_LIMIT, MAX_SYMBOL};
use bx_protocol::PUBLIC_KEY_LEN;
use bx_protocol::{AccountId, Ticks};
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Eq, PartialEq)]
pub struct ConfigError {
    /// Line the problem is on, 1-based. Zero for a problem with the file as a
    /// whole, such as a missing setting.
    pub line: usize,
    pub message: String,
}

impl ConfigError {
    fn at(line: usize, message: impl Into<String>) -> Self {
        Self {
            line,
            message: message.into(),
        }
    }

    fn whole_file(message: impl Into<String>) -> Self {
        Self::at(0, message)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.line == 0 {
            write!(f, "config: {}", self.message)
        } else {
            write!(f, "config line {}: {}", self.line, self.message)
        }
    }
}

impl std::error::Error for ConfigError {}

type Result<T> = std::result::Result<T, ConfigError>;

/// Which block a key belongs to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Section {
    Global,
    Instrument,
    Credential,
}

/// One account's public key, before validation.
#[derive(Debug, Default)]
struct CredentialDraft {
    line: usize,
    account: Option<u64>,
    secret: Option<[u8; PUBLIC_KEY_LEN]>,
}

impl CredentialDraft {
    fn finish(self) -> Result<(AccountId, [u8; PUBLIC_KEY_LEN])> {
        let line = self.line;
        let account = self
            .account
            .ok_or_else(|| ConfigError::at(line, "credential is missing account"))?;
        let secret = self
            .secret
            .ok_or_else(|| ConfigError::at(line, "credential is missing public_key"))?;
        Ok((account, secret))
    }
}

/// One listed instrument, before validation.
#[derive(Debug, Default)]
struct InstrumentDraft {
    line: usize,
    symbol: Option<u32>,
    base: Option<u32>,
    quote: Option<u32>,
    floor_ticks: Option<Ticks>,
    max_quantity: Option<u64>,
    max_open_orders: Option<u32>,
}

impl InstrumentDraft {
    fn finish(self) -> Result<Instrument> {
        let line = self.line;
        let need = |value: Option<u64>, name: &str| -> Result<u64> {
            value.ok_or_else(|| ConfigError::at(line, format!("instrument is missing {name}")))
        };
        let symbol = u32::try_from(need(self.symbol.map(u64::from), "symbol")?)
            .map_err(|_| ConfigError::at(line, "symbol is out of range"))?;
        let base = u32::try_from(need(self.base.map(u64::from), "base")?)
            .map_err(|_| ConfigError::at(line, "base is out of range"))?;
        let quote = u32::try_from(need(self.quote.map(u64::from), "quote")?)
            .map_err(|_| ConfigError::at(line, "quote is out of range"))?;
        let max_quantity = need(self.max_quantity, "max_quantity")?;
        let max_open_orders = u32::try_from(need(
            self.max_open_orders.map(u64::from),
            "max_open_orders",
        )?)
        .map_err(|_| ConfigError::at(line, "max_open_orders is out of range"))?;
        let floor_ticks = self
            .floor_ticks
            .ok_or_else(|| ConfigError::at(line, "instrument is missing floor_ticks"))?;

        if symbol >= MAX_SYMBOL {
            return Err(ConfigError::at(
                line,
                format!(
                    "symbol {symbol} is at or above the limit of {MAX_SYMBOL}; \
                     instruments are held in a table indexed by symbol, so number \
                     them densely from zero"
                ),
            ));
        }
        if base == quote {
            return Err(ConfigError::at(
                line,
                "base and quote are the same asset, so the instrument trades nothing",
            ));
        }
        if max_quantity == 0 {
            return Err(ConfigError::at(
                line,
                "max_quantity of zero rejects every order",
            ));
        }
        if max_open_orders == 0 {
            return Err(ConfigError::at(
                line,
                "max_open_orders of zero rejects every order",
            ));
        }
        if max_open_orders > MAX_OPEN_ORDERS_LIMIT {
            return Err(ConfigError::at(
                line,
                format!("max_open_orders exceeds the engine's limit of {MAX_OPEN_ORDERS_LIMIT}"),
            ));
        }
        Ok(Instrument::new(
            symbol,
            base,
            quote,
            floor_ticks,
            max_quantity,
            max_open_orders,
        ))
    }
}

/// A validated deployment configuration.
#[derive(Debug)]
pub struct Config {
    pub listen: String,
    /// None keeps the journal in memory, which is for measurement only.
    pub journal: Option<PathBuf>,
    pub snapshot: Option<PathBuf>,
    /// How long a restart may take. Turned into a snapshot cadence with the
    /// measured replay rate.
    pub target_recovery: Duration,
    /// Commands a second this machine replays at. Measured, not assumed.
    pub replay_rate: u64,
    pub retained_per_channel: usize,
    /// The account permitted to halt a symbol or stop another account.
    ///
    /// Absent means no account can, which is the safe default: a kill switch
    /// reachable because a line was forgotten reads exactly like one that was
    /// meant to be there.
    pub admin_account: Option<AccountId>,
    /// Whether to publish a verifiable chain over the sequenced stream.
    pub chain: bool,
    /// Records per seal. Zero means the journal's default.
    pub chain_interval: u64,
    /// File holding the venue's chain signing key, as 64 hex characters.
    ///
    /// A path, never the key itself. A secret in a configuration file is a secret
    /// in whatever holds that file -- a repository, an image layer, a backup --
    /// and the public half is the only part that belongs anywhere shareable.
    pub chain_key_file: Option<PathBuf>,
    /// Second listener, speaking TLS 1.3, for sessions that arrive over the
    /// internet. The raw listener stays for the colocated cross-connect.
    pub tls_listen: Option<String>,
    /// The venue's certificate chain, PEM. A path, like every key here.
    pub tls_cert_file: Option<PathBuf>,
    /// The certificate's private key, PEM. Never inline in configuration.
    pub tls_key_file: Option<PathBuf>,
    /// Where to serve counters for a monitoring system to scrape. Absent means
    /// the venue reports to its log and nowhere else.
    pub metrics_listen: Option<String>,
    /// Where public market data is served, on its own thread and port. Absent
    /// keeps the public feeds on the trading sessions, which is fine for a
    /// venue with few subscribers and measurably not fine with many.
    pub feed_listen: Option<String>,
    /// Multicast groups the public feed is sent to. Two is A and B: identical
    /// packets on independent paths, so a receiver takes whichever copy of a
    /// sequence arrives first. Empty sends none.
    pub multicast: Vec<String>,
    pub max_records_per_session: usize,
    /// Connections held at once. Beyond this the venue refuses rather than
    /// serving everyone slowly.
    pub max_sessions: usize,
    /// Followers to replicate to. Empty means a lone leader.
    pub replicas: Vec<String>,
    /// How long the leader waits for a follower to confirm.
    pub ack_timeout: Duration,
    /// This leader's term, when nobody is electing one. Must increase every time
    /// leadership moves: followers refuse an older one, which is what stops a
    /// replaced leader writing.
    ///
    /// Ignored once `peers` is set, because then the term comes from the
    /// election — which is the point of having one. A number a person types into
    /// a file is a promotion that waits for a person.
    pub term: u64,
    /// This node's identity in the leadership cluster.
    pub node_id: u64,
    /// Every node that may lead, this one included, as `id@address`.
    ///
    /// Empty means no election: the venue leads unconditionally on the `term`
    /// above, which is the single-node and measurement shape. Non-empty means a
    /// node serves only while it holds the leadership, and stops the moment it
    /// does not.
    pub peers: Vec<(u64, String)>,
    /// Where this node's leadership state lives. Required when `peers` is set,
    /// because a vote that does not survive a restart is a vote that can be cast
    /// twice.
    pub leadership_state: Option<PathBuf>,
    /// Ceiling on the memory the subscription feed may hold. Checked against
    /// what the retention window actually costs, so a venue refuses to start
    /// rather than being killed under load.
    pub max_feed_memory: u64,
    /// Whether sessions must prove who they are. Stated explicitly, never
    /// defaulted: a venue that is open because a key was forgotten looks exactly
    /// like one that is open on purpose.
    pub authentication: Mode,
    /// Account public keys, one per `[credential]` block. Required to be non-empty
    /// when authentication is required, or nobody could ever connect.
    pub credentials: Credentials,
    /// How fast one account may send. None means unlimited, and costs nothing.
    pub rate_limit: Option<RateLimit>,
    /// Whether to stamp arrival and match times. Two wall-clock readings a
    /// pass, shared by the whole group, so it vanishes under load and is worth
    /// about a quarter of a pass when the group is one.
    pub timestamps: bool,
    pub instruments: Instruments,
}

/// Bytes the subscription feed can hold at most: every public channel for every
/// listed instrument, at one retention window each.
///
/// Private channels are per connected account and are not counted here; they are
/// bounded by concurrent connections rather than by the instrument list.
#[must_use]
pub fn feed_memory(retained_per_channel: usize, symbols: usize) -> u64 {
    const PUBLIC_CHANNELS_PER_SYMBOL: u64 = 3; // book, trades and bbo
    const EVENT_BYTES: u64 = 64;
    retained_per_channel as u64 * symbols as u64 * PUBLIC_CHANNELS_PER_SYMBOL * EVENT_BYTES
}

/// Settings whose zero would be a venue that does nothing rather than a venue
/// with a small limit.
fn check_non_zero(values: &[(u64, &str)]) -> Result<()> {
    for (value, name) in values {
        if *value == 0 {
            return Err(ConfigError::whole_file(format!("{name} must not be zero")));
        }
    }
    Ok(())
}

/// Builds the instrument table, refusing a duplicate symbol and a retention
/// window the feed could not afford.
///
/// The window is per channel, so its cost multiplies by the instrument list. At
/// 65,536 events a channel and a thousand symbols that is 7.8 GiB, which is an
/// out-of-memory kill rather than a slow venue -- and it was the shipped
/// default. Refusing here is the difference between a startup error and a venue
/// that dies under load.
fn build_instruments(
    drafts: Vec<InstrumentDraft>,
    retained: u64,
    budget_mb: u64,
) -> Result<(Instruments, usize)> {
    let mut instruments = Instruments::new();
    let mut listed: Vec<u32> = Vec::new();
    for draft in drafts {
        let line = draft.line;
        let instrument = draft.finish()?;
        if listed.contains(&instrument.symbol) {
            return Err(ConfigError::at(
                line,
                format!("symbol {} is listed twice", instrument.symbol),
            ));
        }
        listed.push(instrument.symbol);
        instruments.insert(instrument);
    }

    let retained = usize::try_from(retained)
        .map_err(|_| ConfigError::whole_file("retained_per_channel is too large"))?;
    let needed = feed_memory(retained, listed.len());
    if needed > budget_mb * 1024 * 1024 {
        return Err(ConfigError::whole_file(format!(
            "the feed needs {} MiB for {} instruments at {retained} events a \
             channel, over the {budget_mb} MiB budget; lower \
             retained_per_channel or raise max_feed_memory_mb",
            needed / 1024 / 1024,
            listed.len()
        )));
    }
    Ok((instruments, retained))
}

/// Resolves the authentication mode and the keys that go with it.
///
/// Stated, never inferred. A venue open because a key was forgotten reads
/// exactly like one open on purpose, and the two could not be further apart.
fn resolve_authentication(
    mode: Option<Mode>,
    secrets: Vec<CredentialDraft>,
) -> Result<(Mode, Credentials)> {
    let mode = mode.ok_or_else(|| {
        ConfigError::whole_file(
            "authentication is not set: `required` to make every session prove \
             which account it is, or `open` for measurement runs only",
        )
    })?;
    let mut credentials = Credentials::new();
    for draft in secrets {
        let (account, secret) = draft.finish()?;
        credentials
            .insert(account, secret)
            .map_err(|why| ConfigError::at(0, why))?;
    }
    if mode == Mode::Required && credentials.is_empty() {
        return Err(ConfigError::whole_file(
            "authentication is required but no [credential] block is listed, so \
             no client could ever connect",
        ));
    }
    if mode == Mode::Open && !credentials.is_empty() {
        return Err(ConfigError::whole_file(
            "credentials are listed but authentication is `open`, so they would \
             never be checked",
        ));
    }
    Ok((mode, credentials))
}

/// An administrator is a privilege, and a privilege is only worth anything over
/// a proven identity.
///
/// On an open venue a session's account is whatever its first command claims, so
/// naming one here would let any client at all halt a symbol or stop an account
/// from trading -- the two commands that most need to be somebody's. The other
/// half: an administrator who cannot connect is a halt switch that does not
/// exist, and it fails at the moment it is needed rather than at startup.
fn check_admin(admin: Option<AccountId>, mode: Mode, credentials: &Credentials) -> Result<()> {
    let Some(admin) = admin else {
        return Ok(());
    };
    if mode == Mode::Open {
        return Err(ConfigError::whole_file(
            "admin_account is set but authentication is `open`, so any client \
             could claim that account and halt the venue",
        ));
    }
    if !credentials.knows(admin) {
        return Err(ConfigError::whole_file(format!(
            "admin_account is {admin} but no [credential] block lists that \
             account, so the administrator could never connect"
        )));
    }
    Ok(())
}

/// A chain that signs or seals nothing, which reads as verifiability that is
/// not there.
fn check_chain(on: Option<bool>, key_file: Option<&PathBuf>, interval: Option<u64>) -> Result<()> {
    if !on.unwrap_or(false) {
        if key_file.is_some() {
            return Err(ConfigError::whole_file(
                "chain_key_file is set but chain is off, so nothing would be signed",
            ));
        }
        if interval.is_some() {
            return Err(ConfigError::whole_file(
                "chain_interval is set but chain is off, so nothing would be sealed",
            ));
        }
    }
    if interval == Some(0) {
        return Err(ConfigError::whole_file(
            "chain_interval of zero would seal nothing; leave it unset for the default",
        ));
    }
    Ok(())
}

/// Half a TLS door is worse than none: a listener with no identity cannot
/// serve, and a certificate nothing listens with reads as encryption that is
/// not there.
fn check_tls(listen: bool, cert: bool, key: bool) -> Result<()> {
    let parts = [listen, cert, key];
    if parts.iter().any(|set| *set) && !parts.iter().all(|set| *set) {
        return Err(ConfigError::whole_file(
            "tls_listen, tls_cert_file and tls_key_file go together: all three, or none",
        ));
    }
    Ok(())
}

/// Groups nothing builds packets for, and more lines than redundancy needs.
fn check_multicast(groups: &[String], feed_listening: bool) -> Result<()> {
    if !groups.is_empty() && !feed_listening {
        return Err(ConfigError::whole_file(
            "multicast is listed but feed_listen is not: the packets are built \
             by the feed thread, so there is nothing to send them",
        ));
    }
    if groups.len() > 2 {
        return Err(ConfigError::whole_file(
            "multicast takes one group, or two for A and B; more than two is \
             bandwidth rather than redundancy",
        ));
    }
    Ok(())
}

/// A cluster that cannot say which node it is, or where its vote is kept, is one
/// that would either wait forever or forget what it voted.
fn check_cluster(
    peers: &[(u64, String)],
    node_id: Option<u64>,
    leadership_state: Option<&PathBuf>,
) -> Result<()> {
    if peers.is_empty() {
        return Ok(());
    }
    let me = node_id
        .ok_or_else(|| ConfigError::whole_file("peers are listed but node_id is not set"))?;
    if !peers.iter().any(|(id, _)| *id == me) {
        return Err(ConfigError::whole_file(format!(
            "node_id {me} is not among the peers, so this node could never be \
             elected and would wait forever"
        )));
    }
    if leadership_state.is_none() {
        return Err(ConfigError::whole_file(
            "peers are listed but leadership_state is not set; a vote that does \
             not survive a restart is a vote that can be cast twice, and two \
             leaders in one term is what an election exists to prevent",
        ));
    }
    let mut listed: Vec<u64> = peers.iter().map(|(id, _)| *id).collect();
    listed.sort_unstable();
    let before = listed.len();
    listed.dedup();
    if listed.len() != before {
        return Err(ConfigError::whole_file(
            "two peers share an identity, so a majority could be counted twice",
        ));
    }
    Ok(())
}

impl Config {
    /// # Errors
    /// Reports the first problem found, with the line it is on.
    ///
    /// Long, and deliberately so after the parts that could usefully leave did.
    /// What remains is one `match` from key name to field, plus the two section
    /// blocks that do the same for `[instrument]` and `[credential]`. That match
    /// *is* the configuration schema: splitting it across functions would scatter
    /// the list of what a venue can be told over several places, and the first
    /// question anyone brings to this file is "what keys are there". Every check
    /// that can be named and read on its own already is one -- see the functions
    /// above, each of which states an invariant and is testable without a file.
    #[allow(clippy::too_many_lines)]
    pub fn parse(text: &str) -> Result<Self> {
        let mut listen = None;
        let mut journal = None;
        let mut snapshot = None;
        let mut admin_account = None;
        let mut chain = None;
        let mut chain_interval = None;
        let mut chain_key_file = None;
        let mut tls_listen = None;
        let mut tls_cert_file = None;
        let mut tls_key_file = None;
        let mut metrics_listen = None;
        let mut feed_listen = None;
        let mut multicast: Vec<String> = Vec::new();
        let mut target_recovery_ms = None;
        let mut replay_rate = None;
        let mut retained = None;
        let mut max_records = None;
        let mut max_sessions = None;
        let mut ack_timeout_ms = None;
        let mut term = None;
        let mut max_feed_memory_mb = None;
        let mut authentication = None;
        let mut node_id = None;
        let mut leadership_state = None;
        let mut peers: Vec<(u64, String)> = Vec::new();
        let mut timestamps = None;
        let mut rate = None;
        let mut burst = None;
        let mut replicas = Vec::new();
        let mut drafts: Vec<InstrumentDraft> = Vec::new();
        let mut secrets: Vec<CredentialDraft> = Vec::new();
        // Which block a key belongs to. Tracked rather than inferred from "is
        // there an instrument draft yet", which would let a key in a later block
        // be claimed by an earlier one.
        let mut section = Section::Global;

        for (index, raw) in text.lines().enumerate() {
            let line = index + 1;
            let content = raw.split('#').next().unwrap_or("").trim();
            if content.is_empty() {
                continue;
            }
            if content == "[instrument]" {
                section = Section::Instrument;
                drafts.push(InstrumentDraft {
                    line,
                    ..InstrumentDraft::default()
                });
                continue;
            }
            if content == "[credential]" {
                section = Section::Credential;
                secrets.push(CredentialDraft {
                    line,
                    ..CredentialDraft::default()
                });
                continue;
            }

            let Some((key, value)) = content.split_once('=') else {
                return Err(ConfigError::at(line, "expected `key = value`"));
            };
            let key = key.trim();
            let value = value.trim();
            if value.is_empty() {
                return Err(ConfigError::at(line, format!("{key} has no value")));
            }

            let number = |name: &str| -> Result<u64> {
                value
                    .parse::<u64>()
                    .map_err(|_| ConfigError::at(line, format!("{name} must be a whole number")))
            };

            if section == Section::Credential
                && let Some(draft) = secrets.last_mut()
            {
                match key {
                    "account" => {
                        draft.account = Some(number("account")?);
                        continue;
                    }
                    // `public_key`, not `secret`: the venue holds no secret for
                    // an account any more. The old name is refused rather than
                    // accepted quietly, because a file that still says `secret`
                    // was written for a venue that could forge its clients'
                    // logons and the operator should be told.
                    "public_key" => {
                        draft.secret = Some(
                            auth::key_bytes_from_hex(value)
                                .map_err(|why| ConfigError::at(line, why))?,
                        );
                        continue;
                    }
                    "secret" => {
                        return Err(ConfigError::at(
                            line,
                            "credentials take `public_key` now, not `secret`: the venue \
                             verifies an Ed25519 signature and holds no secret \
                             of yours",
                        ));
                    }
                    _ => {}
                }
            }

            // Inside an instrument block, instrument keys win.
            if section == Section::Instrument
                && let Some(draft) = drafts.last_mut()
            {
                let claimed = match key {
                    "symbol" => {
                        draft.symbol = Some(number("symbol")? as u32);
                        true
                    }
                    "base" => {
                        draft.base = Some(number("base")? as u32);
                        true
                    }
                    "quote" => {
                        draft.quote = Some(number("quote")? as u32);
                        true
                    }
                    "floor_ticks" => {
                        draft.floor_ticks = Some(value.parse::<Ticks>().map_err(|_| {
                            ConfigError::at(line, "floor_ticks must be a whole number")
                        })?);
                        true
                    }
                    "max_quantity" => {
                        draft.max_quantity = Some(number("max_quantity")?);
                        true
                    }
                    "max_open_orders" => {
                        draft.max_open_orders = Some(number("max_open_orders")? as u32);
                        true
                    }
                    _ => false,
                };
                if claimed {
                    continue;
                }
            }

            match key {
                "listen" => listen = Some(value.to_string()),
                "chain" => {
                    chain = Some(match value {
                        "on" => true,
                        "off" => false,
                        other => {
                            return Err(ConfigError::at(
                                line,
                                format!("chain is `on` or `off`, not `{other}`"),
                            ));
                        }
                    });
                }
                "chain_interval" => chain_interval = Some(number("chain_interval")?),
                "chain_key_file" => chain_key_file = Some(PathBuf::from(value)),
                "tls_listen" => tls_listen = Some(value.to_string()),
                "tls_cert_file" => tls_cert_file = Some(PathBuf::from(value)),
                "tls_key_file" => tls_key_file = Some(PathBuf::from(value)),
                "metrics_listen" => metrics_listen = Some(value.to_string()),
                "feed_listen" => feed_listen = Some(value.to_string()),
                "multicast" => multicast.push(value.to_string()),
                "journal" => journal = Some(PathBuf::from(value)),
                "snapshot" => snapshot = Some(PathBuf::from(value)),
                "target_recovery_ms" => target_recovery_ms = Some(number("target_recovery_ms")?),
                "replay_rate" => replay_rate = Some(number("replay_rate")?),
                "retained_per_channel" => retained = Some(number("retained_per_channel")?),
                "admin_account" => admin_account = Some(number("admin_account")?),
                "max_records_per_session" => max_records = Some(number("max_records_per_session")?),
                "max_sessions" => max_sessions = Some(number("max_sessions")?),
                "ack_timeout_ms" => ack_timeout_ms = Some(number("ack_timeout_ms")?),
                "term" => term = Some(number("term")?),
                "max_feed_memory_mb" => max_feed_memory_mb = Some(number("max_feed_memory_mb")?),
                "node_id" => node_id = Some(number("node_id")?),
                "leadership_state" => leadership_state = Some(PathBuf::from(value)),
                "peer" => {
                    let Some((id, address)) = value.split_once('@') else {
                        return Err(ConfigError::at(
                            line,
                            "a peer is `id@address`, so a majority is counted over \
                             identities rather than over whoever happens to answer",
                        ));
                    };
                    let id = id.trim().parse::<u64>().map_err(|_| {
                        ConfigError::at(line, "a peer identity must be a whole number")
                    })?;
                    peers.push((id, address.trim().to_string()));
                }
                "replica" => replicas.push(value.to_string()),
                "authentication" => {
                    authentication = Some(match value {
                        "required" => Mode::Required,
                        "open" => Mode::Open,
                        other => {
                            return Err(ConfigError::at(
                                line,
                                format!("authentication is `required` or `open`, not `{other}`"),
                            ));
                        }
                    });
                }
                "timestamps" => {
                    timestamps = Some(match value {
                        "on" => true,
                        "off" => false,
                        other => {
                            return Err(ConfigError::at(
                                line,
                                format!("timestamps is `on` or `off`, not `{other}`"),
                            ));
                        }
                    });
                }
                "max_commands_per_second" => rate = Some(number("max_commands_per_second")?),
                "burst_commands" => burst = Some(number("burst_commands")?),
                // Not ignored. A key nobody reads means the venue is running
                // with a value the operator thinks they set.
                other => {
                    return Err(ConfigError::at(line, format!("unknown setting `{other}`")));
                }
            }
        }

        let required = |value: Option<u64>, name: &str| -> Result<u64> {
            value.ok_or_else(|| ConfigError::whole_file(format!("{name} is not set")))
        };
        let target_recovery_ms = required(target_recovery_ms, "target_recovery_ms")?;
        let replay_rate = required(replay_rate, "replay_rate")?;
        let retained = required(retained, "retained_per_channel")?;
        let max_records = required(max_records, "max_records_per_session")?;
        let max_sessions = required(max_sessions, "max_sessions")?;
        let ack_timeout_ms = required(ack_timeout_ms, "ack_timeout_ms")?;
        let max_feed_memory_mb = required(max_feed_memory_mb, "max_feed_memory_mb")?;

        if drafts.is_empty() {
            return Err(ConfigError::whole_file(
                "no instruments listed, so the venue would accept nothing",
            ));
        }
        check_non_zero(&[
            (target_recovery_ms, "target_recovery_ms"),
            (replay_rate, "replay_rate"),
            (retained, "retained_per_channel"),
            (max_records, "max_records_per_session"),
            (max_sessions, "max_sessions"),
            (ack_timeout_ms, "ack_timeout_ms"),
            (max_feed_memory_mb, "max_feed_memory_mb"),
        ])?;

        let (instruments, retained) = build_instruments(drafts, retained, max_feed_memory_mb)?;
        let (authentication, credentials) = resolve_authentication(authentication, secrets)?;
        check_admin(admin_account, authentication, &credentials)?;

        check_chain(chain, chain_key_file.as_ref(), chain_interval)?;
        check_tls(
            tls_listen.is_some(),
            tls_cert_file.is_some(),
            tls_key_file.is_some(),
        )?;
        check_multicast(&multicast, feed_listen.is_some())?;
        check_cluster(&peers, node_id, leadership_state.as_ref())?;

        // Both or neither: a rate without a burst refuses the opening quotes
        // every market maker sends, and a burst without a rate never refills.
        let rate_limit = match (rate, burst) {
            (Some(rate), Some(burst)) => {
                let bounded = |value: u64, name: &str| -> Result<u32> {
                    u32::try_from(value)
                        .ok()
                        .filter(|held| *held > 0)
                        .ok_or_else(|| {
                            ConfigError::whole_file(format!(
                                "{name} must be between 1 and {}",
                                u32::MAX
                            ))
                        })
                };
                Some(RateLimit::new(
                    bounded(rate, "max_commands_per_second")?,
                    bounded(burst, "burst_commands")?,
                ))
            }
            (None, None) => None,
            _ => {
                return Err(ConfigError::whole_file(
                    "max_commands_per_second and burst_commands must be set together",
                ));
            }
        };

        Ok(Self {
            listen: listen.unwrap_or_else(|| "127.0.0.1:7070".to_string()),
            authentication,
            credentials,
            rate_limit,
            // On unless a deployment says otherwise: a venue that owes anybody a
            // traceable timestamp should not have to remember to ask for one.
            timestamps: timestamps.unwrap_or(true),
            journal,
            snapshot,
            target_recovery: Duration::from_millis(target_recovery_ms),
            replay_rate,
            retained_per_channel: retained,
            admin_account,
            chain: chain.unwrap_or(false),
            chain_interval: chain_interval.unwrap_or(0),
            chain_key_file,
            tls_listen,
            tls_cert_file,
            tls_key_file,
            metrics_listen,
            feed_listen,
            multicast,
            max_records_per_session: usize::try_from(max_records)
                .map_err(|_| ConfigError::whole_file("max_records_per_session is too large"))?,
            max_sessions: usize::try_from(max_sessions)
                .map_err(|_| ConfigError::whole_file("max_sessions is too large"))?,
            replicas,
            ack_timeout: Duration::from_millis(ack_timeout_ms),
            // Defaults to one: a lone leader that never fails over still has a
            // term, and it is the same every restart.
            term: term.unwrap_or(1),
            node_id: node_id.unwrap_or(1),
            peers,
            leadership_state,
            max_feed_memory: max_feed_memory_mb * 1024 * 1024,
            instruments,
        })
    }

    /// # Errors
    /// Fails if the file cannot be read, or is not a valid configuration.
    pub fn read(path: &std::path::Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::whole_file(format!("cannot read {}: {e}", path.display())))?;
        Self::parse(&text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = "\
# The venue.
listen = 127.0.0.1:7070
journal = venue.log
snapshot = venue.snap
target_recovery_ms = 2000
replay_rate = 7600000
retained_per_channel = 65536
max_records_per_session = 4096
max_sessions = 4096
ack_timeout_ms = 250
max_feed_memory_mb = 64
authentication = required
max_commands_per_second = 50000
burst_commands = 1000
replica = 10.0.0.2:7100
replica = 10.0.0.3:7100

[credential]
account = 1
public_key = d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737

[instrument]
symbol = 1
base = 1
quote = 2
floor_ticks = 10000
max_quantity = 1000000
max_open_orders = 1000000
";

    #[test]
    fn a_valid_configuration_parses_completely() {
        let config = Config::parse(VALID).unwrap();
        assert_eq!(config.listen, "127.0.0.1:7070");
        assert_eq!(config.journal, Some(PathBuf::from("venue.log")));
        assert_eq!(config.snapshot, Some(PathBuf::from("venue.snap")));
        assert_eq!(config.target_recovery, Duration::from_secs(2));
        assert_eq!(config.replay_rate, 7_600_000);
        assert_eq!(config.retained_per_channel, 65_536);
        assert_eq!(config.max_records_per_session, 4_096);
        assert_eq!(config.max_sessions, 4_096);
        assert_eq!(config.ack_timeout, Duration::from_millis(250));
        assert_eq!(config.term, 1, "a term is not required of a lone leader");
        assert_eq!(config.max_feed_memory, 64 * 1024 * 1024);
        assert_eq!(config.replicas, vec!["10.0.0.2:7100", "10.0.0.3:7100"]);
        assert_eq!(config.authentication, Mode::Required);
        assert_eq!(config.credentials.len(), 1);
        let rate = config.rate_limit.expect("a rate limit was configured");
        assert_eq!(rate.per_second(), 50_000);
        assert_eq!(rate.burst(), 1_000);

        // The credential block did not swallow the instrument block that follows
        // it, which is the failure a shared "last draft wins" parser invites.
        let listed: Vec<u32> = config.instruments.iter().map(|i| i.symbol).collect();
        assert_eq!(listed, vec![1]);
        let instrument = config.instruments.get(1).unwrap();
        assert_eq!(instrument.floor_ticks, 10_000);
        assert_eq!(instrument.max_open_orders, 1_000_000);
    }

    /// Reports what a configuration was refused for, so a test can assert on the
    /// reason rather than merely that something went wrong.
    fn refusal(text: &str) -> String {
        Config::parse(text)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_else(|| panic!("the configuration was accepted"))
    }

    #[test]
    fn a_venue_cannot_be_open_by_omission() {
        // The whole point of the setting: leaving it out is an error, because a
        // venue that is open because a line was forgotten looks exactly like one
        // that is open on purpose.
        let text = VALID.replace("authentication = required\n", "");
        assert!(refusal(&text).contains("authentication is not set"));
    }

    #[test]
    fn requiring_authentication_without_credentials_is_refused() {
        // It would start, listen, challenge every client, and refuse all of
        // them. Better to fail at startup than to look healthy and serve nobody.
        let text = VALID.replace(
            "[credential]\naccount = 1\npublic_key = \
             d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737\n",
            "",
        );
        assert!(refusal(&text).contains("no [credential] block"));
    }

    #[test]
    fn credentials_that_would_never_be_checked_are_refused() {
        // Reads as though the venue is secured. It is not.
        let text = VALID.replace("authentication = required", "authentication = open");
        assert!(refusal(&text).contains("would never be checked"));
    }

    #[test]
    fn an_administrator_on_an_open_venue_is_refused() {
        // Nothing proves an account on an open venue, so a named administrator
        // is a halt switch handed to every client that connects.
        let text = VALID
            .replace("authentication = required", "authentication = open")
            .replace("\n[credential]\naccount = 1\npublic_key = d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737\n", "\n")
            + "admin_account = 1\n";
        assert!(refusal(&text).contains("could claim that account"));
    }

    #[test]
    fn an_administrator_with_no_key_of_its_own_is_refused() {
        // Configured, plausible, and unusable: the account named can never
        // connect, so the halt fails at the moment somebody reaches for it.
        let text = VALID.to_owned() + "admin_account = 99\n";
        let message = refusal(&text);
        assert!(message.contains("admin_account is 99"), "{message}");
        assert!(message.contains("never connect"), "{message}");
    }

    #[test]
    fn an_administrator_that_holds_a_key_is_accepted() {
        let text = VALID.to_owned() + "admin_account = 1\n";
        let settings = Config::parse(&text).expect("a keyed administrator is valid");
        assert_eq!(settings.admin_account, Some(1));
    }

    #[test]
    fn a_chain_key_with_no_chain_to_sign_is_refused() {
        let text = VALID.to_owned()
            + "chain_key_file = venue.key
";
        assert!(refusal(&text).contains("nothing would be signed"));
    }

    #[test]
    fn a_chain_interval_with_no_chain_is_refused() {
        let text = VALID.to_owned()
            + "chain_interval = 1024
";
        assert!(refusal(&text).contains("nothing would be sealed"));
    }

    #[test]
    fn a_chain_interval_of_zero_is_refused() {
        let text = VALID.to_owned()
            + "chain = on
chain_interval = 0
";
        assert!(refusal(&text).contains("seal nothing"));
    }

    #[test]
    fn a_chain_that_is_neither_on_nor_off_is_refused() {
        let text = VALID.to_owned()
            + "chain = yes
";
        assert!(refusal(&text).contains("`on` or `off`"));
    }

    #[test]
    fn a_signed_chain_parses() {
        let text = VALID.to_owned()
            + "chain = on
chain_interval = 4096
chain_key_file = venue.key
";
        let config = Config::parse(&text).expect("a signed chain is valid configuration");
        assert!(config.chain);
        assert_eq!(config.chain_interval, 4_096);
        assert_eq!(config.chain_key_file, Some(PathBuf::from("venue.key")));
    }

    #[test]
    fn a_chain_defaults_to_off() {
        let config = Config::parse(VALID).unwrap();
        assert!(!config.chain, "verifiability turned itself on");
        assert_eq!(config.chain_interval, 0);
        assert!(config.chain_key_file.is_none());
    }

    #[test]
    fn half_a_tls_door_is_refused() {
        for partial in [
            "tls_listen = 127.0.0.1:7443
",
            "tls_cert_file = venue.crt
",
            "tls_listen = 127.0.0.1:7443
tls_cert_file = venue.crt
",
            "tls_key_file = venue.tls.key
",
        ] {
            let text = VALID.to_owned() + partial;
            assert!(
                refusal(&text).contains("all"),
                "accepted a partial TLS configuration: {partial}"
            );
        }
    }

    #[test]
    fn a_whole_tls_door_parses() {
        let text = VALID.to_owned()
            + "tls_listen = 127.0.0.1:7443
tls_cert_file = venue.crt
tls_key_file = venue.tls.key
";
        let config = Config::parse(&text).expect("a complete TLS door is valid");
        assert_eq!(config.tls_listen.as_deref(), Some("127.0.0.1:7443"));
        assert!(config.tls_cert_file.is_some() && config.tls_key_file.is_some());
    }

    #[test]
    fn a_malformed_public_key_names_its_line() {
        let text = VALID.replace(
            "public_key = d04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737",
            "public_key = abc",
        );
        let message = refusal(&text);
        assert!(message.contains("64 hex characters"), "{message}");
        assert!(message.contains("line"), "{message}");
    }

    #[test]
    fn a_rate_without_a_burst_is_refused() {
        // A rate with no burst refuses the opening quotes every market maker
        // sends; a burst with no rate never refills. Half a limiter is worse
        // than none because it looks configured.
        let text = VALID.replace("burst_commands = 1000\n", "");
        assert!(refusal(&text).contains("must be set together"));
        let text = VALID.replace("max_commands_per_second = 50000\n", "");
        assert!(refusal(&text).contains("must be set together"));
    }

    #[test]
    fn no_rate_limit_at_all_is_a_valid_choice() {
        let text = VALID
            .replace("max_commands_per_second = 50000\n", "")
            .replace("burst_commands = 1000\n", "");
        assert!(Config::parse(&text).unwrap().rate_limit.is_none());
    }

    #[test]
    fn an_unspellable_authentication_mode_is_refused() {
        let text = VALID.replace("authentication = required", "authentication = yes");
        assert!(refusal(&text).contains("`required` or `open`"));
    }

    #[test]
    fn comments_and_blank_lines_are_ignored() {
        let text = format!("{VALID}\n# trailing comment\n\n   \n");
        assert!(Config::parse(&text).is_ok());
    }

    #[test]
    fn a_misspelled_setting_is_refused_rather_than_ignored() {
        // Silently ignoring this means the venue runs with a retention window
        // the operator believes they changed.
        let text = VALID.replace("retained_per_channel", "retained_per_chanel");
        let error = Config::parse(&text).unwrap_err();
        assert!(error.message.contains("unknown setting"), "got {error}");
        assert!(error.line > 0, "the error should name the line");
    }

    #[test]
    fn a_missing_setting_names_itself() {
        let text = VALID
            .lines()
            .filter(|l| !l.starts_with("ack_timeout_ms"))
            .collect::<Vec<_>>()
            .join("\n");
        let error = Config::parse(&text).unwrap_err();
        assert_eq!(error.message, "ack_timeout_ms is not set");
    }

    #[test]
    fn a_value_that_is_not_a_number_is_refused() {
        let text = VALID.replace("replay_rate = 7600000", "replay_rate = fast");
        let error = Config::parse(&text).unwrap_err();
        assert!(error.message.contains("whole number"), "got {error}");
    }

    #[test]
    fn zero_is_refused_where_it_would_break_the_venue() {
        for setting in [
            "target_recovery_ms",
            "replay_rate",
            "retained_per_channel",
            "max_records_per_session",
            "max_sessions",
            "ack_timeout_ms",
            "max_feed_memory_mb",
        ] {
            let text = VALID
                .lines()
                .map(|line| {
                    if line.starts_with(setting) {
                        format!("{setting} = 0")
                    } else {
                        line.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            let error = Config::parse(&text).unwrap_err();
            assert!(
                error.message.contains("must not be zero"),
                "{setting}: got {error}"
            );
        }
    }

    #[test]
    fn a_feed_that_would_not_fit_in_memory_is_refused_at_startup() {
        // 65,536 events a channel over 1,000 symbols, on three public channels
        // each, is 11.7 GiB. The venue must say so rather than be killed under
        // load.
        assert_eq!(feed_memory(65_536, 1_000) / 1024 / 1024, 12_000);

        let text = VALID.replace("max_feed_memory_mb = 64", "max_feed_memory_mb = 1");
        let error = Config::parse(&text).unwrap_err();
        assert!(
            error.message.contains("over the 1 MiB budget"),
            "got {error}"
        );
        assert!(
            error.message.contains("lower retained_per_channel"),
            "got {error}"
        );
    }

    #[test]
    fn the_budget_counts_every_listed_instrument() {
        // One instrument at 65,536 events across three public channels is 12
        // MiB, so a 13 MiB budget holds one and not two.
        let one = VALID.replace("max_feed_memory_mb = 64", "max_feed_memory_mb = 13");
        assert!(Config::parse(&one).is_ok());

        let two = format!(
            "{one}
[instrument]
symbol = 2
base = 1
quote = 3
floor_ticks = 100
max_quantity = 10
max_open_orders = 10
"
        );
        let error = Config::parse(&two).unwrap_err();
        assert!(error.message.contains("2 instruments"), "got {error}");
    }

    #[test]
    fn a_symbol_beyond_the_dense_tables_limit_is_refused() {
        let text = VALID.replace("symbol = 1", "symbol = 4294967295");
        let error = Config::parse(&text).unwrap_err();
        assert!(
            error.message.contains("table indexed by symbol"),
            "got {error}"
        );
    }

    #[test]
    fn a_venue_with_no_instruments_is_refused() {
        let text: String = VALID
            .lines()
            .take_while(|l| !l.starts_with("[instrument]"))
            .collect::<Vec<_>>()
            .join("\n");
        let error = Config::parse(&text).unwrap_err();
        assert!(error.message.contains("no instruments"), "got {error}");
    }

    #[test]
    fn the_same_symbol_listed_twice_is_refused() {
        let text = format!(
            "{VALID}
[instrument]
symbol = 1
base = 3
quote = 4
floor_ticks = 500
max_quantity = 10
max_open_orders = 10
"
        );
        let error = Config::parse(&text).unwrap_err();
        assert!(error.message.contains("listed twice"), "got {error}");
    }

    #[test]
    fn an_instrument_that_trades_an_asset_against_itself_is_refused() {
        let text = VALID.replace("quote = 2", "quote = 1");
        let error = Config::parse(&text).unwrap_err();
        assert!(error.message.contains("same asset"), "got {error}");
    }

    #[test]
    fn an_incomplete_instrument_names_what_is_missing() {
        let text = VALID
            .lines()
            .filter(|l| !l.starts_with("floor_ticks"))
            .collect::<Vec<_>>()
            .join("\n");
        let error = Config::parse(&text).unwrap_err();
        assert!(error.message.contains("floor_ticks"), "got {error}");
    }

    #[test]
    fn an_order_pool_beyond_the_engines_limit_is_refused() {
        let text = VALID.replace("max_open_orders = 1000000", "max_open_orders = 4294967295");
        let error = Config::parse(&text).unwrap_err();
        assert!(error.message.contains("engine's limit"), "got {error}");
    }

    #[test]
    fn a_negative_floor_price_is_allowed_because_prices_are_signed() {
        let text = VALID.replace("floor_ticks = 10000", "floor_ticks = -5000");
        let config = Config::parse(&text).unwrap();
        assert_eq!(config.instruments.get(1).unwrap().floor_ticks, -5_000);
    }

    #[test]
    fn a_line_that_is_not_a_setting_is_refused() {
        let text = format!("{VALID}\nthis is not a setting\n");
        let error = Config::parse(&text).unwrap_err();
        assert!(error.message.contains("key = value"), "got {error}");
    }

    #[test]
    fn an_in_memory_journal_is_expressed_by_leaving_it_out() {
        let text = VALID
            .lines()
            .filter(|l| !l.starts_with("journal") && !l.starts_with("snapshot"))
            .collect::<Vec<_>>()
            .join("\n");
        let config = Config::parse(&text).unwrap();
        assert!(config.journal.is_none());
        assert!(config.snapshot.is_none());
    }
}
