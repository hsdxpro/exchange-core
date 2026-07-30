//! A server over real sockets.
//!
//! One thread, no locks. Matching a symbol is inherently single-writer, so a
//! thread per connection would only add contention around a section that cannot
//! run in parallel anyway. The loop takes whatever arrived, applies it as one
//! group, commits once, and writes each session whatever it has not yet seen.
//!
//! Sessions are found by readiness rather than by scanning. Reading every socket
//! every pass cost a syscall per idle connection -- measured at 428 ns, linear --
//! and an active client paid for all of them because the scan landed in the same
//! pass. A thousand idle connections put 428 microseconds in front of every
//! order. `mio` gives epoll, IOCP or kqueue depending on the platform, so an idle
//! connection now costs nothing until it speaks.
//!
//! That shape is what makes group commit real rather than something a test
//! calls by hand: the group is however many commands happened to arrive since
//! the last pass, so it grows under load — exactly when the sync needs
//! amortising — and falls to one when the venue is idle and latency matters
//! more.
//!
//! A session receives the private feed for the account it trades as -- declared
//! by being the account on its first command -- and whatever public channels it
//! asks for. It asks with `Subscribe`, which is session control: the gateway
//! handles it and it never reaches the exchange or the journal, because a
//! subscription belongs to a connection and connections do not survive a
//! restart.
//!
//! One socket carries a session's acknowledgements and its market data, so a
//! client that reads its feed slowly does back up its own fills. QUIC was built
//! to avoid that -- a stream per channel, each with its own flow control -- and
//! then removed, because measured against the same venue it cost 38.6 us of round
//! trip against this transport's 8.6 us, and 1.48M orders a second against 3.76M.
//! The bounded outbox and shedding a session that exceeds it are what handle the
//! consequence instead, and a shed client rebuilds from a snapshot on reconnect.
//!
//! Public feeds are opt-in rather than given to everyone. At one instrument the
//! difference is invisible; at a thousand, sending every session every book
//! would multiply the venue's outbound traffic by the number of instruments
//! nobody asked about.
//!
//! A client following a book is sent the current levels before any increments.
//! Increments alone are not enough to build a book -- a subscriber has no idea
//! what was resting before it arrived -- so the venue states the levels as
//! `BookSnapshot` at the sequence the feed resumes from, and `BookDelta` follows.
//! The same thing happens if a client falls outside the retention window, which
//! is what makes falling behind recoverable instead of fatal.

use crate::auth::{self, Credentials, Mode};
use crate::codec::{Decoder, FRAME_LEN, encode};
use crate::limit::{Bucket, RateLimit};
use crate::metrics::Metrics;
use crate::venue::Venue;
use bx_journal::LogStorage;
use bx_pipeline::fastmap::FastMap;
use bx_pipeline::hub::{Channel, Resume};
use bx_pipeline::instrument::Instruments;
use bx_pipeline::snapshot::Snapshot;
use bx_protocol::{
    AccountId, CHALLENGE_LEN, Command, CommandKind, Event, EventKind, PROOF_LEN, RejectReason,
    SIGNATURE_LEN, Sequence, Side, SymbolId, Ticks,
};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use std::fs::File;
use std::io::{self, ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// One connected client.
#[derive(Debug)]
struct Session {
    stream: TcpStream,
    /// Present when this session arrived on the TLS listener. All reads and
    /// writes go through it; the raw cross-connect path never allocates one.
    tls: Option<Box<rustls::ServerConnection>>,
    decoder: Decoder,
    /// Bytes framed but not yet accepted by the socket.
    ///
    /// Bounded, because a client that connects and never reads would otherwise
    /// make the venue allocate without limit. The decoder was already capped;
    /// this was the remaining direction in which one session could exhaust the
    /// whole process.
    outbox: Vec<u8>,
    max_outbox: usize,
    /// Channels this session follows, and where it has read to.
    cursors: Vec<(Channel, Sequence)>,
    /// Who this session may act for. Proved against a challenge when the venue
    /// requires authentication, and taken from the first command when it does
    /// not.
    account: Option<AccountId>,
    /// The nonce this session must sign, while it still owes an answer.
    ///
    /// `Some` means nothing but `Authenticate` is accepted. Cleared once the
    /// proof checks out, so the ordinary path costs one `Option` test.
    challenge: Option<[u8; CHALLENGE_LEN]>,
    /// Cancel this account's resting orders when the connection goes.
    ///
    /// Per session, and scoped to the account: two sessions on one account should
    /// agree about it, because the venue cannot tell which of them placed a given
    /// order without an index it does not keep.
    cancel_on_disconnect: bool,
    /// Already in the set of sessions this pass will read.
    ///
    /// A flag rather than a search. Readiness can name a session the previous
    /// pass already left pending, so the two sources have to be merged, and
    /// checking membership by scanning made that merge quadratic in ready
    /// sessions: a thousand events against a thousand carried-over sessions is a
    /// million comparisons, about a millisecond, on the pass that is already the
    /// busiest.
    queued: bool,
    /// Already in the set of sessions this pass will write to.
    ///
    /// The same trick as `queued`, and for the same reason: a session's outbox
    /// is filled from several places in a pass and every one of them must be
    /// able to say "this one owes bytes" without searching a list.
    owes: bool,
    open: bool,
    /// First 32 bytes of a signature and the account that sent them, waiting for
    /// the continuation record.
    ///
    /// A signature is 64 bytes and a record carries 32, so answering a challenge
    /// takes a pair. Present only between the two halves of one attempt.
    ///
    /// The account is kept because both halves must name the same one. Without
    /// that, a client could pair the first half of an attempt for one account
    /// with the second half of an attempt for another and have the venue verify
    /// the assembled bytes against whichever key the last record happened to
    /// name.
    half_signature: Option<([u8; PROOF_LEN], AccountId)>,
}

/// One account's send allowance, shared by every session trading as it.
#[derive(Clone, Copy, Debug)]
struct Allowance {
    bucket: Bucket,
    /// Live sessions on this account. The entry goes when this reaches zero.
    sessions: u32,
}

/// The listener's token. Sessions take `index + 1`, so zero is never a session.
const LISTENER: Token = Token(0);

/// The TLS listener's token, when one is configured. Sessions start above both.
const TLS_LISTENER: Token = Token(1);

/// Committed groups between watermarks.
///
/// The interval bounds two things at once: how many private outcomes a recovery
/// may have to redeliver, and how much of the journal a promotion replays into
/// the redelivery buffer. At 64 the overhead is one 64-byte record per 64
/// groups -- invisible -- and the buffer stays a few thousand events at worst.
const WATERMARK_EVERY: usize = 64;

/// Nanoseconds since the Unix epoch.
///
/// A wall clock rather than a monotonic one, deliberately: this number leaves
/// the venue and has to mean something to a client comparing it with its own
/// records, which `Instant` cannot do. It is read in the gateway only.
fn wall_clock_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos() as u64)
}

/// Tells one client its command was refused before the venue ever saw it.
///
/// No sequence, because it never entered the stream: this is the gateway
/// speaking, not the exchange. A refusal is always sent rather than the command
/// silently dropped — a client that is told nothing retries forever.
fn refused_locally(command: &Command, reason: RejectReason) -> Event {
    Event {
        sequence: 0,
        cause_sequence: 0,
        account: command.account,
        order_id: command.order_id,
        counterparty_order_id: 0,
        quantity: command.quantity,
        price: command.price,
        symbol: command.symbol,
        kind: EventKind::Rejected as u8,
        side: command.side,
        reject_reason: reason as u8,
        _pad: [0; 1],
    }
}

fn session_token(index: usize) -> Token {
    Token(index + 2)
}

fn session_index(token: Token) -> Option<usize> {
    (token.0 >= 2).then(|| token.0 - 2)
}

impl Session {
    fn new(
        stream: TcpStream,
        tls: Option<Box<rustls::ServerConnection>>,
        max_records: usize,
        max_outbox: usize,
        challenge: Option<[u8; CHALLENGE_LEN]>,
    ) -> Self {
        Self {
            stream,
            tls,
            decoder: Decoder::new(max_records),
            outbox: Vec::new(),
            max_outbox,
            cursors: Vec::new(),
            account: None,
            challenge,
            cancel_on_disconnect: false,
            queued: false,
            owes: false,
            open: true,
            half_signature: None,
        }
    }

    /// True while the session still owes proof of who it is.
    const fn unproven(&self) -> bool {
        self.challenge.is_some()
    }

    /// Abandons a half-delivered signature.
    ///
    /// Anything other than the matching continuation record immediately after
    /// the first half ends the attempt. A signature assembled from two records
    /// that arrived either side of other traffic is one nobody can reason about,
    /// and accepting it would let a client interleave attempts and have the venue
    /// try whichever pairing happens to verify.
    const fn forget_half_signature(&mut self) {
        self.half_signature = None;
    }

    fn stop_following(&mut self, channel: Channel) {
        self.cursors.retain(|(held, _)| *held != channel);
    }

    /// Moves a followed channel's cursor, for a session being resynchronised.
    fn reposition(&mut self, channel: Channel, to: Sequence) {
        for (held, cursor) in &mut self.cursors {
            if *held == channel {
                *cursor = to;
            }
        }
    }

    /// Reads one buffer's worth, appending decoded commands to `out`.
    ///
    /// Returns true if the socket may still hold more.
    ///
    /// Deliberately **one** read per pass rather than draining the socket. A
    /// pass that read until the socket blocked would let one client hand the
    /// venue an unbounded group: a client pushing a hundred thousand orders in
    /// one write produced more events in a single pass than the subscription
    /// rings retain, so the ring wrapped before anyone was sent anything and
    /// every session was then dropped for lagging -- including clients that had
    /// never been given a chance to read. Bounding the pass bounds the events
    /// one group can produce, and it shares the venue fairly between sessions
    /// instead of serving whoever writes hardest.
    ///
    /// Readiness is edge-triggered: a socket is reported once when it becomes
    /// readable, and not again until it has been read to exhaustion. Stopping
    /// after one buffer therefore cannot rely on another event -- so the session
    /// stays in the ready set until a read genuinely returns `WouldBlock`. The
    /// cost is one extra read after a session's last data, which is far cheaper
    /// than scanning every connection, and the alternative is a session that
    /// goes silent forever because nothing will wake it again.
    fn read_into(&mut self, out: &mut Vec<Command>, undecodable: &mut u64) -> bool {
        if self.decoder.writable().is_empty() {
            return true;
        }
        if self.tls.is_some() {
            return self.read_through_tls(out, undecodable);
        }
        match self.stream.read(self.decoder.writable()) {
            // A read of zero on a stream socket means the peer closed.
            Ok(0) => {
                self.open = false;
                false
            }
            Ok(bytes) => {
                self.decoder.advance(bytes);
                self.decoder.drain(out, undecodable);
                // Not "bytes == room". A short read does not prove the socket is
                // empty, and only `WouldBlock` re-arms the notification.
                true
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::Interrupted => {
                false
            }
            Err(_) => {
                self.open = false;
                false
            }
        }
    }

    /// The TLS half of [`Self::read_into`]: ciphertext off the socket, through
    /// the record layer, plaintext into the same decoder the raw path fills.
    ///
    /// Also what drives the handshake. The client speaks first, so every
    /// handshake flight arrives here; whatever rustls wants to send back is
    /// pushed by [`Self::flush`], which the caller owes after any read that
    /// leaves `wants_write` true.
    fn read_through_tls(&mut self, out: &mut Vec<Command>, undecodable: &mut u64) -> bool {
        let tls = self.tls.as_mut().expect("checked by the caller");
        let keep = match tls.read_tls(&mut self.stream) {
            Ok(0) => {
                self.open = false;
                false
            }
            Ok(_) => true,
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::Interrupted => {
                false
            }
            Err(_) => {
                self.open = false;
                false
            }
        };
        let Ok(state) = tls.process_new_packets() else {
            // Garbage on a TLS port -- plaintext, a scanner, a broken client.
            // There is no protocol left to answer in.
            self.open = false;
            return false;
        };
        if state.peer_has_closed() {
            self.open = false;
        }
        let mut pending = state.plaintext_bytes_to_read();
        while pending > 0 && !self.decoder.writable().is_empty() {
            let room = self.decoder.writable().len().min(pending);
            let Ok(bytes) = tls.reader().read(&mut self.decoder.writable()[..room]) else {
                break;
            };
            if bytes == 0 {
                break;
            }
            self.decoder.advance(bytes);
            self.decoder.drain(out, undecodable);
            pending -= bytes;
        }
        // Leftover plaintext stays buffered in rustls; reporting "keep reading"
        // brings the pass back for it, the same contract the raw path has with
        // a short read.
        keep || pending > 0
    }

    /// True once this session owes more bytes than it is allowed to queue.
    ///
    /// Such a client is already beyond saving: the backlog exceeds a full
    /// retention window, so even if it started reading now the feed it needs
    /// would have been overwritten. Dropping it is the same policy as lagging
    /// out of the window, applied one step earlier and for the same reason --
    /// the venue neither slows down for a slow client nor accumulates for one.
    ///
    /// This measures what the venue is holding, not what the kernel is holding
    /// on the session's behalf. Pinning the send buffer to bound that too was
    /// tried and reverted: it also caps how far a *healthy* reader may fall
    /// behind, and a subscriber with a few milliseconds of jitter was being
    /// disconnected. Tolerance belongs in this budget, which an operator sets
    /// and can see, plus whatever the kernel is willing to hold.
    fn over_budget(&self) -> bool {
        self.outbox.len() > self.max_outbox
    }

    /// Pushes whatever the socket will take. Anything it will not take stays
    /// queued rather than blocking the venue on one slow client.
    fn flush(&mut self) {
        if self.tls.is_some() {
            self.flush_through_tls();
            return;
        }
        let mut written = 0;
        while written < self.outbox.len() {
            match self.stream.write(&self.outbox[written..]) {
                Ok(0) => break,
                Ok(bytes) => written += bytes,
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => {
                    self.open = false;
                    break;
                }
            }
        }
        self.outbox.drain(..written);
    }

    /// The TLS half of [`Self::flush`]: outbox plaintext into the record layer,
    /// ciphertext onto the socket.
    ///
    /// rustls buffers a bounded 64 KiB of its own (checked, not assumed: a
    /// memory bound that lives in somebody else's default is not a bound). Past
    /// that its writer accepts nothing, so a slow client pushes back into the
    /// outbox and the operator's budget -- the thing they set and can see --
    /// stays the measure of how far behind a session is, plus that fixed slack.
    /// Runs even with an empty outbox, because handshake and alert bytes want
    /// the wire without any application data owing.
    fn flush_through_tls(&mut self) {
        let tls = self.tls.as_mut().expect("checked by the caller");
        loop {
            let mut fed = 0;
            while fed < self.outbox.len() {
                match tls.writer().write(&self.outbox[fed..]) {
                    Ok(0) => break,
                    Ok(bytes) => fed += bytes,
                    Err(_) => {
                        self.open = false;
                        break;
                    }
                }
            }
            self.outbox.drain(..fed);
            let mut sent = false;
            while tls.wants_write() {
                match tls.write_tls(&mut self.stream) {
                    Ok(0) => {
                        self.open = false;
                        break;
                    }
                    Ok(_) => sent = true,
                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == ErrorKind::Interrupted => {}
                    Err(_) => {
                        self.open = false;
                        break;
                    }
                }
            }
            if !self.open || self.outbox.is_empty() || (fed == 0 && !sent) {
                break;
            }
        }
    }
}

/// When to take the next snapshot.
///
/// Expressed as a recovery-time target rather than a cadence, because the
/// cadence is not the thing anyone cares about: what matters is how long the
/// venue is down after a crash, and that is set by how many commands have to be
/// replayed. Replay was measured at roughly 6.5 million commands per second, so
/// a target of a few seconds turns directly into a number of commands.
///
/// Snapshotting is not required for correctness. The journal remains the source
/// of truth, and a venue that never snapshots recovers identically, only slower.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotPolicy {
    /// Commands allowed to accumulate before the next snapshot.
    every: u64,
    /// Sequence the next snapshot is due at.
    due_at: Sequence,
}

impl SnapshotPolicy {
    /// `replay_rate` is commands per second the venue replays at, and
    /// `target_recovery` how long a restart may take. Measure the rate rather
    /// than assuming it: it depends on the traffic mix.
    ///
    /// # Panics
    /// If either argument is zero, which would ask for a snapshot per command
    /// or one never.
    #[must_use]
    pub fn from_recovery_target(replay_rate: u64, target_recovery: Duration) -> Self {
        assert!(replay_rate > 0, "replay rate must be positive");
        let every = (replay_rate as f64 * target_recovery.as_secs_f64()) as u64;
        assert!(
            every > 0,
            "the recovery target is shorter than a single command's replay"
        );
        Self {
            every,
            due_at: every,
        }
    }

    /// Commands between snapshots.
    #[must_use]
    pub const fn interval(&self) -> u64 {
        self.every
    }

    fn is_due(&self, sequence: Sequence) -> bool {
        sequence >= self.due_at
    }

    fn taken_at(&mut self, sequence: Sequence) {
        self.due_at = sequence.saturating_add(self.every);
    }
}

/// The venue, listening.
#[derive(Debug)]
pub struct Server<S: LogStorage> {
    poll: Poll,
    events: Events,
    listener: TcpListener,
    /// Second front door, for sessions that need encryption. `None` on a venue
    /// serving only the cross-connect.
    tls_listener: Option<TcpListener>,
    tls_config: Option<std::sync::Arc<rustls::ServerConfig>>,
    venue: Venue<S>,
    /// Indexed by token, so a session's identity is stable while its neighbours
    /// come and go. A scanning loop could use a plain `Vec` and compact it;
    /// readiness hands back a token and needs the index to still mean something.
    sessions: Vec<Option<Session>>,
    free: Vec<usize>,
    live: usize,
    /// Sessions whose sockets may still hold data the last pass did not take.
    /// Edge-triggered readiness will not mention them again, so the loop
    /// remembers them itself.
    still_readable: Vec<usize>,
    /// Committed groups since the venue last journalled a watermark.
    groups_since_watermark: usize,
    /// Sessions that closed this pass, collected as they are noticed rather than
    /// found by sweeping every slot afterwards.
    closing: Vec<usize>,
    /// Unsent bytes one session may owe. Derived from the retention window
    /// rather than chosen: a session further behind than the window cannot be
    /// caught up whatever the venue does.
    max_outbox: usize,
    /// Set to snapshot periodically. None means never, which is slower to
    /// recover and no less correct.
    snapshots: Option<(SnapshotPolicy, PathBuf)>,
    refused: u64,
    /// Whether a session must prove who it is, and the secrets it proves
    /// against. `Open` is for measurement; it is stated in the configuration
    /// rather than defaulted, so a venue is never open by accident.
    mode: Mode,
    credentials: Credentials,
    /// Sessions dropped for failing to prove who they are, and commands
    /// discarded for exceeding an allowance. Both are the shape of an attack as
    /// much as a bug, so they are counted rather than merely acted on.
    rejected_proofs: u64,
    /// Keys dropped by an administrator. Worth seeing: a rising count means
    /// somebody is responding to a compromise.
    keys_revoked: u64,
    throttled: u64,
    /// How fast one session may send. `None` disables the check entirely, so a
    /// venue that does not want it pays nothing for it.
    rate: Option<RateLimit>,
    /// The one account allowed to halt a symbol or stop another account.
    ///
    /// `None` means nobody can, which is the default: a venue whose kill switch
    /// is reachable because a configuration line was forgotten is worse than one
    /// that has none, because it reads the same as a venue that meant to have it.
    admin: Option<AccountId>,
    /// One allowance per account rather than per connection, so opening ten
    /// sessions does not buy ten times the rate. Held here rather than on the
    /// session for that reason, and dropped when an account's last session goes,
    /// so the table tracks connected accounts rather than every account ever
    /// seen.
    allowances: FastMap<AccountId, Allowance>,
    /// Which sessions follow each channel.
    ///
    /// The write pass used to ask every session about every channel it followed,
    /// every pass, whether or not anything had happened — which is why an idle
    /// connection cost 23 ns a pass and four thousand of them put ninety
    /// microseconds in front of every order. Now the hub says which channels
    /// received something and this says who was waiting on them, so a connection
    /// that has nothing to be told costs nothing at all.
    subscribers: FastMap<Channel, Vec<usize>>,
    /// Sessions whose outbox has bytes in it, so the write pass visits those and
    /// no others. Reused across passes.
    owing: Vec<usize>,
    /// The channels the last group touched, copied out so the venue is not
    /// borrowed while sessions are written to.
    touched: Vec<Channel>,
    /// What the venue is doing, counted as it does it. Timings are sampled every
    /// sixty-fourth pass so the clock reading does not land on the order path.
    metrics: Metrics,
    /// Whether to stamp arrival and match times onto commands and events.
    ///
    /// Off costs nothing and is what the benchmarks run with. On costs two
    /// wall-clock readings a pass — about 50 ns, shared by every command in the
    /// group, so it disappears under load and is worth roughly a quarter of a
    /// pass when the group is one. A venue that owes anybody a traceable
    /// timestamp pays it; one being measured does not.
    timestamps: bool,
    /// Reused across passes so a steady-state loop allocates nothing.
    inbound: Vec<Command>,
    max_records_per_session: usize,
    /// Connections this venue will hold at once.
    ///
    /// An idle connection costs 23 ns a pass -- measured -- now that sessions are
    /// found by readiness instead of by scanning. It was 422 ns when every socket
    /// was read every pass. What remains is userspace: a cursor check and a
    /// liveness check per session, no syscalls.
    ///
    /// It is still linear, and still paid by every *active* client because it
    /// lands in the same pass, so there is still a ceiling: 4,096 connections is
    /// about 94 microseconds of scanning in front of an order. Past the ceiling
    /// the venue refuses rather than accepting everyone and serving all of them
    /// slowly, and counts the refusals so an operator knows to add a gateway.
    max_sessions: usize,
}

impl<S: LogStorage> Server<S> {
    /// Binds and prepares the venue. Pass port 0 to let the OS choose, then ask
    /// [`Self::address`].
    ///
    /// # Errors
    /// Fails if the address cannot be bound or the journal cannot be opened.
    pub fn bind(
        address: &str,
        storage: S,
        instruments: Instruments,
        retained_per_channel: usize,
        max_records_per_session: usize,
        max_sessions: usize,
    ) -> io::Result<Self> {
        let parsed: SocketAddr = address
            .parse()
            .map_err(|_| io::Error::other(format!("`{address}` is not an address")))?;
        let mut listener = TcpListener::bind(parsed)?;
        let poll = Poll::new()?;
        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)?;
        let venue = Venue::new(storage, instruments, retained_per_channel)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let server = Self {
            poll,
            // One pass hands back at most this many ready sockets; the rest wait
            // for the next, which is fair and bounds the work a pass can take.
            events: Events::with_capacity(1_024),
            listener,
            tls_listener: None,
            tls_config: None,
            venue,
            sessions: Vec::new(),
            free: Vec::new(),
            live: 0,
            still_readable: Vec::new(),
            groups_since_watermark: 0,
            closing: Vec::new(),
            inbound: Vec::new(),
            max_records_per_session,
            max_sessions,
            max_outbox: retained_per_channel * FRAME_LEN,
            snapshots: None,
            refused: 0,
            mode: Mode::Open,
            admin: None,
            credentials: Credentials::new(),
            rejected_proofs: 0,
            keys_revoked: 0,
            throttled: 0,
            rate: None,
            allowances: FastMap::default(),
            subscribers: FastMap::default(),
            owing: Vec::new(),
            touched: Vec::new(),
            metrics: Metrics::default(),
            timestamps: false,
        };
        Ok(server)
    }

    /// Requires every session to prove which account it acts for.
    ///
    /// Until this is called the venue is open: a session states its account and
    /// is believed. That is a measurement setting, and the configuration has to
    /// name it explicitly so it is never reached by omission.
    pub fn require_authentication(&mut self, credentials: Credentials) {
        self.mode = Mode::Required;
        self.credentials = credentials;
    }

    /// Bounds how fast one session may send. Unset means unlimited.
    pub const fn rate_limit(&mut self, limit: RateLimit) {
        self.rate = Some(limit);
    }

    /// Names the account permitted to halt symbols and stop other accounts.
    ///
    /// Checked in the gateway, before sequencing, so a command from anybody else
    /// never reaches the journal -- the same placement as authentication and rate
    /// limiting, and for the same reason.
    pub const fn administrator(&mut self, account: AccountId) {
        self.admin = Some(account);
    }

    /// Stamps arrival and match times onto commands and acknowledgements.
    pub const fn stamp_times(&mut self, on: bool) {
        self.timestamps = on;
    }

    /// Sessions dropped for failing to prove who they are.
    #[must_use]
    pub const fn rejected_proofs(&self) -> u64 {
        self.rejected_proofs
    }

    /// Commands discarded for exceeding an account's allowance.
    #[must_use]
    pub const fn throttled(&self) -> u64 {
        self.throttled
    }

    /// # Errors
    /// Fails if the socket has no local address.
    pub fn address(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    #[must_use]
    pub const fn venue(&self) -> &Venue<S> {
        &self.venue
    }

    pub fn venue_mut(&mut self) -> &mut Venue<S> {
        &mut self.venue
    }

    #[must_use]
    pub const fn sessions(&self) -> usize {
        self.live
    }

    /// Writes a snapshot to `path` whenever `policy` says one is due.
    ///
    /// Written to a temporary file and renamed, so a crash midway leaves the
    /// previous snapshot intact rather than a half-written one. A snapshot that
    /// cannot be trusted is worse than none, because recovery would start from
    /// it.
    pub fn snapshot_to(&mut self, policy: SnapshotPolicy, path: PathBuf) {
        self.snapshots = Some((policy, path));
    }

    /// Takes a snapshot now, if one is due. Returns the sequence it covers.
    ///
    /// # Errors
    /// Fails if the snapshot cannot be written. The venue keeps trading: the
    /// journal is still authoritative, so a failed snapshot costs recovery time
    /// and nothing else.
    pub fn snapshot_if_due(&mut self) -> io::Result<Option<Sequence>> {
        let Some((policy, path)) = self.snapshots.as_mut() else {
            return Ok(None);
        };
        let sequence = self.venue.exchange().next_sequence();
        if !policy.is_due(sequence) {
            return Ok(None);
        }

        let snapshot = self.venue.snapshot();
        let staging = path.with_extension("writing");
        {
            let mut file = File::create(&staging)?;
            snapshot
                .write_to(&mut file)
                .map_err(|e| io::Error::other(e.to_string()))?;
            // Durable before the rename, or the rename could publish a file the
            // filesystem has not finished writing.
            file.sync_all()?;
        }
        std::fs::rename(&staging, &path)?;

        policy.taken_at(sequence);
        Ok(Some(sequence))
    }

    /// Loads a snapshot and replays the journal after it, or replays everything
    /// if there is no snapshot to load.
    ///
    /// # Errors
    /// Fails if the journal is unreadable. A snapshot that will not parse is
    /// reported, not skipped: silently falling back to a full replay would hide
    /// a corrupt snapshot for as long as the venue kept running.
    pub fn recover(&mut self, snapshot_path: Option<&Path>) -> io::Result<u64> {
        let loaded = match snapshot_path {
            Some(path) if path.exists() => Some(
                Snapshot::read_from(&mut File::open(path)?)
                    .map_err(|e| io::Error::other(e.to_string()))?,
            ),
            _ => None,
        };
        let replayed = match loaded {
            Some(snapshot) => self.venue.recover_from(&snapshot),
            None => self.venue.recover(),
        };
        replayed.map_err(|e| io::Error::other(e.to_string()))
    }

    /// Opens the TLS 1.3 listener beside the raw one.
    ///
    /// Sessions arriving here are identical to raw ones past the record layer:
    /// same framing, same logon, same budgets. See the `tls` module for why the
    /// venue offers both doors.
    ///
    /// # Errors
    /// Fails if the address cannot be bound or the certificate and key do not
    /// load.
    pub fn tls_listen(
        &mut self,
        address: &str,
        cert_file: &std::path::Path,
        key_file: &std::path::Path,
    ) -> io::Result<()> {
        let config = crate::tls::server_config(cert_file, key_file)?;
        let parsed = address
            .parse()
            .map_err(|e| io::Error::new(ErrorKind::InvalidInput, format!("{address}: {e}")))?;
        let mut listener = TcpListener::bind(parsed)?;
        self.poll
            .registry()
            .register(&mut listener, TLS_LISTENER, Interest::READABLE)?;
        self.tls_listener = Some(listener);
        self.tls_config = Some(config);
        Ok(())
    }

    /// Where the TLS listener ended up, when one is open.
    ///
    /// # Errors
    /// Returns the underlying I/O error if the address cannot be read.
    pub fn tls_address(&self) -> io::Result<Option<std::net::SocketAddr>> {
        self.tls_listener
            .as_ref()
            .map(|listener| listener.local_addr())
            .transpose()
    }

    /// One pass: accept, read, apply as a group, commit, write.
    ///
    /// Returns how many commands were applied.
    ///
    /// # Errors
    /// Fails only if the journal cannot be written or flushed.
    pub fn poll(&mut self) -> bx_journal::Result<usize> {
        // Sampled, so the clock is read on one pass in sixty-four rather than in
        // front of every order.
        let timing = self.metrics.sampling().then(Instant::now);

        // Which sessions to look at: the ones readiness named, plus the ones the
        // previous pass left with data still in the socket.
        let mut ready = std::mem::take(&mut self.still_readable);
        let mut accept = false;
        // A zero timeout keeps this a poll rather than a wait, so a caller that
        // busy-polls a pinned core still can. A deployment that would rather
        // yield the core can afford a small timeout at the cost of that much
        // latency.
        if self
            .poll
            .poll(&mut self.events, Some(Duration::ZERO))
            .is_ok()
        {
            for event in self.events.iter() {
                match session_index(event.token()) {
                    None => accept = true,
                    Some(index) => {
                        // Sessions carried over from the previous pass are
                        // already flagged, so this both dedupes and skips them.
                        if let Some(session) = self.sessions.get_mut(index).and_then(Option::as_mut)
                            && !session.queued
                        {
                            session.queued = true;
                            ready.push(index);
                        }
                    }
                }
            }
        }
        if accept {
            self.accept_pending();
        }

        self.inbound.clear();
        // The watermark goes in *first*, before this pass reads a socket, so it
        // sequences ahead of every command the pass is about to take.
        //
        // The ordering is the whole claim. Appended at the end of the group
        // instead -- which is how this was first written -- it sequences after
        // commands committed in the same pass, whose outcome events are written
        // to sockets later in that same pass. A venue dying in between would
        // leave a marker asserting those outcomes had been handed to the feed,
        // and recovery would drop exactly the events the client never got. In
        // front, it can only ever claim what earlier passes produced, and an
        // earlier pass flushed before this one began.
        //
        // The counter only advances on a committed group, so this fires only
        // after real traffic. On an idle venue it costs one 64-byte record once
        // and then nothing, because the reset holds until groups commit again.
        if self.groups_since_watermark >= WATERMARK_EVERY {
            self.groups_since_watermark = 0;
            self.inbound.push(Command::watermark());
        }
        // One clock reading for the whole pass, not one per command. A client
        // that sent a thousand orders in a single write refills its bucket
        // against this one `Instant::now()`; the per-command cost is a compare
        // and a decrement. Skipped entirely when nothing is rate limited.
        let now = self.rate.map(|_| Instant::now());
        // Likewise for arrival: every command in this group came off its socket
        // in this pass, so the resolution is the pass. Per-packet accuracy has
        // to come from the NIC, not from here.
        let ingress_ns = self.timestamps.then(wall_clock_ns).unwrap_or_default();
        for index in ready {
            let start = self.inbound.len();
            let Some(session) = self.sessions.get_mut(index).and_then(Option::as_mut) else {
                continue;
            };
            let mut undecodable = 0_u64;
            let again = session.read_into(&mut self.inbound, &mut undecodable);
            if again {
                // Stays flagged: it is going straight back into the ready set.
                self.still_readable.push(index);
            } else {
                session.queued = false;
            }
            if undecodable > 0 {
                self.metrics.undecodable(undecodable);
            }
            // A TLS session may owe the wire handshake or alert bytes that no
            // outbox entry accounts for; the flush list is how bytes reach a
            // socket, so it goes on the list.
            let owes_tls = session.tls.as_ref().is_some_and(|tls| tls.wants_write());
            if !session.open {
                self.closing.push(index);
                continue;
            }
            if owes_tls && !session.owes {
                session.owes = true;
                self.owing.push(index);
            }

            // Nothing from a session that has not proved who it is reaches the
            // venue -- not an order, not even a subscription.
            if session.unproven() {
                self.admit(index, start);
                let ready = self.sessions[index]
                    .as_ref()
                    .is_some_and(|session| session.open && !session.unproven());
                if !ready {
                    continue;
                }
            } else if session.account.is_none()
                && let Some(command) = self.inbound.get(start)
                // A command the venue itself owns cannot claim who a session
                // is: a wire watermark carries account zero, and letting it
                // attach would bind the session to nobody and refuse every
                // real command after it. It is answered below, unattached.
                && command.kind() != Some(CommandKind::Watermark)
            {
                // Attribute each session's account from its *own* first command.
                // Reading everyone into one buffer first and then handing
                // accounts out would give a session whoever happened to be at
                // the front. Reachable only when the venue runs open: an
                // authenticated session already knows who it is.
                let account = command.account;
                session.account = Some(account);
                // A session always gets its own private feed; it has to ask for
                // anything public.
                let channel = Channel::Account(account);
                let from = self.venue.subscribe(channel);
                self.follow(index, channel, from);
                self.claim_allowance(account, now);
                self.deliver_recovered_outcomes(index, account);
            }

            // Discarded here, before sequencing, so a flood never reaches the
            // journal and replay never has to ask what time it was.
            if let (Some(limit), Some(now)) = (self.rate, now) {
                self.throttle(index, start, limit, now);
            }

            // Control messages belong to the connection, not the venue. Taking
            // them out here is what keeps them out of the journal: a
            // subscription replayed after a restart would resurrect a feed for a
            // connection that no longer exists.
            // Compacted in place, for the same reason as the throttle above:
            // removing one at a time shifts the rest, so a client that sent
            // nothing but subscriptions paid for the square of them.
            let account = self.session_at(index).account.unwrap_or_default();
            let end = self.inbound.len();
            let mut kept = start;
            for cursor in start..end {
                let command = self.inbound[cursor];
                if command.kind() == Some(CommandKind::Watermark) {
                    // The venue's own marker. A client that could inject one
                    // would move the redelivery boundary and silence outcomes
                    // another account is owed.
                    let refusal = refused_locally(&command, RejectReason::NotPermitted);
                    let session = self.session_at(index);
                    encode(&refusal, &mut session.outbox);
                    self.owes_bytes(index);
                } else if command.kind() == Some(CommandKind::RevokeKey) {
                    self.revoke_key(index, &command, account);
                } else if command.is_session_control() {
                    self.apply_control(index, &command, account);
                } else if command.account != account && !command.is_administrative() {
                    // A session acts for exactly one account: the one it proved,
                    // or in an open venue the one its first command claimed.
                    //
                    // Without this, authentication established identity at
                    // connect and then bound nothing to it -- a session holding
                    // one valid credential could put any account in an order and
                    // spend that account's balance. Refused here, before
                    // sequencing, so it never reaches the journal.
                    //
                    // Administrative commands are the exception by design: a halt
                    // names the symbol and a kill switch names its subject, and
                    // the check that they came from the administrator is below.
                    let refusal = refused_locally(&command, RejectReason::NotPermitted);
                    let session = self.session_at(index);
                    encode(&refusal, &mut session.outbox);
                    self.owes_bytes(index);
                } else if command.is_administrative() && self.admin != Some(account) {
                    // Refused here rather than in the pipeline, so an
                    // unauthorised halt is never sequenced and never replayed.
                    let refusal = refused_locally(&command, RejectReason::NotPermitted);
                    let session = self.session_at(index);
                    encode(&refusal, &mut session.outbox);
                    self.owes_bytes(index);
                } else {
                    self.inbound[kept] = command;
                    kept += 1;
                }
            }
            self.inbound.truncate(kept);
        }

        let applied = self.inbound.len();
        if applied > 0 {
            if self.timestamps {
                for command in &mut self.inbound {
                    command.ingress_ns = ingress_ns;
                }
                self.venue.exchange_mut().matching_now(wall_clock_ns());
            }
            let started = timing.map(|_| Instant::now());
            let mut group = std::mem::take(&mut self.inbound);
            let result = self.venue.accept(&mut group);
            self.inbound = group;
            if let Some(started) = started {
                self.metrics.commit_took(started.elapsed());
            }
            result?;
            self.groups_since_watermark += 1;
        }

        self.push_updates(applied > 0);
        self.drop_closed();
        self.metrics.pass(applied);
        if let Some(started) = timing {
            self.metrics.pass_took(started.elapsed());
        }
        Ok(applied)
    }

    /// What the venue has been doing. Counts are exact; timings are sampled.
    #[must_use]
    pub const fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// The session a pass is currently working on.
    ///
    /// Every caller reaches this while handling commands that session just
    /// sent, so the slot is occupied for a reason stronger than the token being
    /// fresh: a session marked closed keeps its slot until [`Self::drop_closed`]
    /// runs, and that happens once, at the end of the pass, after all of this.
    /// Nothing else empties a slot. So a closed session is still `Some` here,
    /// and a slot freed on one pass cannot be reused until the next.
    ///
    /// That is the invariant this panic guards. If a future change frees a slot
    /// mid-pass, this is where it surfaces -- immediately, rather than as a
    /// session quietly handling somebody else's orders.
    fn session_at(&mut self, index: usize) -> &mut Session {
        self.sessions[index]
            .as_mut()
            .expect("a slot is only emptied by drop_closed, which runs after this")
    }

    /// Starts a session following a channel, in both the session and the index.
    ///
    /// The two must agree exactly. A cursor without an index entry is a client
    /// that silently receives nothing; an index entry without a cursor is a
    /// session visited for a channel it does not follow. Both go through here so
    /// there is one place to get it right.
    fn follow(&mut self, index: usize, channel: Channel, from: Sequence) {
        let session = self.session_at(index);
        if session.cursors.iter().any(|(held, _)| *held == channel) {
            return;
        }
        session.cursors.push((channel, from));
        self.subscribers.entry(channel).or_default().push(index);
    }

    /// Stops a session following a channel, in both places.
    fn stop_following(&mut self, index: usize, channel: Channel) {
        self.session_at(index).stop_following(channel);
        if let Some(following) = self.subscribers.get_mut(&channel) {
            following.retain(|held| *held != index);
            if following.is_empty() {
                self.subscribers.remove(&channel);
            }
        }
    }

    /// Takes a session out of every channel's subscriber list, on the way out.
    fn forget_subscriptions(&mut self, index: usize, cursors: &[(Channel, Sequence)]) {
        for (channel, _) in cursors {
            if let Some(following) = self.subscribers.get_mut(channel) {
                following.retain(|held| *held != index);
                if following.is_empty() {
                    self.subscribers.remove(channel);
                }
            }
        }
    }

    /// Lets a session act only once it has proved which account it is.
    ///
    /// Works in place over the session's own slice of the group, so a client may
    /// pipeline its opening orders directly behind the proof — which a market
    /// maker will, having already spent one round trip collecting the challenge.
    /// Everything before the proof is refused *with a reason*: a client told
    /// nothing retries forever.
    ///
    /// A wrong proof closes the connection rather than allowing another attempt.
    /// Retrying costs a reconnect and a fresh nonce, which makes guessing a tag
    /// pointless without slowing an honest client that has the secret.
    fn admit(&mut self, index: usize, start: usize) {
        // Compacted in place. Taking commands off the front one at a time shifts
        // everything behind them, and this runs *before* a client has proved
        // anything -- so a stranger could make the venue do the square of
        // whatever they sent, on the one path that exists to keep strangers out.
        let end = self.inbound.len();
        let mut kept = start;
        for cursor in start..end {
            let command = self.inbound[cursor];

            // Re-read each time: an earlier command in this same buffer may have
            // proved the session, and everything after that is ordinary traffic
            // for the rest of the pass to handle.
            let Some(challenge) = self.sessions[index].as_ref().and_then(|s| s.challenge) else {
                self.inbound[kept] = command;
                kept += 1;
                continue;
            };

            let held = self.session_at(index).half_signature;
            let first_half = match (command.kind(), held) {
                // First half. Nothing is verified yet: a signature is only a
                // signature once both halves are in hand. Overwrites any half
                // already held, which is what starting a fresh attempt means.
                (Some(CommandKind::Authenticate), _) => {
                    let account = command.account;
                    let proof = command.proof();
                    self.session_at(index).half_signature = Some((proof, account));
                    continue;
                }
                // Second half, in the right place and for the same account.
                (Some(CommandKind::AuthenticateContinued), Some((first, account)))
                    if account == command.account =>
                {
                    first
                }
                // Anything else while unproven: a continuation with no first
                // half, a continuation naming a different account, or ordinary
                // traffic before proving. All the same answer -- refused with a
                // reason, because a client told nothing retries forever -- and
                // any half held is abandoned.
                _ => {
                    let session = self.session_at(index);
                    session.forget_half_signature();
                    encode(
                        &refused_locally(&command, RejectReason::NotAuthenticated),
                        &mut session.outbox,
                    );
                    self.owes_bytes(index);
                    continue;
                }
            };

            // Both halves, in order, from one session, for one account.
            let mut signature = [0_u8; SIGNATURE_LEN];
            signature[..PROOF_LEN].copy_from_slice(&first_half);
            signature[PROOF_LEN..].copy_from_slice(&command.proof());
            self.session_at(index).forget_half_signature();

            if !self
                .credentials
                .verifies(command.account, &challenge, &signature)
            {
                self.rejected_proofs += 1;
                let session = self.session_at(index);
                encode(
                    &refused_locally(&command, RejectReason::NotAuthenticated),
                    &mut session.outbox,
                );
                // The write pass runs before the session is dropped, so the
                // client is told why rather than seeing a bare disconnect.
                session.open = false;
                self.owes_bytes(index);
                self.closing.push(index);
                // Nothing this session sent survives, proved or not.
                self.inbound.truncate(start);
                return;
            }

            let account = command.account;
            let session = self.session_at(index);
            session.challenge = None;
            session.account = Some(account);
            encode(
                &Event {
                    account,
                    kind: EventKind::Authenticated as u8,
                    ..Event::default()
                },
                &mut session.outbox,
            );
            self.owes_bytes(index);
            let channel = Channel::Account(account);
            let from = self.venue.subscribe(channel);
            self.follow(index, channel, from);
            self.claim_allowance(account, Some(Instant::now()));
            self.deliver_recovered_outcomes(index, account);
        }
        self.inbound.truncate(kept);
    }

    /// Drops an account's key and closes every session trading as it.
    ///
    /// Admin only, and handled here rather than in the pipeline: keys are not
    /// deterministic venue state, and a replay that reapplied a revocation would
    /// need the key material in the journal.
    ///
    /// Closing the sessions is the point. Dropping the key stops the *next*
    /// logon, and a stolen key whose session is already open could otherwise go
    /// on cancelling orders -- cancels stay permitted under every other
    /// restriction, deliberately, so revocation has to reach the connection
    /// itself.
    ///
    /// This is the immediate half. The lasting half is removing the key from the
    /// configuration, because that is where it comes back from on a restart.
    fn revoke_key(&mut self, index: usize, command: &Command, sender: AccountId) {
        if self.admin != Some(sender) {
            let refusal = refused_locally(command, RejectReason::NotPermitted);
            let session = self.session_at(index);
            encode(&refusal, &mut session.outbox);
            self.owes_bytes(index);
            return;
        }
        let revoked = command.account;
        self.credentials.revoke(revoked);
        self.keys_revoked += 1;
        for held in 0..self.sessions.len() {
            let closing = self.sessions[held]
                .as_ref()
                .is_some_and(|session| session.open && session.account == Some(revoked));
            if closing {
                if let Some(session) = self.sessions[held].as_mut() {
                    session.open = false;
                }
                self.closing.push(held);
            }
        }
        let acknowledgement = Event {
            account: revoked,
            kind: EventKind::Received as u8,
            ..Event::default()
        };
        let session = self.session_at(index);
        encode(&acknowledgement, &mut session.outbox);
        self.owes_bytes(index);
    }

    /// Attaches this session to its account's allowance, creating it if this is
    /// the account's first connection.
    fn claim_allowance(&mut self, account: AccountId, now: Option<Instant>) {
        let (Some(limit), Some(now)) = (self.rate, now) else {
            return;
        };
        self.allowances
            .entry(account)
            .and_modify(|held| held.sessions += 1)
            .or_insert_with(|| Allowance {
                bucket: Bucket::new(limit, now),
                sessions: 1,
            });
    }

    /// Drops whatever this session sent beyond its account's allowance.
    ///
    /// The bucket is refilled once here rather than once per command, which is
    /// the whole reason the clock is read at the top of the pass.
    fn throttle(&mut self, index: usize, start: usize, limit: RateLimit, now: Instant) {
        let Some(account) = self.sessions[index].as_ref().and_then(|s| s.account) else {
            return;
        };
        let Some(allowance) = self.allowances.get_mut(&account) else {
            return;
        };
        allowance.bucket.refill(limit, now);

        // Compacted in place rather than removed one at a time. `Vec::remove`
        // shifts everything behind it, so discarding was quadratic in the size
        // of the batch — and the batch is largest exactly when a client is
        // flooding, which is when this runs.
        //
        // The harm is a latency spike, not a throughput collapse: a pass is
        // bounded, so the venue keeps up while every client on it stalls for the
        // length of one bad pass. Measured at roughly 1.7 ms against a pass
        // otherwise counted in hundreds of nanoseconds — four orders of
        // magnitude, on the path whose job is to make floods cheap.
        //
        // No test distinguishes the two, and that is stated rather than papered
        // over: the batch is capped by what one socket read returns, so the
        // damage tops out near a millisecond, which a black-box timing test
        // cannot separate from noise on a loaded machine. A test that passes
        // either way would claim coverage it does not have.
        let end = self.inbound.len();
        let mut kept = start;
        let mut discarded = 0_u64;
        for cursor in start..end {
            let command = self.inbound[cursor];
            if allowance.bucket.take() {
                self.inbound[kept] = command;
                kept += 1;
            } else {
                discarded += 1;
                if let Some(session) = self.sessions[index].as_mut() {
                    encode(
                        &refused_locally(&command, RejectReason::RateLimited),
                        &mut session.outbox,
                    );
                }
            }
        }
        self.inbound.truncate(kept);
        self.owes_bytes(index);
        self.throttled += discarded;
    }

    /// Deregisters and forgets whatever closed this pass.
    ///
    /// Works from the list built while sessions were being visited, so a pass
    /// with no disconnections does no work here at all.
    fn drop_closed(&mut self) {
        if self.closing.is_empty() {
            return;
        }
        let mut withdraw = Vec::new();
        for index in std::mem::take(&mut self.closing) {
            if let Some(mut session) = self.sessions[index].take() {
                let _ = self.poll.registry().deregister(&mut session.stream);
                // Out of every channel's subscriber list. Leaving an index entry
                // behind would have the next session to take this slot written
                // to for a channel it never asked for.
                let cursors = std::mem::take(&mut session.cursors);
                self.forget_subscriptions(index, &cursors);
                if let Some(account) = session.account {
                    if session.cancel_on_disconnect {
                        withdraw.push(account);
                    }
                    // The allowance outlives any one session but not the
                    // account's last, or the table would grow with every client
                    // that ever connected.
                    if let std::collections::hash_map::Entry::Occupied(mut held) =
                        self.allowances.entry(account)
                    {
                        held.get_mut().sessions -= 1;
                        if held.get().sessions == 0 {
                            held.remove();
                        }
                    }
                }
                self.free.push(index);
                self.live -= 1;
            }
        }
        // Applied as ordinary commands after the session is gone, so they are
        // journalled, published, and visible to everyone still watching.
        for account in withdraw {
            let mut cancels = self.venue.cancels_for(account);
            if !cancels.is_empty() {
                let _ = self.venue.accept(&mut cancels);
            }
        }
        self.still_readable
            .retain(|index| self.sessions[*index].is_some());
    }

    /// Handles one session-control message: a feed starting or stopping, or a
    /// question about the session's own orders.
    fn apply_control(&mut self, index: usize, command: &Command, account: AccountId) {
        if command.kind() == Some(CommandKind::QueryOpenOrders) {
            self.answer_open_orders(index, command.symbol, account);
            return;
        }
        if command.kind() == Some(CommandKind::CancelOnDisconnect) {
            self.session_at(index).cancel_on_disconnect = command.quantity != 0;
            return;
        }
        let Some(kind) = command.channel_kind() else {
            return;
        };
        let channel = Channel::requested(kind, command.symbol, account);
        if command.kind() == Some(CommandKind::Resume) {
            self.resume_from(index, channel, command.order_id, command.symbol);
            return;
        }
        if command.kind() == Some(CommandKind::Subscribe) {
            let from = self.venue.subscribe(channel);
            self.follow(index, channel, from);
            // State before change, on both public feeds that carry state. A
            // subscriber cannot derive where the book is from a stream that only
            // says where it moved.
            match channel {
                Channel::Book(symbol) => self.send_book_state(index, symbol, from),
                Channel::Bbo(symbol) => self.send_top_state(index, symbol, from),
                // Neither carries state a subscriber has to be told first: a
                // print is an event, and a chain head is only meaningful from the
                // next one onward -- a client cannot verify a head over records it
                // never saw, so there is nothing useful to restate.
                Channel::Trades(_) | Channel::Account(_) | Channel::Checkpoint => {}
            }
        } else {
            self.stop_following(index, channel);
        }
    }

    /// Follows a channel from where the client left it, rather than from now.
    ///
    /// Answers one of three things and never silence, which is the whole point of
    /// the message: the events it missed, or a fresh restatement if it cannot have
    /// them. A client told nothing has no way to tell a gap from a quiet market.
    ///
    /// A resume implies a subscription, so a client reconnecting sends one message
    /// rather than two and cannot end up followed from the wrong place in between.
    ///
    /// Across a promotion this lands on `Ahead` and restates. Channel numbering
    /// starts over when a venue does -- a new leader replays the journal to rebuild
    /// state but does not republish -- so a cursor from the previous leader names a
    /// sequence this channel has never reached. Gap-filling across a promotion
    /// would mean retaining rings for every channel and republishing during
    /// replay, which costs the whole feed budget on a venue with no subscribers at
    /// all. Restating is correct and cheap; the cost is one snapshot per client per
    /// promotion.
    fn resume_from(&mut self, index: usize, channel: Channel, from: Sequence, symbol: SymbolId) {
        // Retained from here on whatever happens next, so a client that is told
        // to restate is already being followed when it does.
        let now = self.venue.subscribe(channel);
        self.follow(index, channel, from.min(now));

        // A resume from zero says "I hold nothing", and on a channel that carries
        // state that means a restatement rather than a replay. Serving it as an
        // ordinary resume would answer `Delivered` over an empty window and hand
        // the client a book channel with no book -- silence that looks like a
        // quiet market.
        if from == 0 {
            self.metrics.restated();
            self.resynchronise(&[(index, channel)]);
            return;
        }

        let mut bytes = std::mem::take(&mut self.session_at(index).outbox);
        let outcome = self.venue.resume_bytes(channel, from, &mut bytes);
        self.session_at(index).outbox = bytes;

        match outcome {
            Resume::Delivered { next } => {
                self.session_at(index).reposition(channel, next);
                self.owes_bytes(index);
            }
            // Too far behind, or holding a cursor this run never issued.
            Resume::Lagged { .. } | Resume::Ahead { .. } => {
                self.metrics.restated();
                if let Channel::Account(account) = channel {
                    // A private feed cannot be restated the way a book can --
                    // there is no snapshot of "your orders since sequence N".
                    // The passive path drops the session for exactly that
                    // reason, and dropping it here would be wrong: the client
                    // *asked*, and answering a request with a disconnect leaves
                    // it to reconnect and ask again identically, forever.
                    //
                    // So it is placed at the current end of its own feed and
                    // told what it holds there, which is the same answer a
                    // client gets from `QueryOpenOrders` and the only thing that
                    // makes the position it has been given meaningful.
                    // Bounded to the instrument the command names, not swept over
                    // every listed symbol. The budget that sheds a session counts
                    // queued bytes, so a sweep makes the answer to a resume large
                    // enough to shed the client it is answering -- and a client
                    // shed by its own resume reconnects, resumes, and is shed
                    // again. One symbol costs at most that book's order limit,
                    // and `QueryOpenOrders` is how a client walks the rest at its
                    // own pace, which is the mechanism it already uses.
                    let at = self.venue.hub().next_sequence(channel).unwrap_or_default();
                    self.session_at(index).reposition(channel, at);
                    self.answer_open_orders(index, symbol, account);
                } else {
                    self.resynchronise(&[(index, channel)]);
                }
            }
            // `subscribe` above created the ring, so this cannot happen. Restated
            // rather than ignored: if it ever does, the client is following a
            // channel nothing retains and would otherwise sit silent.
            Resume::NotSubscribed => {
                self.metrics.restated();
                self.resynchronise(&[(index, channel)]);
            }
        }
    }

    /// Hands an account the outcomes a replay regenerated for it.
    ///
    /// Non-empty only after a recovery, and only for commands past the last
    /// watermark: the ones whose outcome events died with the previous run.
    /// Delivered once, to the first session that proves the account -- the
    /// events carry sequence zero, marking them as redelivery rather than a
    /// position in the account channel, which restarted.
    fn deliver_recovered_outcomes(&mut self, index: usize, account: AccountId) {
        let Some(outcomes) = self.venue.exchange_mut().take_pending_outcomes(account) else {
            return;
        };
        let session = self.session_at(index);
        for outcome in &outcomes {
            encode(outcome, &mut session.outbox);
        }
        self.owes_bytes(index);
    }

    /// Tells one session what it still has working on a symbol.
    ///
    /// A client can rebuild a book from a snapshot but not its own orders, so
    /// this is what a trader needs after reconnecting before it can act. Costs
    /// that account's own order count rather than a scan of the venue.
    fn answer_open_orders(&mut self, index: usize, symbol: SymbolId, account: AccountId) {
        let orders = self.venue.exchange().open_orders_for(account, symbol);
        let session = self.session_at(index);
        for resting in &orders {
            encode(
                &bx_pipeline::order_state(account, symbol, resting),
                &mut session.outbox,
            );
        }
        self.owes_bytes(index);
    }

    /// Writes the current top of book to one session, one event a side.
    ///
    /// The same `Bbo` events the feed carries, not a separate snapshot kind: the
    /// top of book *is* its own state, so restating it and changing it are the
    /// same message. That is why this channel needs no equivalent of
    /// `BookSnapshot`.
    fn send_top_state(&mut self, index: usize, symbol: SymbolId, at: Sequence) {
        let Some(book) = self.venue.book(symbol) else {
            return;
        };
        let tops = [Side::Bid, Side::Ask].map(|side| {
            let (price, quantity) = book.top(side);
            (side, price, quantity)
        });

        let session = self.session_at(index);
        for (side, price, quantity) in tops {
            encode(
                &Event {
                    sequence: at,
                    quantity,
                    price,
                    symbol,
                    kind: EventKind::Bbo as u8,
                    side: side as u8,
                    ..Event::default()
                },
                &mut session.outbox,
            );
        }
        self.owes_bytes(index);
    }

    /// Writes the book's current levels to one session as `BookSnapshot`.
    ///
    /// `at` is the channel sequence the increments will resume from, and every
    /// snapshot event carries it, so a client knows precisely where state ends
    /// and change begins. Taken in the same pass as the cursor, so the two cannot
    /// disagree.
    fn send_book_state(&mut self, index: usize, symbol: SymbolId, at: Sequence) {
        let Some(book) = self.venue.book(symbol) else {
            return;
        };
        // Owned, so the borrow of the venue ends before the session is written.
        let levels: Vec<(Side, Ticks, u64)> = [Side::Bid, Side::Ask]
            .into_iter()
            .flat_map(|side| {
                book.depth(side, usize::MAX)
                    .into_iter()
                    .map(move |(price, quantity)| (side, price, quantity))
            })
            .collect();

        let session = self.session_at(index);
        for (side, price, quantity) in levels {
            encode(
                &Event {
                    sequence: at,
                    cause_sequence: 0,
                    account: 0,
                    order_id: 0,
                    counterparty_order_id: 0,
                    quantity,
                    price,
                    symbol,
                    kind: EventKind::BookSnapshot as u8,
                    side: side as u8,
                    reject_reason: 0,
                    _pad: [0; 1],
                },
                &mut session.outbox,
            );
        }
        self.owes_bytes(index);
    }

    /// Entries in the channel-to-session index.
    ///
    /// Exposed because the failure it guards against is invisible otherwise. A
    /// departed session's entries are harmless to *correctness* — the write pass
    /// checks that the session actually follows the channel before sending it
    /// anything — so a leak here would never produce a wrong event. It would only
    /// grow, quietly, for the life of the process, which at a million clients
    /// connecting and disconnecting is the kind of leak found in production
    /// rather than in a test.
    #[must_use]
    pub fn tracked_subscriptions(&self) -> usize {
        self.subscribers.values().map(Vec::len).sum()
    }

    /// Sessions refused because the venue was already full. An operator watching
    /// this climb knows to add a gateway rather than wonder why latency drifted.
    #[must_use]
    pub const fn refused(&self) -> u64 {
        self.refused
    }

    fn accept_pending(&mut self) {
        self.accept_from(false);
        if self.tls_listener.is_some() {
            self.accept_from(true);
        }
    }

    /// Drains one listener. The two doors differ in exactly one thing: a TLS
    /// session carries a record layer; everything past it is identical.
    fn accept_from(&mut self, encrypted: bool) {
        loop {
            let accepted = if encrypted {
                self.tls_listener
                    .as_ref()
                    .expect("checked by the caller")
                    .accept()
            } else {
                self.listener.accept()
            };
            match accepted {
                Ok((mut stream, _)) => {
                    // Accepted and dropped, so the client learns immediately
                    // rather than waiting on a venue that will not read it.
                    if self.live >= self.max_sessions {
                        self.refused += 1;
                        self.metrics.refused();
                        drop(stream);
                        continue;
                    }
                    let tls = if encrypted {
                        let config = self.tls_config.as_ref().expect("set with the listener");
                        match rustls::ServerConnection::new(std::sync::Arc::clone(config)) {
                            Ok(connection) => Some(Box::new(connection)),
                            Err(_) => continue,
                        }
                    } else {
                        None
                    };
                    let index = self.free.pop().unwrap_or(self.sessions.len());
                    if self
                        .poll
                        .registry()
                        .register(&mut stream, session_token(index), Interest::READABLE)
                        .is_err()
                    {
                        self.free.push(index);
                        continue;
                    }
                    let _ = stream.set_nodelay(true);
                    // A fresh nonce per connection, which is what makes a
                    // captured proof worthless against the next one.
                    let challenge = (self.mode == Mode::Required).then(auth::nonce);
                    let mut session = Session::new(
                        stream,
                        tls,
                        self.max_records_per_session,
                        self.max_outbox,
                        challenge,
                    );
                    // Queued before the client has said anything: it cannot
                    // prove who it is until it knows what to sign. Behind TLS
                    // it waits in the record layer until the handshake ends;
                    // rustls holds plaintext written this early and sends it
                    // with the first application flight.
                    if let Some(nonce) = challenge {
                        encode(&Event::challenging(nonce), &mut session.outbox);
                        session.flush();
                    }
                    let unsent = !session.outbox.is_empty();
                    if index == self.sessions.len() {
                        self.sessions.push(Some(session));
                    } else {
                        self.sessions[index] = Some(session);
                    }
                    // A challenge the socket would not take yet still has to
                    // reach the client, or it waits for a nonce that is sitting
                    // in a buffer nobody writes.
                    if unsent {
                        self.owes_bytes(index);
                    }
                    self.live += 1;
                    self.metrics.accepted();
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }

    /// Sends each session everything on its channels it has not seen.
    fn push_updates(&mut self, published: bool) {
        // Channels that fell outside the window, handled after the borrows end.
        let mut lagged: Vec<(usize, Channel)> = Vec::new();

        // Only the channels that received something, and only the sessions
        // following them. What this replaced — every session, every channel it
        // follows, every pass — is what made an idle connection cost anything.
        //
        // Read only after a group that produced events. The hub's list is from
        // its last publish, so consulting it on a pass that applied nothing
        // would walk the previous group's subscribers again and hand back most
        // of the cost this exists to remove.
        self.touched.clear();
        if published {
            self.touched.extend_from_slice(self.venue.hub().touched());
        }
        // Every follower is served before this pass returns to the order
        // socket, which is what puts the crowd table's p50 in milliseconds
        // while the same order load alone answers in microseconds.
        //
        // Serving a bounded slice per pass and resuming where it left off is
        // the obvious fix and does not work. It measured p50 down from 14.3 ms
        // to 1.9 ms and then failed the crowd test twice: with a thousand
        // followers resident, new connections stopped being accepted and the
        // load client timed out connecting, every phase after that lost. The
        // unit suite passed throughout, which is exactly why the load test
        // exists. Whatever starves the accept path there is not understood
        // yet, and a latency win that costs a venue its ability to take on a
        // client is not a win.
        //
        // The fix a venue actually uses is a publisher that is not this
        // thread -- or multicast, which costs one packet regardless of
        // audience. Both are real work; neither is a smaller constant here.
        for channel in &self.touched {
            let Some(following) = self.subscribers.get(channel) else {
                continue;
            };
            for &index in following {
                let Some(session) = self.sessions[index].as_mut() else {
                    continue;
                };
                let Some(cursor) = session
                    .cursors
                    .iter_mut()
                    .find(|(held, _)| held == channel)
                    .map(|(_, cursor)| cursor)
                else {
                    continue;
                };
                // Straight from the ring into the bytes that go on the wire. The
                // events already *are* those bytes, so a `Vec<Event>` in between
                // copied every event a second time, per subscriber, per message.
                match self
                    .venue
                    .resume_bytes(*channel, *cursor, &mut session.outbox)
                {
                    Resume::Delivered { next } => *cursor = next,
                    // Both need the same repair -- drop the queue and restate
                    // the channel -- and they are counted the same way. They
                    // are different diagnoses though: lagging is a client too
                    // slow to keep up, while ahead is a client holding a cursor
                    // this run never issued, which after a promotion is every
                    // client that was connected to the previous leader.
                    Resume::Lagged { .. } | Resume::Ahead { .. } => {
                        lagged.push((index, *channel));
                    }
                    Resume::NotSubscribed => {}
                }
                if !session.outbox.is_empty() && !session.owes {
                    session.owes = true;
                    self.owing.push(index);
                }
            }
        }

        // Written to only the sessions that have something waiting. A session
        // whose socket is full stays here until it drains.
        // Compacted in place. Draining into two fresh vectors and assigning the
        // survivors back cost a pair of allocations on every pass that wrote
        // anything, which is every pass under load. The list only ever shrinks
        // here, so the survivors fit in front of the entries already read.
        let mut shed = 0_u32;
        let mut kept = 0_usize;
        for cursor in 0..self.owing.len() {
            let index = self.owing[cursor];
            let Some(session) = self.sessions[index].as_mut() else {
                continue;
            };
            // The flag, not the list, is the truth. A slot freed on one pass can
            // be taken by a new connection on the next while this list still
            // names it, and that entry now points at somebody else, who has
            // their own flag saying whether they owe anything. Trusting the list
            // would flush a stranger and, worse, let the same index be carried
            // forward twice and accumulate.
            if !session.owes {
                continue;
            }
            session.flush();
            if session.over_budget() {
                session.open = false;
                shed += 1;
            }
            let still_owes = session.open && !session.outbox.is_empty();
            let closed = !session.open;
            if !still_owes {
                session.owes = false;
            }
            if still_owes {
                self.owing[kept] = index;
                kept += 1;
            } else if closed {
                self.closing.push(index);
            }
        }
        self.owing.truncate(kept);
        for _ in 0..shed {
            self.metrics.shed();
        }
        self.resynchronise(&lagged);
    }

    /// Marks a session as owing bytes, so the write pass visits it.
    ///
    /// Called wherever something is put in an outbox outside the feed path — a
    /// challenge, a refusal, a book restated on subscribe. Missing one of these
    /// would leave a session holding bytes nobody ever writes, which is a client
    /// that hangs rather than an error anybody sees.
    fn owes_bytes(&mut self, index: usize) {
        if let Some(session) = self.sessions[index].as_mut()
            && !session.owes
            && !session.outbox.is_empty()
        {
            session.owes = true;
            self.owing.push(index);
        }
    }

    /// Puts a session that fell outside the retention window back on its feet.
    ///
    /// Rarely reached, and worth saying why: a cursor advances every pass whether
    /// or not the client is reading, so a session only falls outside the window if
    /// a single group produced more events than the window holds. The ordinary
    /// overload path is the outbox budget, which sheds the session -- and what
    /// makes *that* survivable is that reconnecting restates the book.
    ///
    /// What is possible depends on what the channel carries, and the three cases
    /// are genuinely different rather than three spellings of one:
    ///
    /// - **A book** is state, so it can be restated: send the current levels and
    ///   resume from there. Falling behind costs the client the increments it
    ///   missed and nothing else.
    /// - **A tape** is history. The prints are gone, and no snapshot brings them
    ///   back, so the cursor jumps to the present. The tape is informational, so
    ///   a hole in it is a loss of information rather than of correctness.
    /// - **An account feed** is neither. Its missed events are the client's own
    ///   fills, and skipping them would leave the client believing a position it
    ///   does not hold. Nothing here can repair that, so the session is dropped:
    ///   a client that knows it is broken can reconcile, one that was quietly
    ///   skipped forward cannot. An order-status query is what would fix this
    ///   properly, and there is not one yet.
    fn resynchronise(&mut self, lagged: &[(usize, Channel)]) {
        for (index, channel) in lagged.iter().copied() {
            self.metrics.restated();
            match channel {
                Channel::Book(symbol) => {
                    let at = self.venue.hub().next_sequence(channel).unwrap_or_default();
                    if let Some(session) = self.sessions[index].as_mut() {
                        session.reposition(channel, at);
                    }
                    // Queued bytes are left alone. Dropping them looks free --
                    // superseded book deltas the snapshot is about to overwrite --
                    // but a session has one outbox and a cursor per channel, so
                    // the queue also holds this client's own fills, and those
                    // cursors have already moved past them. Clearing lost them
                    // permanently: a trader whose depth feed fell behind stopped
                    // being told about its own orders, which is the one failure a
                    // private feed may never have. The snapshot carries the
                    // sequence it was taken at and supersedes anything before it,
                    // so the stale deltas ahead of it are harmless -- a few
                    // wasted frames on the repair path, and nothing on the hot
                    // path, which never comes here.
                    self.send_book_state(index, symbol, at);
                }
                // Also state, and cheaper to restate than a book: two events.
                Channel::Bbo(symbol) => {
                    let at = self.venue.hub().next_sequence(channel).unwrap_or_default();
                    if let Some(session) = self.sessions[index].as_mut() {
                        session.reposition(channel, at);
                    }
                    self.send_top_state(index, symbol, at);
                }
                // A gap in the tape is a gap: there is no state to restate, so
                // the cursor moves to the front and the client knows it missed
                // prints.
                //
                // The chain heads are the same shape and for a sharper reason. A
                // head commits to records; one a client did not see cannot be
                // checked, so replaying it would hand over a commitment the
                // client has no way to test. Moving the cursor forward tells it
                // to verify from here on.
                Channel::Trades(_) | Channel::Checkpoint => {
                    let at = self.venue.hub().next_sequence(channel).unwrap_or_default();
                    if let Some(session) = self.sessions[index].as_mut() {
                        session.reposition(channel, at);
                    }
                }
                Channel::Account(_) => {
                    if let Some(session) = self.sessions[index].as_mut() {
                        session.open = false;
                    }
                    self.closing.push(index);
                }
            }
        }
    }
}

/// Reads whole events out of a client socket. Test and tooling helper.
///
/// # Errors
/// Returns the underlying I/O error.
pub fn read_events(
    stream: &mut std::net::TcpStream,
    want: usize,
    out: &mut Vec<Event>,
) -> io::Result<()> {
    let mut buffer = vec![0_u8; want * FRAME_LEN];
    let mut filled = 0;
    while out.len() < want {
        match stream.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(bytes) => {
                filled += bytes;
                let whole = filled / FRAME_LEN;
                for index in 0..whole {
                    let start = index * FRAME_LEN;
                    if let Ok(event) = <Event as zerocopy::FromBytes>::read_from_bytes(
                        &buffer[start..start + FRAME_LEN],
                    ) {
                        out.push(event);
                    }
                }
                buffer.copy_within(whole * FRAME_LEN..filled, 0);
                filled -= whole * FRAME_LEN;
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
