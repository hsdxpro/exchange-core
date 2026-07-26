//! A server over real sockets.
//!
//! One thread, non-blocking sockets, no locks. Matching a symbol is inherently
//! single-writer, so a thread per connection would only add contention around a
//! section that cannot run in parallel anyway. The loop reads whatever has
//! arrived from every session, applies it as one group, commits once, and
//! writes each session whatever it has not yet seen.
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
//! Public feeds are opt-in rather than given to everyone. At one instrument the
//! difference is invisible; at a thousand, sending every session every book
//! would multiply the venue's outbound traffic by the number of instruments
//! nobody asked about.

use crate::codec::{Decoder, FRAME_LEN, encode};
use crate::venue::Venue;
use bx_journal::LogStorage;
use bx_pipeline::hub::{Channel, Resume};
use bx_pipeline::instrument::Instruments;
use bx_pipeline::snapshot::Snapshot;
use bx_protocol::{AccountId, Command, CommandKind, Event, Sequence};
use std::fs::File;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// One connected client.
#[derive(Debug)]
struct Session {
    stream: TcpStream,
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
    /// Set by the first command, which is how a session declares who it is.
    account: Option<AccountId>,
    open: bool,
}

impl Session {
    fn new(stream: TcpStream, max_records: usize, max_outbox: usize) -> Self {
        Self {
            stream,
            decoder: Decoder::new(max_records),
            outbox: Vec::new(),
            max_outbox,
            cursors: Vec::new(),
            account: None,
            open: true,
        }
    }

    /// Adds a channel this session will be sent, from the position it is at now.
    /// Subscribing twice is idempotent and does not rewind the cursor.
    fn follow(&mut self, channel: Channel, from: Sequence) {
        if !self.cursors.iter().any(|(held, _)| *held == channel) {
            self.cursors.push((channel, from));
        }
    }

    fn stop_following(&mut self, channel: Channel) {
        self.cursors.retain(|(held, _)| *held != channel);
    }

    /// Reads one buffer's worth, appending decoded commands to `out`.
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
    fn read_into(&mut self, out: &mut Vec<Command>) {
        if self.decoder.is_full() {
            return;
        }
        match self.stream.read(self.decoder.writable()) {
            // A read of zero on a stream socket means the peer closed.
            Ok(0) => self.open = false,
            Ok(bytes) => {
                self.decoder.advance(bytes);
                self.decoder.drain(out);
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::Interrupted => {}
            Err(_) => self.open = false,
        }
    }

    /// True once this session owes more bytes than it is allowed to queue.
    ///
    /// Such a client is already beyond saving: the backlog exceeds a full
    /// retention window, so even if it started reading now the feed it needs
    /// would have been overwritten. Dropping it is the same policy as lagging
    /// out of the window, applied one step earlier and for the same reason --
    /// the venue neither slows down for a slow client nor accumulates for one.
    fn over_budget(&self) -> bool {
        self.outbox.len() > self.max_outbox
    }

    /// Pushes whatever the socket will take. Anything it will not take stays
    /// queued rather than blocking the venue on one slow client.
    fn flush(&mut self) {
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
    listener: TcpListener,
    venue: Venue<S>,
    sessions: Vec<Session>,
    /// Unsent bytes one session may owe. Derived from the retention window
    /// rather than chosen: a session further behind than the window cannot be
    /// caught up whatever the venue does.
    max_outbox: usize,
    /// Set to snapshot periodically. None means never, which is slower to
    /// recover and no less correct.
    snapshots: Option<(SnapshotPolicy, PathBuf)>,
    /// Reused across passes so a steady-state loop allocates nothing.
    inbound: Vec<Command>,
    outbound: Vec<Event>,
    max_records_per_session: usize,
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
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        let venue = Venue::new(storage, instruments, retained_per_channel)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let server = Self {
            listener,
            venue,
            sessions: Vec::new(),
            inbound: Vec::new(),
            outbound: Vec::new(),
            max_records_per_session,
            max_outbox: retained_per_channel * FRAME_LEN,
            snapshots: None,
        };
        Ok(server)
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
    pub fn sessions(&self) -> usize {
        self.sessions.len()
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

    /// One pass: accept, read, apply as a group, commit, write.
    ///
    /// Returns how many commands were applied.
    ///
    /// # Errors
    /// Fails only if the journal cannot be written or flushed.
    pub fn poll(&mut self) -> bx_journal::Result<usize> {
        self.accept_pending();

        self.inbound.clear();
        for index in 0..self.sessions.len() {
            let start = self.inbound.len();
            self.sessions[index].read_into(&mut self.inbound);

            // Attribute each session's account from its *own* first command.
            // Reading everyone into one buffer first and then handing accounts
            // out would give a session whoever happened to be at the front.
            if self.sessions[index].account.is_none()
                && let Some(command) = self.inbound.get(start)
            {
                let account = command.account;
                self.sessions[index].account = Some(account);
                // A session always gets its own private feed; it has to ask for
                // anything public.
                let channel = Channel::Account(account);
                let from = self.venue.subscribe(channel);
                self.sessions[index].follow(channel, from);
            }

            // Control messages belong to the connection, not the venue. Taking
            // them out here is what keeps them out of the journal: a
            // subscription replayed after a restart would resurrect a feed for a
            // connection that no longer exists.
            let account = self.sessions[index].account.unwrap_or_default();
            let mut cursor = start;
            while cursor < self.inbound.len() {
                if self.inbound[cursor].is_session_control() {
                    let command = self.inbound.remove(cursor);
                    self.apply_control(index, &command, account);
                } else {
                    cursor += 1;
                }
            }
        }

        let applied = self.inbound.len();
        if applied > 0 {
            let mut group = std::mem::take(&mut self.inbound);
            let result = self.venue.accept(&mut group);
            self.inbound = group;
            result?;
        }

        self.push_updates();
        self.sessions.retain(|session| session.open);
        Ok(applied)
    }

    /// Starts or stops one session's feed. A channel that does not decode is
    /// ignored rather than guessed at.
    fn apply_control(&mut self, index: usize, command: &Command, account: AccountId) {
        let Some(kind) = command.channel_kind() else {
            return;
        };
        let channel = Channel::requested(kind, command.symbol, account);
        if command.kind() == Some(CommandKind::Subscribe) {
            let from = self.venue.subscribe(channel);
            self.sessions[index].follow(channel, from);
        } else {
            self.sessions[index].stop_following(channel);
        }
    }

    fn accept_pending(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    if stream.set_nonblocking(true).is_ok() && stream.set_nodelay(true).is_ok() {
                        self.sessions.push(Session::new(
                            stream,
                            self.max_records_per_session,
                            self.max_outbox,
                        ));
                    }
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }

    /// Sends each session everything on its channels it has not seen.
    fn push_updates(&mut self) {
        for session in &mut self.sessions {
            for (channel, cursor) in &mut session.cursors {
                self.outbound.clear();
                match self.venue.resume(*channel, *cursor, &mut self.outbound) {
                    Resume::Delivered { next } => *cursor = next,
                    // The session fell outside the retention window. It is told
                    // by being dropped rather than being fed a hole; a real
                    // client reconnects and takes a snapshot.
                    Resume::Lagged { .. } => session.open = false,
                    Resume::NotSubscribed => {}
                }
                for event in &self.outbound {
                    encode(event, &mut session.outbox);
                }
            }
            session.flush();
            if session.over_budget() {
                session.open = false;
            }
        }
    }
}

/// Reads whole events out of a client socket. Test and tooling helper.
///
/// # Errors
/// Returns the underlying I/O error.
pub fn read_events(stream: &mut TcpStream, want: usize, out: &mut Vec<Event>) -> io::Result<()> {
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
