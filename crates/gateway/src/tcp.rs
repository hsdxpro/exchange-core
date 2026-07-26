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

use crate::codec::{Decoder, FRAME_LEN, encode};
use crate::venue::Venue;
use bx_journal::LogStorage;
use bx_pipeline::hub::{Channel, Resume};
use bx_pipeline::instrument::Instruments;
use bx_pipeline::snapshot::Snapshot;
use bx_protocol::{
    AccountId, Command, CommandKind, Event, EventKind, Sequence, Side, SymbolId, Ticks,
};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use std::fs::File;
use std::io::{self, ErrorKind, Read, Write};
use std::net::SocketAddr;
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
    open: bool,
}

/// The listener's token. Sessions take `index + 1`, so zero is never a session.
const LISTENER: Token = Token(0);

fn session_token(index: usize) -> Token {
    Token(index + 1)
}

fn session_index(token: Token) -> Option<usize> {
    (token != LISTENER).then(|| token.0 - 1)
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
            cancel_on_disconnect: false,
            queued: false,
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
    fn read_into(&mut self, out: &mut Vec<Command>) -> bool {
        if self.decoder.writable().is_empty() {
            return true;
        }
        match self.stream.read(self.decoder.writable()) {
            // A read of zero on a stream socket means the peer closed.
            Ok(0) => {
                self.open = false;
                false
            }
            Ok(bytes) => {
                self.decoder.advance(bytes);
                self.decoder.drain(out);
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
    poll: Poll,
    events: Events,
    listener: TcpListener,
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
    /// Reused across passes so a steady-state loop allocates nothing.
    inbound: Vec<Command>,
    outbound: Vec<Event>,
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
            venue,
            sessions: Vec::new(),
            free: Vec::new(),
            live: 0,
            still_readable: Vec::new(),
            closing: Vec::new(),
            inbound: Vec::new(),
            outbound: Vec::new(),
            max_records_per_session,
            max_sessions,
            max_outbox: retained_per_channel * FRAME_LEN,
            snapshots: None,
            refused: 0,
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

    /// One pass: accept, read, apply as a group, commit, write.
    ///
    /// Returns how many commands were applied.
    ///
    /// # Errors
    /// Fails only if the journal cannot be written or flushed.
    pub fn poll(&mut self) -> bx_journal::Result<usize> {
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
        for index in ready {
            let start = self.inbound.len();
            let Some(session) = self.sessions.get_mut(index).and_then(Option::as_mut) else {
                continue;
            };
            if session.read_into(&mut self.inbound) {
                // Stays flagged: it is going straight back into the ready set.
                self.still_readable.push(index);
            } else {
                session.queued = false;
            }
            if !session.open {
                self.closing.push(index);
                continue;
            }

            // Attribute each session's account from its *own* first command.
            // Reading everyone into one buffer first and then handing accounts
            // out would give a session whoever happened to be at the front.
            if session.account.is_none()
                && let Some(command) = self.inbound.get(start)
            {
                let account = command.account;
                session.account = Some(account);
                // A session always gets its own private feed; it has to ask for
                // anything public.
                let channel = Channel::Account(account);
                let from = self.venue.subscribe(channel);
                self.session_at(index).follow(channel, from);
            }

            // Control messages belong to the connection, not the venue. Taking
            // them out here is what keeps them out of the journal: a
            // subscription replayed after a restart would resurrect a feed for a
            // connection that no longer exists.
            let account = self.session_at(index).account.unwrap_or_default();
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
        self.drop_closed();
        Ok(applied)
    }

    fn session_at(&mut self, index: usize) -> &mut Session {
        self.sessions[index]
            .as_mut()
            .expect("a ready token names a live session")
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
                if session.cancel_on_disconnect
                    && let Some(account) = session.account
                {
                    withdraw.push(account);
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
        if command.kind() == Some(CommandKind::Subscribe) {
            let from = self.venue.subscribe(channel);
            self.session_at(index).follow(channel, from);
            if let Channel::Book(symbol) = channel {
                self.send_book_state(index, symbol, from);
            }
        } else {
            self.session_at(index).stop_following(channel);
        }
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
    }

    /// Sessions refused because the venue was already full. An operator watching
    /// this climb knows to add a gateway rather than wonder why latency drifted.
    #[must_use]
    pub const fn refused(&self) -> u64 {
        self.refused
    }

    fn accept_pending(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((mut stream, _)) => {
                    // Accepted and dropped, so the client learns immediately
                    // rather than waiting on a venue that will not read it.
                    if self.live >= self.max_sessions {
                        self.refused += 1;
                        drop(stream);
                        continue;
                    }
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
                    let session =
                        Session::new(stream, self.max_records_per_session, self.max_outbox);
                    if index == self.sessions.len() {
                        self.sessions.push(Some(session));
                    } else {
                        self.sessions[index] = Some(session);
                    }
                    self.live += 1;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }

    /// Sends each session everything on its channels it has not seen.
    fn push_updates(&mut self) {
        let mut closed = Vec::new();
        // Channels that fell outside the window, handled after the borrow ends.
        let mut lagged: Vec<(usize, Channel)> = Vec::new();
        for (index, session) in self
            .sessions
            .iter_mut()
            .enumerate()
            .filter_map(|(index, slot)| slot.as_mut().map(|session| (index, session)))
        {
            for (channel, cursor) in &mut session.cursors {
                self.outbound.clear();
                match self.venue.resume(*channel, *cursor, &mut self.outbound) {
                    Resume::Delivered { next } => *cursor = next,
                    Resume::Lagged { .. } => lagged.push((index, *channel)),
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
            if !session.open {
                closed.push(index);
            }
        }
        self.closing.append(&mut closed);
        self.resynchronise(&lagged);
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
            match channel {
                Channel::Book(symbol) => {
                    let at = self.venue.hub().next_sequence(channel).unwrap_or_default();
                    if let Some(session) = self.sessions[index].as_mut() {
                        session.reposition(channel, at);
                        // Anything already queued describes a book the client is
                        // about to be told afresh.
                        session.outbox.clear();
                    }
                    self.send_book_state(index, symbol, at);
                }
                Channel::Trades(_) => {
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
