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
//! A session receives the public feed for every listed instrument plus the
//! private feed for the account it trades as, which it declares by being the
//! account on its first command. Selective subscription is a protocol addition,
//! not a change to this loop.

use crate::codec::{Decoder, FRAME_LEN, encode};
use crate::venue::Venue;
use bx_journal::LogStorage;
use bx_pipeline::hub::{Channel, Resume};
use bx_pipeline::instrument::Instruments;
use bx_protocol::{AccountId, Command, Event, Sequence, SymbolId};
use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

/// One connected client.
#[derive(Debug)]
struct Session {
    stream: TcpStream,
    decoder: Decoder,
    /// Bytes framed but not yet accepted by the socket.
    outbox: Vec<u8>,
    /// Channels this session follows, and where it has read to.
    cursors: Vec<(Channel, Sequence)>,
    /// Set by the first command, which is how a session declares who it is.
    account: Option<AccountId>,
    open: bool,
}

impl Session {
    fn new(stream: TcpStream, max_records: usize, public: &[Channel]) -> Self {
        Self {
            stream,
            decoder: Decoder::new(max_records),
            outbox: Vec::new(),
            cursors: public.iter().map(|channel| (*channel, 0)).collect(),
            account: None,
            open: true,
        }
    }

    /// Reads whatever has arrived, appending decoded commands to `out`.
    fn read_into(&mut self, out: &mut Vec<Command>) {
        loop {
            if self.decoder.is_full() {
                break;
            }
            match self.stream.read(self.decoder.writable()) {
                // A read of zero on a stream socket means the peer closed.
                Ok(0) => {
                    self.open = false;
                    break;
                }
                Ok(bytes) => {
                    self.decoder.advance(bytes);
                    self.decoder.drain(out);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(_) => {
                    self.open = false;
                    break;
                }
            }
        }
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

/// The venue, listening.
#[derive(Debug)]
pub struct Server<S: LogStorage> {
    listener: TcpListener,
    venue: Venue<S>,
    sessions: Vec<Session>,
    /// Public channels every session follows.
    public: Vec<Channel>,
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
        let symbols: Vec<SymbolId> = instruments.iter().map(|i| i.symbol).collect();
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        let venue = Venue::new(storage, instruments, retained_per_channel)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let mut server = Self {
            listener,
            venue,
            sessions: Vec::new(),
            public: symbols
                .iter()
                .flat_map(|s| [Channel::Book(*s), Channel::Trades(*s)])
                .collect(),
            inbound: Vec::new(),
            outbound: Vec::new(),
            max_records_per_session,
        };
        for channel in server.public.clone() {
            server.venue.subscribe(channel);
        }
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

    /// One pass: accept, read, apply as a group, commit, write.
    ///
    /// Returns how many commands were applied.
    ///
    /// # Errors
    /// Fails only if the journal cannot be written or flushed.
    pub fn poll(&mut self) -> bx_journal::Result<usize> {
        self.accept_pending();

        self.inbound.clear();
        for session in &mut self.sessions {
            // Attribute each session's account from its *own* first command.
            // Reading everyone into one buffer first and then handing accounts
            // out would give a session whoever happened to be at the front.
            let start = self.inbound.len();
            session.read_into(&mut self.inbound);
            if session.account.is_none()
                && let Some(command) = self.inbound.get(start)
            {
                let account = command.account;
                session.account = Some(account);
                session.cursors.push((Channel::Account(account), 0));
                self.venue.subscribe(Channel::Account(account));
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

    fn accept_pending(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    if stream.set_nonblocking(true).is_ok() && stream.set_nodelay(true).is_ok() {
                        self.sessions.push(Session::new(
                            stream,
                            self.max_records_per_session,
                            &self.public,
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
