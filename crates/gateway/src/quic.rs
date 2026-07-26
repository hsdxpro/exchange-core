//! QUIC transport.
//!
//! One protocol for everyone. UDP/443 with 0-RTT resumption reaches retail
//! behind NAT and on mobile networks; a market maker on a cross-connect gets the
//! same API and the same floor. QUIC costs roughly five to twenty microseconds
//! more per exchange than raw TCP for its crypto and userspace processing, which
//! is invisible beneath the durability cost measured elsewhere in this crate: 51
//! microseconds to reach a quorum, 3.1 milliseconds to reach a local disk.
//! Optimising the transport below the sync would be optimising the wrong thing.
//!
//! **Why QUIC rather than a second TCP socket.** Order acknowledgements and
//! market data travel on separate streams, each with its own flow control. One
//! TCP connection carrying both means a client reading its feed slowly backs up
//! its own fills, which is the whole reason the TCP server needs an outbox budget
//! and sheds sessions. Here a slow feed consumer stalls its feed and nothing
//! else.
//!
//! **Async at the edge, single writer at the core.** Matching is a sequential
//! dependency and cannot be parallelised, so the venue stays one thread with no
//! runtime. QUIC connections run on tokio and hand commands across a queue. That
//! queue is also where the group comes from: a pass takes whatever arrived since
//! the last one, so groups grow under load exactly when a sync needs amortising.
//!
//! ```text
//!   connections (tokio)  ──queue──►  venue thread (one writer)
//!            ▲                              │
//!            └────── per-session queues ────┘
//! ```

use crate::codec::{Decoder, FRAME_LEN, encode};
use crate::venue::Venue;
use bx_journal::LogStorage;
use bx_pipeline::hub::{Channel, Resume};
use bx_pipeline::instrument::Instruments;
use bx_protocol::{AccountId, Command, CommandKind, Event, Sequence, Side, SymbolId, Ticks};
use quinn::{Endpoint, ServerConfig};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

/// Protocol name offered in the TLS handshake, so a peer speaking something else
/// is rejected at the handshake rather than misread as orders.
pub const ALPN: &[u8] = b"bx/1";

/// Batches one stream may have waiting before the venue stops queueing for it.
///
/// Counted in batches rather than events because a batch is what one pass
/// produces, so this is "how many passes behind a stream may fall". Past it the
/// client is not keeping up on that channel and its updates are dropped -- a book
/// is restated when it falls outside the window, which costs less than either
/// stalling the venue or queueing without limit.
const QUEUED_BATCHES: usize = 64;

/// What a connection tells the venue thread.
#[derive(Debug)]
enum FromClient {
    /// A decoded command. Session control is included: the venue thread owns the
    /// hub, so it is the only place that can start a feed.
    Command(Command),
    Gone,
}

/// What the venue thread tells a connection. The channel decides the stream.
type ToClient = (Channel, Vec<Event>);

/// A connection, as the venue thread sees it.
#[derive(Debug)]
struct Session {
    outbound: UnboundedSender<ToClient>,
    cursors: Vec<(Channel, Sequence)>,
    account: Option<AccountId>,
    /// Cancel this account's resting orders when the connection goes. Per
    /// session, and scoped to the account.
    cancel_on_disconnect: bool,
}

/// A self-signed certificate, for local runs and tests.
///
/// A deployment loads a real certificate; this exists so the transport can be
/// exercised end to end without one. The DER is handed back so a client can trust
/// exactly this certificate rather than disabling verification, which is a habit
/// worth not forming even in tests.
///
/// # Errors
/// Fails if the certificate cannot be generated or accepted.
pub fn self_signed(hostname: &str) -> io::Result<(ServerConfig, Vec<u8>)> {
    let key = rcgen::generate_simple_self_signed([hostname.to_string()])
        .map_err(|e| io::Error::other(e.to_string()))?;
    let certificate = key.cert.der().to_vec();
    let private = rustls::pki_types::PrivatePkcs8KeyDer::from(key.key_pair.serialize_der());

    // Built explicitly rather than with `with_single_cert`, because the protocol
    // name has to be offered: a peer speaking something else must be turned away
    // at the handshake rather than have its bytes read as orders. TLS 1.3 only,
    // which is all QUIC permits anyway.
    let mut crypto = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|e| io::Error::other(e.to_string()))?
    .with_no_client_auth()
    .with_single_cert(
        vec![key.cert.der().clone()],
        rustls::pki_types::PrivateKeyDer::Pkcs8(private),
    )
    .map_err(|e| io::Error::other(e.to_string()))?;
    crypto.alpn_protocols = vec![ALPN.to_vec()];

    let mut config = ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
            .map_err(|e| io::Error::other(e.to_string()))?,
    ));
    Arc::get_mut(&mut config.transport)
        .expect("transport config is not yet shared")
        // Nagle has no equivalent here, but a keep-alive shorter than the idle
        // timeout stops a quiet market maker's connection being reaped.
        .keep_alive_interval(Some(Duration::from_secs(5)));
    Ok((config, certificate))
}

/// The venue, listening on QUIC.
pub struct QuicVenue<S: LogStorage> {
    venue: Venue<S>,
    endpoint: Endpoint,
    inbound: Receiver<(u64, FromClient)>,
    sender: SyncSender<(u64, FromClient)>,
    sessions: HashMap<u64, Session>,
    runtime: tokio::runtime::Runtime,
    next_session: u64,
    /// Reused across passes, so a steady-state loop allocates nothing.
    group: Vec<Command>,
    scratch: Vec<Event>,
}

impl<S: LogStorage> std::fmt::Debug for QuicVenue<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicVenue")
            .field("sessions", &self.sessions.len())
            .finish_non_exhaustive()
    }
}

impl<S: LogStorage> QuicVenue<S> {
    /// Binds the endpoint and prepares the venue. Port 0 lets the OS choose.
    ///
    /// `queued_commands` bounds how many commands may wait for the venue thread.
    /// It is back-pressure, not a buffer: when it fills, connection tasks wait,
    /// which slows the clients producing the load rather than letting the queue
    /// consume memory without limit.
    ///
    /// # Errors
    /// Fails if the socket cannot be bound or the journal cannot be opened.
    pub fn bind(
        address: SocketAddr,
        config: ServerConfig,
        storage: S,
        instruments: Instruments,
        retained_per_channel: usize,
        queued_commands: usize,
    ) -> io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let endpoint = runtime.block_on(async { Endpoint::server(config, address) })?;
        let venue = Venue::new(storage, instruments, retained_per_channel)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let (sender, inbound) = sync_channel(queued_commands);

        Ok(Self {
            venue,
            endpoint,
            inbound,
            sender,
            sessions: HashMap::new(),
            runtime,
            next_session: 0,
            group: Vec::new(),
            scratch: Vec::new(),
        })
    }

    /// # Errors
    /// Fails if the socket has no local address.
    pub fn address(&self) -> io::Result<SocketAddr> {
        self.endpoint.local_addr()
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

    /// Accepts whatever has connected since the last call.
    ///
    /// Each connection gets a task on the runtime; this only registers it with
    /// the venue thread, which is the single writer and so the only place a
    /// session may be created.
    fn accept_pending(&mut self) {
        while let Some(incoming) = self
            .runtime
            .block_on(async { futures_lite_poll(&self.endpoint) })
        {
            let id = self.next_session;
            self.next_session += 1;
            let (outbound, receiver) = unbounded_channel();
            self.sessions.insert(
                id,
                Session {
                    outbound,
                    cursors: Vec::new(),
                    account: None,
                    cancel_on_disconnect: false,
                },
            );
            let to_venue = self.sender.clone();
            self.runtime
                .spawn(async move { serve(incoming, id, to_venue, receiver).await });
        }
    }

    /// One pass: take what arrived, apply it as one group, commit once, publish.
    ///
    /// Returns how many commands were applied. Blocks for at most `wait` when
    /// there is nothing to do, so an idle venue does not spin a core; pass zero
    /// to busy-poll a pinned thread instead.
    ///
    /// # Errors
    /// Fails only if the journal cannot be written or flushed.
    pub fn poll(&mut self, wait: Duration) -> bx_journal::Result<usize> {
        self.accept_pending();

        self.group.clear();
        let mut gone = Vec::new();
        // One blocking wait, then drain whatever else is already queued: that is
        // what makes the group as large as the load rather than a chosen size.
        if let Ok((id, message)) = self.inbound.recv_timeout(wait) {
            self.take(id, message, &mut gone);
        }
        while let Ok((id, message)) = self.inbound.try_recv() {
            self.take(id, message, &mut gone);
        }

        let applied = self.group.len();
        if applied > 0 {
            let mut group = std::mem::take(&mut self.group);
            let result = self.venue.accept(&mut group);
            self.group = group;
            result?;
        }

        self.publish();
        let mut withdraw = Vec::new();
        for id in gone {
            if let Some(session) = self.sessions.remove(&id)
                && session.cancel_on_disconnect
                && let Some(account) = session.account
            {
                withdraw.push(account);
            }
        }
        // Ordinary commands, applied after the session is gone, so they are
        // journalled and published like any other cancel.
        for account in withdraw {
            let mut cancels = self.venue.cancels_for(account);
            if !cancels.is_empty() {
                self.venue.accept(&mut cancels)?;
            }
        }
        Ok(applied)
    }

    /// Sorts one message into the group, a subscription, or a disconnection.
    fn take(&mut self, id: u64, message: FromClient, gone: &mut Vec<u64>) {
        match message {
            FromClient::Gone => gone.push(id),
            FromClient::Command(command) => {
                // A session's account is whoever it first traded as, and it gets
                // its own feed without asking. It cannot ask for anyone else's:
                // Channel::requested uses the session's account, not the one in
                // the message.
                if let Some(session) = self.sessions.get_mut(&id)
                    && session.account.is_none()
                {
                    session.account = Some(command.account);
                    let channel = Channel::Account(command.account);
                    let from = self.venue.subscribe(channel);
                    if let Some(session) = self.sessions.get_mut(&id) {
                        session.cursors.push((channel, from));
                    }
                }

                if command.is_session_control() {
                    self.apply_control(id, &command);
                } else {
                    self.group.push(command);
                }
            }
        }
    }

    /// Handles one session-control message: a feed starting or stopping, or a
    /// question about the session's own orders.
    fn apply_control(&mut self, id: u64, command: &Command) {
        let account = self
            .sessions
            .get(&id)
            .and_then(|session| session.account)
            .unwrap_or_default();
        if command.kind() == Some(CommandKind::QueryOpenOrders) {
            self.answer_open_orders(id, command.symbol, account);
            return;
        }
        if command.kind() == Some(CommandKind::CancelOnDisconnect) {
            if let Some(session) = self.sessions.get_mut(&id) {
                session.cancel_on_disconnect = command.quantity != 0;
            }
            return;
        }
        let Some(kind) = command.channel_kind() else {
            return;
        };
        let channel = Channel::requested(kind, command.symbol, account);

        if command.kind() == Some(CommandKind::Subscribe) {
            let from = self.venue.subscribe(channel);
            let Some(session) = self.sessions.get_mut(&id) else {
                return;
            };
            if session.cursors.iter().any(|(held, _)| *held == channel) {
                return;
            }
            session.cursors.push((channel, from));
            if let Channel::Book(symbol) = channel {
                self.send_book_state(id, symbol, from);
            }
        } else if let Some(session) = self.sessions.get_mut(&id) {
            session.cursors.retain(|(held, _)| *held != channel);
        }
    }

    /// Tells one session what it still has working on a symbol.
    ///
    /// Sent on the session's own stream, since its orders are private to it.
    fn answer_open_orders(&mut self, id: u64, symbol: SymbolId, account: AccountId) {
        let orders = self.venue.exchange().open_orders_for(account, symbol);
        if orders.is_empty() {
            return;
        }
        let events: Vec<Event> = orders
            .iter()
            .map(|resting| bx_pipeline::order_state(account, symbol, resting))
            .collect();
        if let Some(session) = self.sessions.get(&id) {
            let _ = session.outbound.send((Channel::Account(account), events));
        }
    }

    /// States the book's current levels to one session.
    ///
    /// Increments alone cannot build a book: a subscriber has no idea what was
    /// resting before it arrived. `at` is the sequence the increments resume
    /// from, taken in the same pass, so state and change cannot disagree.
    fn send_book_state(&mut self, id: u64, symbol: SymbolId, at: Sequence) {
        let Some(book) = self.venue.book(symbol) else {
            return;
        };
        let levels: Vec<Event> = [Side::Bid, Side::Ask]
            .into_iter()
            .flat_map(|side| {
                book.depth(side, usize::MAX)
                    .into_iter()
                    .map(move |(price, quantity)| (side, price, quantity))
            })
            .map(|(side, price, quantity)| book_state_event(at, symbol, side, price, quantity))
            .collect();

        if let Some(session) = self.sessions.get(&id)
            && !levels.is_empty()
        {
            let _ = session.outbound.send((Channel::Book(symbol), levels));
        }
    }

    /// Sends each session whatever it has not yet seen, per channel.
    fn publish(&mut self) {
        let mut restate = Vec::new();
        for (id, session) in &mut self.sessions {
            for (channel, cursor) in &mut session.cursors {
                self.scratch.clear();
                match self.venue.resume(*channel, *cursor, &mut self.scratch) {
                    Resume::Delivered { next } => {
                        *cursor = next;
                        if !self.scratch.is_empty() {
                            // A full queue means the client is not keeping up on
                            // this channel; the send is dropped rather than
                            // stalling the venue. Its own streams are unaffected,
                            // which is the point of a stream per channel.
                            let _ = session.outbound.send((*channel, self.scratch.clone()));
                        }
                    }
                    // Outside the retention window: a book can be restated, and
                    // that is handled once the borrow ends.
                    Resume::Lagged { .. } => restate.push((*id, *channel)),
                    Resume::NotSubscribed => {}
                }
            }
        }
        for (id, channel) in restate {
            if let Channel::Book(symbol) = channel {
                let at = self.venue.hub().next_sequence(channel).unwrap_or_default();
                if let Some(session) = self.sessions.get_mut(&id) {
                    for (held, cursor) in &mut session.cursors {
                        if *held == channel {
                            *cursor = at;
                        }
                    }
                }
                self.send_book_state(id, symbol, at);
            } else if let Some(session) = self.sessions.get_mut(&id) {
                let at = self.venue.hub().next_sequence(channel).unwrap_or_default();
                for (held, cursor) in &mut session.cursors {
                    if *held == channel {
                        *cursor = at;
                    }
                }
            }
        }
    }
}

fn book_state_event(
    at: Sequence,
    symbol: SymbolId,
    side: Side,
    price: Ticks,
    quantity: u64,
) -> Event {
    Event {
        sequence: at,
        cause_sequence: 0,
        account: 0,
        order_id: 0,
        counterparty_order_id: 0,
        quantity,
        price,
        symbol,
        kind: bx_protocol::EventKind::BookSnapshot as u8,
        side: side as u8,
        reject_reason: 0,
        _pad: [0; 1],
    }
}

/// Accepts without awaiting, so the venue thread never parks in the runtime.
fn futures_lite_poll(endpoint: &Endpoint) -> Option<quinn::Incoming> {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let mut accept = std::pin::pin!(endpoint.accept());
    let mut context = Context::from_waker(Waker::noop());
    match accept.as_mut().poll(&mut context) {
        Poll::Ready(incoming) => incoming,
        Poll::Pending => None,
    }
}

/// Serves one connection: order entry on the bidirectional stream the client
/// opens, and one server-initiated stream per market-data channel.
async fn serve(
    incoming: quinn::Incoming,
    id: u64,
    to_venue: SyncSender<(u64, FromClient)>,
    mut from_venue: UnboundedReceiver<ToClient>,
) {
    let Ok(connection) = incoming.await else {
        let _ = to_venue.send((id, FromClient::Gone));
        return;
    };
    let Ok((acks, mut orders)) = connection.accept_bi().await else {
        let _ = to_venue.send((id, FromClient::Gone));
        return;
    };

    let reader = {
        let to_venue = to_venue.clone();
        tokio::spawn(async move {
            let mut decoder = Decoder::new(256);
            let mut commands = Vec::new();
            loop {
                let writable = decoder.writable();
                if writable.is_empty() {
                    // Nothing can be decoded until the venue drains; yield rather
                    // than spin.
                    tokio::task::yield_now().await;
                    continue;
                }
                match orders.read(writable).await {
                    Ok(Some(bytes)) => {
                        decoder.advance(bytes);
                        commands.clear();
                        decoder.drain(&mut commands);
                        for command in commands.drain(..) {
                            // Blocking on a full queue is deliberate
                            // back-pressure: it slows the client producing the
                            // load instead of growing a queue without limit.
                            if to_venue.send((id, FromClient::Command(command))).is_err() {
                                return;
                            }
                        }
                    }
                    Ok(None) | Err(_) => break,
                }
            }
            // Whoever notices the connection has gone must say so. The venue
            // frees the session only on this message, and the writer below is
            // parked waiting for the venue to do exactly that -- so leaving it to
            // the writer deadlocks the pair and the session is never reaped.
            let _ = to_venue.send((id, FromClient::Gone));
        })
    };

    // A writer task per stream, each with its own queue.
    //
    // One loop writing every stream in turn would put head-of-line blocking back
    // at the application level: a feed stalled on flow control would hold up the
    // acknowledgements queued behind it, which is precisely the failure separate
    // streams exist to prevent. Dispatching to a task per stream means the
    // dispatcher never awaits a write.
    let mut writers: HashMap<Channel, tokio::sync::mpsc::Sender<Vec<u8>>> = HashMap::new();
    let (ack_queue, ack_receiver) = tokio::sync::mpsc::channel(QUEUED_BATCHES);
    tokio::spawn(write_stream(acks, ack_receiver));

    let mut bytes = Vec::new();
    loop {
        // Ends when the venue drops the session *or* when the peer goes, rather
        // than only the former: waiting solely on the venue is half of a circular
        // wait, since the venue is waiting to be told the peer has gone.
        let next = tokio::select! {
            queued = from_venue.recv() => queued,
            _ = connection.closed() => None,
        };
        let Some((channel, events)) = next else {
            break;
        };
        bytes.clear();
        for event in &events {
            encode(event, &mut bytes);
        }

        // A session's own events are its acknowledgements, so they belong on the
        // stream it is already reading.
        let queue = if matches!(channel, Channel::Account(_)) {
            ack_queue.clone()
        } else if let Some(held) = writers.get(&channel) {
            held.clone()
        } else {
            // Opened on first use, so a session costs streams only for the
            // channels it actually follows.
            let Ok(stream) = connection.open_uni().await else {
                break;
            };
            let (queue, receiver) = tokio::sync::mpsc::channel(QUEUED_BATCHES);
            tokio::spawn(write_stream(stream, receiver));
            writers.insert(channel, queue.clone());
            queue
        };
        // Dropped rather than awaited when a client is not keeping up on this
        // channel. Blocking here would stall the whole session; a book that falls
        // behind is restated on the next pass, which is cheaper than either.
        if queue.try_send(std::mem::take(&mut bytes)).is_err() && queue.is_closed() {
            break;
        }
    }

    reader.abort();
    let _ = to_venue.send((id, FromClient::Gone));
}

/// Drives one stream, writing whatever its queue hands over.
async fn write_stream(
    mut stream: quinn::SendStream,
    mut queue: tokio::sync::mpsc::Receiver<Vec<u8>>,
) {
    while let Some(bytes) = queue.recv().await {
        if stream.write_all(&bytes).await.is_err() {
            return;
        }
    }
}

/// Reads whole records from a QUIC stream. Client-side helper for tests and
/// tooling.
///
/// # Errors
/// Returns the underlying stream error.
pub async fn read_events(
    stream: &mut quinn::RecvStream,
    want: usize,
    out: &mut Vec<Event>,
) -> io::Result<()> {
    let mut buffer = vec![0_u8; want.max(1) * FRAME_LEN];
    let mut filled = 0;
    while out.len() < want {
        match stream.read(&mut buffer[filled..]).await {
            Ok(Some(bytes)) => {
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
            Ok(None) => break,
            Err(e) => return Err(io::Error::other(e.to_string())),
        }
    }
    Ok(())
}
