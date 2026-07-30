//! Public market data, distributed by a thread that is not the venue's.
//!
//! One canonical event stream becomes thousands of downstream streams without
//! the matching thread doing any of the work. The venue hands each pass's
//! events to a [`Handoff`] once; this drains that, keeps its own retention
//! rings, and does the per-subscriber copying and writing on its own thread and
//! its own port.
//!
//! ## Why a separate port rather than the trading session
//!
//! Because that is the split every venue converges on -- Nasdaq runs OUCH for
//! order entry and ITCH for market data -- and the reason is visible in our own
//! measurements: a thousand subscribers on the trading path moved the venue's
//! round trip from 17.5 microseconds to 14.3 milliseconds. Order entry is a
//! conversation with one participant and must stay short. A feed is a broadcast
//! to an audience that has no business being in that path.
//!
//! ## Why this is not the gateway again
//!
//! A feed session is a strictly smaller thing than a trading session, and the
//! difference is the point. There is no logon, because nothing here is private
//! -- these are the public channels, and a client that can reach the port may
//! read them. There is no rate limit, no balance, no account binding and no
//! risk, because nothing a subscriber sends can move the book: the only
//! messages accepted are subscribe, unsubscribe and resume. What *is* shared is
//! shared outright rather than reimplemented -- the retention rings, the
//! sequence numbering and the resume semantics are [`Hub`], the same type the
//! venue uses, and the framing is the same decoder.
//!
//! ## Incremental only, deliberately
//!
//! This carries the increments, in order, numbered. It does not state a book to
//! a client that joins mid-stream: that is a snapshot service's job, and
//! keeping the two apart is what lets the fast path stay a stream of deltas
//! nobody has to interrupt. A subscriber joins, notes the first sequence it
//! received, and reconciles against a snapshot taken at or before it.

use crate::codec::{Decoder, encode};
use crate::handoff::Handoff;
use bx_pipeline::hub::{Channel, Hub, Resume};
use bx_protocol::{Command, CommandKind, Event, Sequence};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use std::io::{self, ErrorKind, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// The listener's token. Subscribers take `index + 1`.
const LISTENER: Token = Token(0);

/// How long a pass waits for something to happen when nothing has.
///
/// A market-data thread is not a matching thread: it may sleep. Busy-polling
/// here would take a core away from the venue to answer a feed that measures
/// its deadlines in microseconds rather than nanoseconds.
const IDLE: Duration = Duration::from_micros(200);

/// Records a subscriber may send in one pass. Subscriptions, not orders: a
/// client with more than this to say is not subscribing.
const MOST_REQUESTS: usize = 64;

/// One subscriber.
#[derive(Debug)]
struct Listener {
    stream: TcpStream,
    decoder: Decoder,
    outbox: Vec<u8>,
    /// Channels followed, and where each has been read to.
    cursors: Vec<(Channel, Sequence)>,
    open: bool,
}

impl Listener {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            decoder: Decoder::new(MOST_REQUESTS),
            outbox: Vec::new(),
            cursors: Vec::new(),
            open: true,
        }
    }

    /// Pushes what the socket will take; the rest stays queued.
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

/// Counters an operator reads without touching the thread that keeps them.
#[derive(Debug, Default)]
pub struct Counts {
    pub subscribers: AtomicU64,
    pub shed: AtomicU64,
    pub events: AtomicU64,
    pub batches: AtomicU64,
}

/// The market-data distributor: a thread, a port, and the rings behind them.
#[derive(Debug)]
pub struct Feed {
    stop: Arc<AtomicBool>,
    counts: Arc<Counts>,
    bound: std::net::SocketAddr,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Feed {
    /// Binds the feed port and starts distributing.
    ///
    /// `retained_per_channel` and `max_outbox` are the same bounds the venue
    /// uses, and mean the same thing: how far a subscriber may fall behind
    /// before it is restated, and how much the feed will hold for one before it
    /// gives up on it.
    ///
    /// # Errors
    /// Fails if the address cannot be parsed or bound.
    pub fn start(
        address: &str,
        handoff: Handoff,
        retained_per_channel: usize,
        max_outbox: usize,
    ) -> io::Result<Self> {
        let parsed = address
            .parse()
            .map_err(|e| io::Error::new(ErrorKind::InvalidInput, format!("{address}: {e}")))?;
        let mut listener = TcpListener::bind(parsed)?;
        let bound = listener.local_addr()?;
        let poll = Poll::new()?;
        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)?;

        let stop = Arc::new(AtomicBool::new(false));
        let counts = Arc::new(Counts::default());
        let flag = Arc::clone(&stop);
        let kept = Arc::clone(&counts);
        let thread = std::thread::spawn(move || {
            let mut state = State {
                poll,
                listener,
                events: Events::with_capacity(256),
                hub: Hub::new(retained_per_channel),
                listeners: Vec::new(),
                free: Vec::new(),
                batches: Vec::new(),
                handoff,
                max_outbox,
                counts: kept,
            };
            while !flag.load(Ordering::Relaxed) {
                state.pass();
            }
        });
        Ok(Self {
            stop,
            counts,
            bound,
            thread: Some(thread),
        })
    }

    /// Where the feed ended up. Useful when the port was left to the OS.
    #[must_use]
    pub const fn address(&self) -> std::net::SocketAddr {
        self.bound
    }

    #[must_use]
    pub fn counts(&self) -> &Counts {
        &self.counts
    }
}

impl Drop for Feed {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Everything the feed thread owns. Nothing here is shared with the venue
/// except the handoff, which is the point.
#[derive(Debug)]
struct State {
    poll: Poll,
    listener: TcpListener,
    events: Events,
    hub: Hub,
    listeners: Vec<Option<Listener>>,
    free: Vec<usize>,
    batches: Vec<Vec<Event>>,
    handoff: Handoff,
    max_outbox: usize,
    counts: Arc<Counts>,
}

impl State {
    /// One pass: take what the venue published, accept, read requests, write.
    fn pass(&mut self) {
        // Waiting here rather than spinning, and the timeout is what bounds how
        // long a batch sits when no socket is doing anything.
        if self.poll.poll(&mut self.events, Some(IDLE)).is_ok() {
            let ready: Vec<Token> = self.events.iter().map(mio::event::Event::token).collect();
            for token in ready {
                if token == LISTENER {
                    self.accept();
                } else {
                    self.read_requests(token.0 - 1);
                }
            }
        }
        self.distribute();
        self.write_and_shed();
    }

    fn accept(&mut self) {
        while let Ok((mut stream, _)) = self.listener.accept() {
            let index = self.free.pop().unwrap_or(self.listeners.len());
            if self
                .poll
                .registry()
                .register(&mut stream, Token(index + 1), Interest::READABLE)
                .is_err()
            {
                self.free.push(index);
                continue;
            }
            let _ = stream.set_nodelay(true);
            let listener = Listener::new(stream);
            if index == self.listeners.len() {
                self.listeners.push(Some(listener));
            } else {
                self.listeners[index] = Some(listener);
            }
            self.counts.subscribers.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Subscribe, unsubscribe, resume. Anything else is ignored rather than
    /// answered: this port has no opinion about orders.
    fn read_requests(&mut self, index: usize) {
        let mut requests = Vec::new();
        let mut undecodable = 0;
        {
            let Some(listener) = self.listeners.get_mut(index).and_then(Option::as_mut) else {
                return;
            };
            loop {
                if listener.decoder.writable().is_empty() {
                    break;
                }
                match listener.stream.read(listener.decoder.writable()) {
                    Ok(0) => {
                        listener.open = false;
                        break;
                    }
                    Ok(bytes) => {
                        listener.decoder.advance(bytes);
                        listener.decoder.drain(&mut requests, &mut undecodable);
                    }
                    Err(e)
                        if e.kind() == ErrorKind::WouldBlock
                            || e.kind() == ErrorKind::Interrupted =>
                    {
                        break;
                    }
                    Err(_) => {
                        listener.open = false;
                        break;
                    }
                }
            }
        }
        for request in requests {
            self.apply(index, &request);
        }
    }

    fn apply(&mut self, index: usize, request: &Command) {
        let Some(kind) = request.channel_kind() else {
            return;
        };
        // Account zero: the private feed is not served here and naming one must
        // not be a way to ask for somebody else's. `Channel::requested` folds a
        // private request onto account zero, which nothing publishes to.
        let channel = Channel::requested(kind, request.symbol, 0);
        if matches!(channel, Channel::Account(_)) {
            return;
        }
        match request.kind() {
            Some(CommandKind::Subscribe) => {
                let from = self.hub.subscribe(channel);
                let Some(listener) = self.listeners.get_mut(index).and_then(Option::as_mut) else {
                    return;
                };
                if !listener.cursors.iter().any(|(held, _)| *held == channel) {
                    listener.cursors.push((channel, from));
                }
            }
            Some(CommandKind::Unsubscribe) => {
                if let Some(listener) = self.listeners.get_mut(index).and_then(Option::as_mut) {
                    listener.cursors.retain(|(held, _)| *held != channel);
                }
            }
            Some(CommandKind::Resume) => {
                let now = self.hub.subscribe(channel);
                let asked = request.order_id;
                let Some(listener) = self.listeners.get_mut(index).and_then(Option::as_mut) else {
                    return;
                };
                let at = asked.min(now);
                if let Some(entry) = listener
                    .cursors
                    .iter_mut()
                    .find(|(held, _)| *held == channel)
                {
                    entry.1 = at;
                } else {
                    listener.cursors.push((channel, at));
                }
            }
            _ => {}
        }
    }

    /// Takes what the venue published and copies it to whoever is following.
    fn distribute(&mut self) {
        self.batches.clear();
        self.handoff.take(&mut self.batches);
        if self.batches.is_empty() {
            return;
        }
        let mut batches = std::mem::take(&mut self.batches);
        for batch in &batches {
            self.counts
                .events
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
            self.counts.batches.fetch_add(1, Ordering::Relaxed);
            self.hub.publish(batch);
            // Written per batch rather than per pass: a batch is one of the
            // venue's groups, and its subscribers must see it whole before the
            // next one overwrites the ring behind them.
            self.fan_out();
        }
        for batch in batches.drain(..) {
            self.handoff.recycle(batch);
        }
        self.batches = batches;
    }

    /// The copy that used to happen on the trading thread.
    fn fan_out(&mut self) {
        let touched: Vec<Channel> = self.hub.touched().to_vec();
        for channel in touched {
            for index in 0..self.listeners.len() {
                let Some(listener) = self.listeners[index].as_mut() else {
                    continue;
                };
                let Some(cursor) = listener
                    .cursors
                    .iter_mut()
                    .find(|(held, _)| *held == channel)
                    .map(|(_, cursor)| cursor)
                else {
                    continue;
                };
                match self
                    .hub
                    .resume_bytes(channel, *cursor, &mut listener.outbox)
                {
                    Resume::Delivered { next } => *cursor = next,
                    // Behind the window, or holding a position this run never
                    // issued. There is no book to restate on an incremental
                    // feed, so it is placed at the front and the gap in its
                    // sequence numbers tells it to reconcile against a
                    // snapshot.
                    Resume::Lagged { .. } | Resume::Ahead { .. } => {
                        *cursor = self.hub.next_sequence(channel).unwrap_or_default();
                    }
                    Resume::NotSubscribed => {}
                }
            }
        }
    }

    fn write_and_shed(&mut self) {
        for index in 0..self.listeners.len() {
            let Some(listener) = self.listeners[index].as_mut() else {
                continue;
            };
            if !listener.outbox.is_empty() {
                listener.flush();
            }
            // The venue neither slows down for a subscriber nor accumulates for
            // one. Past its budget it is gone, and it reconnects.
            let shed = listener.outbox.len() > self.max_outbox;
            if shed {
                self.counts.shed.fetch_add(1, Ordering::Relaxed);
            }
            if !listener.open || shed {
                if let Some(mut gone) = self.listeners[index].take() {
                    let _ = self.poll.registry().deregister(&mut gone.stream);
                }
                self.free.push(index);
                self.counts.subscribers.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

/// Frames one event the way a subscriber reads it. Used by tests and by
/// anything that needs the feed's wire form without the feed.
pub fn framed(event: &Event) -> Vec<u8> {
    let mut out = Vec::new();
    encode(event, &mut out);
    out
}
