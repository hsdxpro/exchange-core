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
use crate::multicast::Multicast;
use bx_pipeline::fastmap::FastMap;
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
    /// On the owing list. The list is a hint; this is the truth.
    owes: bool,
    open: bool,
}

impl Listener {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            decoder: Decoder::new(MOST_REQUESTS),
            outbox: Vec::new(),
            cursors: Vec::new(),
            owes: false,
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
        multicast: Option<Multicast>,
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
                by_channel: FastMap::default(),
                touched: Vec::new(),
                owing: Vec::new(),
                sent_to: FastMap::default(),
                retaining: FastMap::default(),
                batches: Vec::new(),
                handoff,
                max_outbox,
                counts: kept,
                multicast,
                outgoing: Vec::with_capacity(1_024),
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

    /// The feed's counters in Prometheus exposition format, for appending to
    /// the venue's.
    ///
    /// These were kept and shown to nobody until this existed, which is the
    /// worse half of the bargain: the work of maintaining them without the
    /// benefit. Two of them are the only warning an operator gets that
    /// distribution, rather than matching, is the thing falling behind --
    /// `bx_feed_batches_dropped_total` is the venue finding no free buffer to
    /// hand a group over in, and a subscriber sees those as a gap in its
    /// sequence.
    #[must_use]
    pub fn prometheus(&self, handoff: &Handoff) -> String {
        use std::fmt::Write;
        let (batches_dropped, events_dropped) = handoff.dropped();
        let mut out = String::with_capacity(1_024);
        for (name, help, kind, value) in [
            (
                "bx_feed_subscribers",
                "Market-data subscribers connected.",
                "gauge",
                self.counts.subscribers.load(Ordering::Relaxed),
            ),
            (
                "bx_feed_shed_total",
                "Subscribers dropped for owing more than their budget allows.",
                "counter",
                self.counts.shed.load(Ordering::Relaxed),
            ),
            (
                "bx_feed_events_total",
                "Events taken from the venue and distributed.",
                "counter",
                self.counts.events.load(Ordering::Relaxed),
            ),
            (
                "bx_feed_batches_total",
                "Groups taken from the venue.",
                "counter",
                self.counts.batches.load(Ordering::Relaxed),
            ),
            (
                "bx_feed_batches_dropped_total",
                "Groups the venue could not hand over because the feed was behind.",
                "counter",
                batches_dropped,
            ),
            (
                "bx_feed_events_dropped_total",
                "Events inside those groups, which is how much a subscriber missed.",
                "counter",
                events_dropped,
            ),
        ] {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} {kind}");
            let _ = writeln!(out, "{name} {value}");
        }
        out
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
    /// Who follows what. Without it a pass asks every subscriber about every
    /// channel that moved, which is the cost the venue removed from its own
    /// loop and which this reintroduced by not copying it: at a thousand
    /// subscribers a one-symbol group walked a thousand sessions to find the
    /// handful that cared.
    by_channel: FastMap<Channel, Vec<usize>>,
    /// Channels that moved this pass, accumulated across every batch drained,
    /// so subscribers are walked once however many groups arrived at once.
    /// Reused rather than rebuilt, because a per-pass allocation on a feed is a
    /// per-pass allocation.
    touched: Vec<Channel>,
    /// Subscribers holding bytes. An idle one is never visited.
    owing: Vec<usize>,
    /// How far the multicast feed has packetised each channel.
    sent_to: FastMap<Channel, Sequence>,
    /// Public channels this feed has started retaining. Bounded by the listing,
    /// not by the audience, which is what makes a repair answerable.
    retaining: FastMap<Channel, ()>,
    batches: Vec<Vec<Event>>,
    handoff: Handoff,
    max_outbox: usize,
    counts: Arc<Counts>,
    /// The same events as one packet per group rather than one copy per
    /// subscriber, when the venue is configured to send them. Driven from this
    /// thread's single drain, so multicast costs the venue nothing it was not
    /// already paying.
    multicast: Option<Multicast>,
    /// Scratch for reading a channel's new events out of the ring before they
    /// are packetised. Reused; a send path that allocates is one that pauses
    /// for the allocator while a market moves.
    outgoing: Vec<Event>,
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
                    self.by_channel.entry(channel).or_default().push(index);
                }
                self.deliver(index, channel);
            }
            Some(CommandKind::Unsubscribe) => {
                if let Some(listener) = self.listeners.get_mut(index).and_then(Option::as_mut) {
                    listener.cursors.retain(|(held, _)| *held != channel);
                }
                if let Some(following) = self.by_channel.get_mut(&channel) {
                    following.retain(|held| *held != index);
                }
            }
            Some(CommandKind::Resume) => {
                let now = self.hub.subscribe(channel);
                let asked = request.order_id;
                let fresh = {
                    let Some(listener) = self.listeners.get_mut(index).and_then(Option::as_mut)
                    else {
                        return;
                    };
                    // Never past the live edge: a cursor from a previous run of
                    // the venue names a sequence this one has not reached, and
                    // waiting for it would be waiting forever.
                    let at = asked.min(now);
                    match listener
                        .cursors
                        .iter_mut()
                        .find(|(held, _)| *held == channel)
                    {
                        Some(entry) => {
                            entry.1 = at;
                            false
                        }
                        None => {
                            listener.cursors.push((channel, at));
                            true
                        }
                    }
                };
                if fresh {
                    self.by_channel.entry(channel).or_default().push(index);
                }
                // Served now rather than when the channel next moves. A repair
                // is asked for precisely because nothing is arriving, so
                // waiting for the next event would answer a receiver's question
                // with the silence it complained about.
                self.deliver(index, channel);
            }
            _ => {}
        }
    }

    /// Takes what the venue published and copies it to whoever is following.
    ///
    /// Every batch waiting is published first and the audience walked once
    /// afterwards, rather than once per batch. The rings hold the whole pass,
    /// and a cursor read after the last publish delivers everything since the
    /// subscriber's position in one copy -- so arriving behind by ten groups
    /// costs one walk rather than ten. Retention is the only bound on that, and
    /// a subscriber whose channel wrapped is repositioned exactly as it would
    /// have been either way.
    fn distribute(&mut self) {
        self.batches.clear();
        let mut batches = std::mem::take(&mut self.batches);
        self.handoff.take(&mut batches);
        if batches.is_empty() {
            self.batches = batches;
            return;
        }
        let mut touched = std::mem::take(&mut self.touched);
        touched.clear();
        for batch in &batches {
            self.counts
                .events
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
            self.counts.batches.fetch_add(1, Ordering::Relaxed);
            // Retain every public channel the venue produces, whether or not
            // anybody is watching it yet. The venue's own hub keeps only what
            // has been subscribed to, which is right there -- it bounds feed
            // memory to the audience. It is wrong here: a receiver asking for a
            // repair asks *after* losing packets, and a ring created at that
            // moment is empty. A distributor that can only replay what somebody
            // was already watching cannot recover anybody.
            for event in batch {
                let Some(channel) = Channel::of(event) else {
                    continue;
                };
                if crate::multicast::wire_channel(channel).is_some()
                    && self.retaining.insert(channel, ()).is_none()
                {
                    self.hub.subscribe(channel);
                }
            }
            self.hub.publish(batch);
            for channel in self.hub.touched() {
                // Small by construction -- the channels one group moved -- so a
                // scan beats hashing, and it allocates nothing.
                if !touched.contains(channel) {
                    touched.push(*channel);
                }
            }
        }
        self.fan_out(&touched);
        self.emit(&touched);
        self.touched = touched;
        for batch in batches.drain(..) {
            self.handoff.recycle(batch);
        }
        self.batches = batches;
    }

    /// The copy that used to happen on the trading thread.
    ///
    /// Visits the subscribers of a moved channel, not every subscriber. The
    /// difference is the whole cost of an audience: with a thousand watching one
    /// symbol, the index turns a thousand-session walk per group into a walk of
    /// the sessions that asked.
    fn fan_out(&mut self, touched: &[Channel]) {
        for channel in touched {
            let Some(following) = self.by_channel.get(channel) else {
                continue;
            };
            for index in following.clone() {
                self.deliver(index, *channel);
            }
        }
    }

    /// Copies whatever one subscriber has not yet had of one channel.
    ///
    /// Shared by the live path and by subscribe and resume, and that sharing is
    /// the point rather than tidiness. Delivery used to happen only for channels
    /// that moved *this pass*, so a client asking to resume a quiet channel set
    /// a cursor and then waited for a trade to shake its own backlog loose --
    /// which on a repair request is precisely the wrong answer, since a receiver
    /// asks exactly when it has stopped hearing anything.
    fn deliver(&mut self, index: usize, channel: Channel) {
        let Some(listener) = self.listeners.get_mut(index).and_then(Option::as_mut) else {
            return;
        };
        let Some(cursor) = listener
            .cursors
            .iter_mut()
            .find(|(held, _)| *held == channel)
            .map(|(_, cursor)| cursor)
        else {
            return;
        };
        match self
            .hub
            .resume_bytes(channel, *cursor, &mut listener.outbox)
        {
            Resume::Delivered { next } => *cursor = next,
            // Behind the window, or holding a position this run never issued.
            // There is no book to restate on an incremental feed, so it is
            // placed at the front and the gap in its sequence numbers tells it
            // to reconcile against a snapshot.
            Resume::Lagged { .. } | Resume::Ahead { .. } => {
                *cursor = self.hub.next_sequence(channel).unwrap_or_default();
            }
            Resume::NotSubscribed => {}
        }
        if !listener.outbox.is_empty() && !listener.owes {
            listener.owes = true;
            self.owing.push(index);
        }
    }

    /// Puts the same events on the wire as packets, once, for everybody.
    ///
    /// Read straight out of the rings the TCP path reads, from the position the
    /// last packet reached: the multicast feed keeps its own cursor per channel
    /// rather than sharing a subscriber's, because it is not a subscriber -- it
    /// has no socket to fall behind on and nobody to be shed for.
    fn emit(&mut self, touched: &[Channel]) {
        let Some(multicast) = self.multicast.as_mut() else {
            return;
        };
        for channel in touched {
            if crate::multicast::wire_channel(*channel).is_none() {
                continue;
            }
            let at = self.sent_to.entry(*channel).or_insert(0);
            self.outgoing.clear();
            match self.hub.resume(*channel, *at, &mut self.outgoing) {
                Resume::Delivered { next } => {
                    multicast.send(*channel, *at, &self.outgoing);
                    *at = next;
                }
                // The ring wrapped past where the feed had reached, which means
                // this thread fell far enough behind that packets were never
                // built for events now gone. Receivers see the gap in the
                // sequence and recover; the cursor moves to what is still
                // there rather than replaying what is not.
                Resume::Lagged { oldest } => {
                    if let Resume::Delivered { next } =
                        self.hub.resume(*channel, oldest, &mut self.outgoing)
                    {
                        multicast.send(*channel, oldest, &self.outgoing);
                        *at = next;
                    }
                }
                Resume::Ahead { next } => *at = next,
                Resume::NotSubscribed => {}
            }
        }
    }

    /// Writes to the subscribers holding bytes, and to nobody else.
    ///
    /// An idle audience member costs nothing per pass: it is not on the list.
    /// The flag rather than the list is the truth, because a slot freed this
    /// pass can be taken by a new subscriber on the next while the list still
    /// names it.
    fn write_and_shed(&mut self) {
        let mut owing = std::mem::take(&mut self.owing);
        let mut kept = 0;
        for at in 0..owing.len() {
            let index = owing[at];
            let Some(listener) = self.listeners.get_mut(index).and_then(Option::as_mut) else {
                continue;
            };
            if !listener.owes {
                continue;
            }
            listener.flush();
            // The venue neither slows down for a subscriber nor accumulates for
            // one. Past its budget it is gone, and it reconnects.
            let shed = listener.outbox.len() > self.max_outbox;
            if shed {
                self.counts.shed.fetch_add(1, Ordering::Relaxed);
            }
            if !listener.open || shed {
                self.drop_listener(index);
                continue;
            }
            if listener.outbox.is_empty() {
                listener.owes = false;
            } else {
                owing[kept] = index;
                kept += 1;
            }
        }
        owing.truncate(kept);
        self.owing = owing;
        // Subscribers that closed without owing anything are noticed here,
        // which is the only pass that looks at them at all.
        for index in 0..self.listeners.len() {
            if self.listeners[index]
                .as_ref()
                .is_some_and(|listener| !listener.open)
            {
                self.drop_listener(index);
            }
        }
    }

    /// Forgets a subscriber, and unhooks it from every channel index.
    ///
    /// Leaving a stale index behind would be worse than a leak: the slot is
    /// reused, so the next subscriber would inherit a feed it never asked for.
    fn drop_listener(&mut self, index: usize) {
        let Some(mut gone) = self.listeners[index].take() else {
            return;
        };
        let _ = self.poll.registry().deregister(&mut gone.stream);
        for (channel, _) in &gone.cursors {
            if let Some(following) = self.by_channel.get_mut(channel) {
                following.retain(|held| *held != index);
            }
        }
        self.free.push(index);
        self.counts.subscribers.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Frames one event the way a subscriber reads it. Used by tests and by
/// anything that needs the feed's wire form without the feed.
pub fn framed(event: &Event) -> Vec<u8> {
    let mut out = Vec::new();
    encode(event, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_feed_exports_the_counters_an_operator_pages_on() {
        let handoff = Handoff::new();
        let feed = Feed::start("127.0.0.1:0", handoff.clone(), 64, 4_096, None)
            .expect("the feed did not bind");
        let text = feed.prometheus(&handoff);

        // The two that matter most: distribution falling behind is invisible in
        // the venue's own counters, so if these are missing an operator learns
        // about a gapped feed from a client complaining.
        for name in [
            "bx_feed_subscribers",
            "bx_feed_shed_total",
            "bx_feed_events_total",
            "bx_feed_batches_total",
            "bx_feed_batches_dropped_total",
            "bx_feed_events_dropped_total",
        ] {
            assert!(text.contains(name), "{name} is not exported:\n{text}");
        }
        // Same shape rule the venue's exposition follows: every sample is
        // declared, and every value parses as a number.
        let mut declared: Vec<&str> = Vec::new();
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                declared.push(rest.split(' ').next().unwrap());
                continue;
            }
            if line.starts_with("# HELP ") {
                continue;
            }
            let (name, value) = line.rsplit_once(' ').expect("not a sample line");
            value.parse::<u64>().expect("value is not a number");
            assert!(declared.contains(&name), "{name} has no TYPE line");
        }
    }

    #[test]
    fn a_dropped_batch_shows_up_in_what_the_feed_reports() {
        // The counter that says the venue could not hand a group over. A
        // subscriber sees those as a sequence gap; without this an operator
        // sees nothing at all.
        let handoff = Handoff::new();
        let feed = Feed::start("127.0.0.1:0", handoff.clone(), 64, 4_096, None)
            .expect("feed did not bind");
        let event = [Event::default()];
        // Fill the seam without draining it, then overflow it.
        while handoff.offer(&event) {}
        assert!(
            feed.prometheus(&handoff)
                .contains("bx_feed_batches_dropped_total 1"),
            "a drop happened and the exposition did not say so"
        );
    }
}
