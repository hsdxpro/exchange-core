//! Subscription channels, and resuming one after a disconnect.
//!
//! A subscriber follows a [`Channel`] and receives every event on it, numbered
//! from zero without gaps. If the connection drops, it reconnects and asks to
//! resume from the last sequence it saw. The hub either hands back exactly the
//! events it missed, or tells it that it fell too far behind and must take a
//! fresh snapshot before it can follow again. There is no third answer, and in
//! particular there is no answer that quietly skips events — a subscriber that
//! is told it is caught up really is.
//!
//! Each channel owns a fixed-size ring. Publishing overwrites the oldest entry
//! rather than growing, so a subscriber that stops reading costs the venue a
//! bounded amount of memory and never the whole heap. That is the entire
//! back-pressure policy: the venue does not slow down for a slow client, and it
//! does not accumulate for one either.
//!
//! Rings are created by [`Hub::subscribe`] and live until [`Hub::unsubscribe`].
//! Publishing writes only to rings that already exist, so the command path
//! allocates nothing, and a venue with a hundred million accounts holds rings
//! only for the ones actually connected.

use crate::fastmap::FastMap;
use bx_protocol::{AccountId, ChannelKind, Event, EventKind, Sequence, SymbolId};
use zerocopy::IntoBytes;

/// What a subscriber can follow.
///
/// Every event belongs to exactly one channel, which is what lets each channel
/// number its own events contiguously.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Channel {
    /// Price-level changes for one symbol: the public depth feed.
    Book(SymbolId),
    /// Prints for one symbol: the public tape. Carries no identities.
    Trades(SymbolId),
    /// Top of book for one symbol: the cheapest public feed.
    Bbo(SymbolId),
    /// One account's own order lifecycle. Private to that account.
    Account(AccountId),
}

impl Channel {
    /// The channel a client asked for.
    ///
    /// `Account` deliberately ignores the requested account and uses the
    /// session's own: a client must not be able to subscribe to somebody else's
    /// private feed by naming their account number.
    #[must_use]
    pub const fn requested(
        kind: ChannelKind,
        symbol: SymbolId,
        session_account: AccountId,
    ) -> Self {
        match kind {
            ChannelKind::Book => Self::Book(symbol),
            ChannelKind::Trades => Self::Trades(symbol),
            ChannelKind::Bbo => Self::Bbo(symbol),
            ChannelKind::Account => Self::Account(session_account),
        }
    }

    /// The channel an event belongs on, or `None` if its kind does not decode.
    #[must_use]
    pub fn of(event: &Event) -> Option<Self> {
        Some(match event.kind()? {
            EventKind::BookDelta | EventKind::BookSnapshot => Self::Book(event.symbol),
            EventKind::Trade => Self::Trades(event.symbol),
            EventKind::Bbo => Self::Bbo(event.symbol),
            EventKind::Received
            | EventKind::Rejected
            | EventKind::Resting
            | EventKind::Filled
            | EventKind::Canceled
            // A client's own order state is private to it, like its fills.
            | EventKind::OrderState => Self::Account(event.account),
            // Not published at all. A challenge and its acceptance belong to one
            // connection, and putting them on a retained channel would replay a
            // nonce to whoever subscribed next.
            EventKind::Challenge | EventKind::Authenticated => return None,
        })
    }
}

/// The outcome of a resume request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Resume {
    /// Everything from the requested position was still held and has been
    /// delivered. `next` is the sequence to ask for after this.
    Delivered { next: Sequence },
    /// The requested position has already been overwritten. The subscriber
    /// missed events that no longer exist, so it must take a snapshot and
    /// rejoin at `oldest` or later. Nothing was delivered.
    Lagged { oldest: Sequence },
    /// Nobody is subscribed to that channel, so nothing is being retained.
    NotSubscribed,
}

/// A fixed-size window of the most recent events on one channel.
#[derive(Debug)]
struct Ring {
    slots: Box<[Event]>,
    /// Capacity is a power of two, so wrapping is a mask rather than a modulo.
    mask: u64,
    /// Sequence the next published event will take. Also the count published,
    /// since channels number from zero.
    next: Sequence,
    /// The publish this ring last received an event in.
    ///
    /// Compared against the hub's counter to answer "is this channel already in
    /// the touched list" in one comparison. Searching the list instead would be
    /// quadratic in the channels one group touches, and a group spanning a
    /// thousand instruments touches thousands.
    touched_in: u64,
}

impl Ring {
    fn new(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two();
        Self {
            slots: vec![Event::default(); capacity].into_boxed_slice(),
            mask: capacity as u64 - 1,
            next: 0,
            touched_in: 0,
        }
    }

    /// Oldest sequence still held. Everything before this has been overwritten.
    fn oldest(&self) -> Sequence {
        self.next.saturating_sub(self.slots.len() as u64)
    }

    fn push(&mut self, mut event: Event) {
        // Renumber into this channel's own sequence. The venue's global
        // numbering is shared across channels and so would look full of holes
        // to anyone following just one of them.
        event.sequence = self.next;
        self.slots[(self.next & self.mask) as usize] = event;
        self.next += 1;
    }

    fn read_from(&self, from: Sequence, out: &mut Vec<Event>) -> Resume {
        let oldest = self.oldest();
        if from < oldest {
            return Resume::Lagged { oldest };
        }
        // A client asking for a sequence past the end is simply up to date.
        let mut sequence = from.min(self.next);
        while sequence < self.next {
            out.push(self.slots[(sequence & self.mask) as usize]);
            sequence += 1;
        }
        Resume::Delivered { next: self.next }
    }

    /// The same, straight into a byte buffer.
    ///
    /// An event is a fixed 64 bytes with a declared layout, and the frame on the
    /// wire is exactly those bytes — so going through a `Vec<Event>` first meant
    /// copying every event twice on the one path that runs for every subscriber
    /// on every message. Nothing is written when the read fails, so a lagging
    /// subscriber's buffer is left exactly as it was.
    fn read_bytes_from(&self, from: Sequence, out: &mut Vec<u8>) -> Resume {
        let oldest = self.oldest();
        if from < oldest {
            return Resume::Lagged { oldest };
        }
        let mut sequence = from.min(self.next);
        // Reserved once rather than grown per event, so a subscriber catching up
        // on a window's worth does not walk the doubling.
        out.reserve((self.next - sequence) as usize * size_of::<Event>());
        while sequence < self.next {
            out.extend_from_slice(self.slots[(sequence & self.mask) as usize].as_bytes());
            sequence += 1;
        }
        Resume::Delivered { next: self.next }
    }
}

/// Fan-out to subscribed channels, with a bounded replay window on each.
#[derive(Debug)]
pub struct Hub {
    channels: FastMap<Channel, Ring>,
    retained_per_channel: usize,
    /// Channels that received an event in the last publish.
    ///
    /// This is what lets a gateway write to the sessions that have something
    /// waiting instead of asking every session about every channel it follows.
    /// That scan was the venue's ceiling under load: every pass paid for every
    /// connection whether or not anything had happened on it, so four thousand
    /// idle connections put about ninety microseconds in front of every order.
    touched: Vec<Channel>,
    /// Which publish this is, so a ring can say whether it is already listed
    /// without the list being searched.
    publishes: u64,
}

impl Hub {
    /// `retained_per_channel` is how many recent events each channel keeps for
    /// resume, rounded up to a power of two. It is the only tuning knob here
    /// and it is a straight trade: the window is how long a client may be
    /// disconnected before it needs a fresh snapshot, and the cost is 64 bytes
    /// per retained event per subscribed channel. It is a deployment decision,
    /// so there is no default.
    #[must_use]
    pub fn new(retained_per_channel: usize) -> Self {
        Self {
            channels: FastMap::default(),
            retained_per_channel,
            touched: Vec::new(),
            publishes: 0,
        }
    }

    /// Starts retaining a channel, and returns the sequence the subscriber will
    /// see first. Subscribing again is idempotent and does not disturb the
    /// window a reconnecting client is about to resume from.
    pub fn subscribe(&mut self, channel: Channel) -> Sequence {
        self.channels
            .entry(channel)
            .or_insert_with(|| Ring::new(self.retained_per_channel))
            .next
    }

    /// Stops retaining a channel and discards its window.
    pub fn unsubscribe(&mut self, channel: Channel) {
        self.channels.remove(&channel);
    }

    /// Routes a batch of venue events to whichever channels are subscribed.
    ///
    /// Events for channels nobody follows are dropped here rather than buffered
    /// on the chance that someone subscribes later.
    pub fn publish(&mut self, events: &[Event]) {
        self.touched.clear();
        self.publishes += 1;
        let publish = self.publishes;
        for event in events {
            let Some(channel) = Channel::of(event) else {
                continue;
            };
            if let Some(ring) = self.channels.get_mut(&channel) {
                ring.push(*event);
                // One comparison rather than a search of the list, which a group
                // spanning a thousand instruments would make quadratic.
                if ring.touched_in != publish {
                    ring.touched_in = publish;
                    self.touched.push(channel);
                }
            }
        }
    }

    /// Channels that received an event in the last [`Self::publish`].
    ///
    /// A gateway writes to the subscribers of these and to nobody else. Asking
    /// every session about every channel it follows is what made an idle
    /// connection cost anything at all.
    #[must_use]
    pub fn touched(&self) -> &[Channel] {
        &self.touched
    }

    /// Appends everything on `channel` from `from` onward to `out`.
    ///
    /// This is the reconnect path: a client sends the last sequence it
    /// processed plus one, and either gets the gap filled or is told to
    /// snapshot.
    pub fn resume(&self, channel: Channel, from: Sequence, out: &mut Vec<Event>) -> Resume {
        self.channels
            .get(&channel)
            .map_or(Resume::NotSubscribed, |ring| ring.read_from(from, out))
    }

    /// Everything a subscriber missed, written straight into its outbound bytes.
    ///
    /// What [`Self::resume`] does, without the intermediate `Vec<Event>`. A
    /// gateway wants bytes and the ring holds records that already *are* those
    /// bytes; the copy between them was work done once per event, per subscriber,
    /// forever.
    pub fn resume_bytes(&self, channel: Channel, from: Sequence, out: &mut Vec<u8>) -> Resume {
        self.channels
            .get(&channel)
            .map_or(Resume::NotSubscribed, |ring| {
                ring.read_bytes_from(from, out)
            })
    }

    /// Sequence the next event on `channel` will take.
    #[must_use]
    pub fn next_sequence(&self, channel: Channel) -> Option<Sequence> {
        self.channels.get(&channel).map(|ring| ring.next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bx_protocol::{RejectReason, Side};

    const RETAINED: usize = 8;

    fn event(kind: EventKind, symbol: SymbolId, account: AccountId) -> Event {
        Event {
            sequence: 999,
            cause_sequence: 0,
            account,
            order_id: 1,
            counterparty_order_id: 0,
            quantity: 10,
            price: 100,
            symbol,
            kind: kind as u8,
            side: Side::Bid as u8,
            reject_reason: RejectReason::None as u8,
            _pad: [0; 1],
        }
    }

    fn drain(hub: &Hub, channel: Channel, from: Sequence) -> (Resume, Vec<Event>) {
        let mut out = Vec::new();
        let resume = hub.resume(channel, from, &mut out);
        (resume, out)
    }

    #[test]
    fn a_client_cannot_subscribe_to_another_accounts_private_feed() {
        // The client asks for account 999's feed; it gets its own.
        let channel = Channel::requested(ChannelKind::Account, 1, 42);
        assert_eq!(channel, Channel::Account(42));
    }

    #[test]
    fn a_requested_public_channel_is_the_one_named() {
        assert_eq!(
            Channel::requested(ChannelKind::Book, 7, 42),
            Channel::Book(7)
        );
        assert_eq!(
            Channel::requested(ChannelKind::Trades, 7, 42),
            Channel::Trades(7)
        );
    }

    #[test]
    fn events_route_to_the_channel_that_owns_them() {
        assert_eq!(
            Channel::of(&event(EventKind::BookDelta, 7, 42)),
            Some(Channel::Book(7))
        );
        assert_eq!(
            Channel::of(&event(EventKind::Trade, 7, 42)),
            Some(Channel::Trades(7))
        );
        assert_eq!(
            Channel::of(&event(EventKind::Filled, 7, 42)),
            Some(Channel::Account(42))
        );
    }

    #[test]
    fn an_undecodable_event_is_dropped_rather_than_misrouted() {
        let mut broken = event(EventKind::Trade, 1, 1);
        broken.kind = 200;
        assert_eq!(Channel::of(&broken), None);

        let mut hub = Hub::new(RETAINED);
        hub.subscribe(Channel::Trades(1));
        hub.publish(&[broken]);
        assert_eq!(hub.next_sequence(Channel::Trades(1)), Some(0));
    }

    #[test]
    fn nothing_is_retained_for_a_channel_nobody_follows() {
        let mut hub = Hub::new(RETAINED);
        hub.publish(&[event(EventKind::Trade, 1, 1)]);
        assert_eq!(drain(&hub, Channel::Trades(1), 0).0, Resume::NotSubscribed);
    }

    #[test]
    fn each_channel_numbers_its_own_events_from_zero() {
        let mut hub = Hub::new(RETAINED);
        hub.subscribe(Channel::Book(1));
        hub.subscribe(Channel::Trades(1));
        // Interleaved, as the venue emits them.
        hub.publish(&[
            event(EventKind::BookDelta, 1, 0),
            event(EventKind::Trade, 1, 0),
            event(EventKind::BookDelta, 1, 0),
        ]);

        let (resume, book) = drain(&hub, Channel::Book(1), 0);
        assert_eq!(resume, Resume::Delivered { next: 2 });
        assert_eq!(
            book.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![0, 1],
            "a subscriber to one channel must see no holes"
        );
        let (_, trades) = drain(&hub, Channel::Trades(1), 0);
        assert_eq!(
            trades.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![0]
        );
    }

    #[test]
    fn resuming_delivers_exactly_what_was_missed() {
        let mut hub = Hub::new(RETAINED);
        hub.subscribe(Channel::Trades(1));
        for _ in 0..5 {
            hub.publish(&[event(EventKind::Trade, 1, 0)]);
        }
        // Client processed 0 and 1, then dropped.
        let (resume, missed) = drain(&hub, Channel::Trades(1), 2);
        assert_eq!(resume, Resume::Delivered { next: 5 });
        assert_eq!(
            missed.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
    }

    #[test]
    fn a_client_that_is_already_current_gets_nothing_and_no_error() {
        let mut hub = Hub::new(RETAINED);
        hub.subscribe(Channel::Trades(1));
        hub.publish(&[event(EventKind::Trade, 1, 0)]);
        let (resume, events) = drain(&hub, Channel::Trades(1), 1);
        assert_eq!(resume, Resume::Delivered { next: 1 });
        assert!(events.is_empty());
    }

    #[test]
    fn falling_behind_the_window_is_reported_not_silently_skipped() {
        let mut hub = Hub::new(RETAINED);
        hub.subscribe(Channel::Trades(1));
        for _ in 0..RETAINED + 3 {
            hub.publish(&[event(EventKind::Trade, 1, 0)]);
        }
        // Sequence 0 has been overwritten.
        let (resume, events) = drain(&hub, Channel::Trades(1), 0);
        assert_eq!(resume, Resume::Lagged { oldest: 3 });
        assert!(
            events.is_empty(),
            "a lagged subscriber must get nothing, not a partial gap"
        );
        // The oldest sequence it names really is still there.
        let (resume, events) = drain(&hub, Channel::Trades(1), 3);
        assert_eq!(resume, Resume::Delivered { next: 11 });
        assert_eq!(events.len(), 8);
    }

    #[test]
    fn the_window_holds_its_size_no_matter_how_much_is_published() {
        let mut hub = Hub::new(RETAINED);
        hub.subscribe(Channel::Trades(1));
        for _ in 0..10_000 {
            hub.publish(&[event(EventKind::Trade, 1, 0)]);
        }
        let (_, events) = drain(&hub, Channel::Trades(1), 0);
        assert!(events.is_empty(), "lagged, so nothing delivered");
        let (_, events) = drain(&hub, Channel::Trades(1), 10_000 - RETAINED as u64);
        assert_eq!(events.len(), RETAINED, "window grew or shrank");
    }

    #[test]
    fn resubscribing_does_not_disturb_a_window_being_resumed() {
        let mut hub = Hub::new(RETAINED);
        hub.subscribe(Channel::Trades(1));
        hub.publish(&[event(EventKind::Trade, 1, 0)]);
        assert_eq!(hub.subscribe(Channel::Trades(1)), 1, "window was reset");
        let (_, events) = drain(&hub, Channel::Trades(1), 0);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn unsubscribing_stops_retention() {
        let mut hub = Hub::new(RETAINED);
        hub.subscribe(Channel::Account(42));
        hub.publish(&[event(EventKind::Filled, 1, 42)]);
        assert!(hub.next_sequence(Channel::Account(42)).is_some());
        hub.unsubscribe(Channel::Account(42));
        assert!(hub.next_sequence(Channel::Account(42)).is_none());
        assert_eq!(
            drain(&hub, Channel::Account(42), 0).0,
            Resume::NotSubscribed
        );
    }

    #[test]
    fn one_accounts_events_never_reach_another() {
        let mut hub = Hub::new(RETAINED);
        hub.subscribe(Channel::Account(1));
        hub.subscribe(Channel::Account(2));
        hub.publish(&[
            event(EventKind::Filled, 1, 1),
            event(EventKind::Filled, 1, 1),
            event(EventKind::Filled, 1, 2),
        ]);
        assert_eq!(drain(&hub, Channel::Account(1), 0).1.len(), 2);
        let (_, theirs) = drain(&hub, Channel::Account(2), 0);
        assert_eq!(theirs.len(), 1);
        assert!(theirs.iter().all(|e| e.account == 2));
    }

    #[test]
    fn a_capacity_that_is_not_a_power_of_two_is_rounded_up() {
        let mut hub = Hub::new(5);
        hub.subscribe(Channel::Trades(1));
        for _ in 0..8 {
            hub.publish(&[event(EventKind::Trade, 1, 0)]);
        }
        let (resume, events) = drain(&hub, Channel::Trades(1), 0);
        assert_eq!(resume, Resume::Delivered { next: 8 });
        assert_eq!(events.len(), 8, "rounded 5 up to 8, so all 8 are held");
    }
}
