//! End-to-end tests for the subscription feed and reconnection.
//!
//! Nothing is fabricated. Simulated traders send real limit and market orders
//! through the real API, the real engine matches them, and the resulting events
//! are fanned out to channels. Clients here only ever see what the hub gives
//! them, and rebuild the book from those deltas alone. The assertion throughout
//! is that a client's reconstruction equals the venue's actual book — including
//! after it has been disconnected and missed events.

mod common;

use bx_journal::MemoryLog;
use bx_pipeline::hub::{Channel, Hub, Resume};
use bx_pipeline::{Exchange, limit_order, market_order};
use bx_protocol::{Command, Event, EventKind, Sequence, Side, Ticks};
use common::{SYMBOL, TraderPopulation, cancel, funded};
use std::collections::BTreeMap;

/// The venue as a client meets it: the exchange, plus the fan-out that feeds
/// subscribers.
struct Venue {
    exchange: Exchange<MemoryLog>,
    hub: Hub,
}

impl Venue {
    fn new(retained_per_channel: usize) -> Self {
        Self {
            exchange: funded(),
            hub: Hub::new(retained_per_channel),
        }
    }

    fn send(&mut self, mut command: Command) {
        let events = self.exchange.submit(&mut command).unwrap();
        self.hub.publish(events);
    }

    fn depth(&self, side: Side, limit: usize) -> Vec<(Ticks, u64)> {
        self.exchange.book(SYMBOL).unwrap().depth(side, limit)
    }
}

/// A market-data client. It knows only the events it has been handed, and it
/// pulls them the same way whether or not it was just disconnected — which is
/// the point of resuming by sequence rather than by timestamp.
#[derive(Debug, Default)]
struct DepthClient {
    bids: BTreeMap<Ticks, u64>,
    asks: BTreeMap<Ticks, u64>,
    /// The channel sequence this client expects next.
    next: Sequence,
    gaps: usize,
    snapshots: usize,
}

impl DepthClient {
    /// Pulls whatever is outstanding. Returns what the hub said.
    fn poll(&mut self, venue: &Venue, channel: Channel) -> Resume {
        let mut events = Vec::new();
        let outcome = venue.hub.resume(channel, self.next, &mut events);
        if let Resume::Delivered { .. } = outcome {
            self.consume(&events);
        }
        outcome
    }

    fn consume(&mut self, events: &[Event]) {
        for event in events {
            // A client detects loss by arithmetic, not by waiting.
            if event.sequence != self.next {
                self.gaps += 1;
            }
            self.next = event.sequence + 1;

            if event.kind != EventKind::BookDelta as u8 {
                continue;
            }
            let side = if event.side == Side::Bid as u8 {
                &mut self.bids
            } else {
                &mut self.asks
            };
            if event.quantity == 0 {
                side.remove(&event.price);
            } else {
                side.insert(event.price, event.quantity);
            }
        }
    }

    /// What a client does when told it fell behind: throw away its book, take
    /// the venue's current one, and rejoin the stream at the sequence that
    /// snapshot corresponds to.
    fn take_snapshot(&mut self, venue: &Venue, channel: Channel) {
        self.bids = venue.depth(Side::Bid, usize::MAX).into_iter().collect();
        self.asks = venue.depth(Side::Ask, usize::MAX).into_iter().collect();
        self.next = venue.hub.next_sequence(channel).unwrap();
        self.snapshots += 1;
    }

    fn depth(&self, side: Side, limit: usize) -> Vec<(Ticks, u64)> {
        match side {
            // Bids descend from the best price, asks ascend.
            Side::Bid => self
                .bids
                .iter()
                .rev()
                .map(|(p, q)| (*p, *q))
                .take(limit)
                .collect(),
            Side::Ask => self
                .asks
                .iter()
                .map(|(p, q)| (*p, *q))
                .take(limit)
                .collect(),
        }
    }

    fn assert_matches(&self, venue: &Venue, context: &str) {
        assert_eq!(
            self.depth(Side::Bid, usize::MAX),
            venue.depth(Side::Bid, usize::MAX),
            "{context}: client's bids diverged from the book"
        );
        assert_eq!(
            self.depth(Side::Ask, usize::MAX),
            venue.depth(Side::Ask, usize::MAX),
            "{context}: client's asks diverged from the book"
        );
        assert_eq!(self.gaps, 0, "{context}: client saw a gap");
    }
}

const RETAINED: usize = 8_192;
const BOOK: Channel = Channel::Book(SYMBOL);

#[test]
fn a_subscriber_rebuilds_the_book_from_the_channel_alone() {
    let mut venue = Venue::new(RETAINED);
    venue.hub.subscribe(BOOK);
    let mut client = DepthClient::default();

    venue.send(limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5));
    venue.send(limit_order(2, SYMBOL, 201, Side::Ask, 10_110, 4));
    venue.send(limit_order(1, SYMBOL, 102, Side::Bid, 10_090, 8));

    assert_eq!(client.poll(&venue, BOOK), Resume::Delivered { next: 3 });
    client.assert_matches(&venue, "steady state");
}

#[test]
fn a_client_that_reconnects_receives_exactly_what_it_missed() {
    let mut venue = Venue::new(RETAINED);
    venue.hub.subscribe(BOOK);
    let mut client = DepthClient::default();

    venue.send(limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5));
    client.poll(&venue, BOOK);
    client.assert_matches(&venue, "before the drop");
    let resumed_from = client.next;

    // The connection drops. The venue does not stop trading.
    venue.send(limit_order(2, SYMBOL, 201, Side::Ask, 10_110, 4));
    venue.send(limit_order(1, SYMBOL, 102, Side::Bid, 10_095, 3));
    venue.send(cancel(1, 101));
    venue.send(market_order(2, SYMBOL, 202, Side::Bid, 2));

    // It comes back and asks for everything since the last sequence it saw.
    let outcome = client.poll(&venue, BOOK);
    assert!(
        matches!(outcome, Resume::Delivered { .. }),
        "a client inside the window must be caught up, not told to snapshot"
    );
    assert!(
        client.next > resumed_from,
        "the reconnect delivered nothing at all"
    );
    client.assert_matches(&venue, "after reconnecting");
}

#[test]
fn a_client_gone_too_long_is_told_to_snapshot_rather_than_given_a_hole() {
    // A window small enough to overrun deliberately.
    let mut venue = Venue::new(4);
    venue.hub.subscribe(BOOK);
    let mut client = DepthClient::default();

    venue.send(limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5));
    client.poll(&venue, BOOK);

    // Gone long enough that its position is overwritten.
    for i in 0..20 {
        venue.send(limit_order(
            1,
            SYMBOL,
            200 + i,
            Side::Bid,
            10_000 + i as Ticks,
            1,
        ));
    }

    let outcome = client.poll(&venue, BOOK);
    let Resume::Lagged { oldest } = outcome else {
        panic!("expected to be told it lagged, got {outcome:?}");
    };
    assert!(oldest > client.next, "nothing was actually lost");
    assert_eq!(
        client.gaps, 0,
        "a lagged client must be given nothing, not a partial gap"
    );

    // It snapshots and rejoins, then keeps up.
    client.take_snapshot(&venue, BOOK);
    client.assert_matches(&venue, "immediately after snapshotting");

    venue.send(limit_order(2, SYMBOL, 301, Side::Ask, 10_150, 7));
    assert!(matches!(
        client.poll(&venue, BOOK),
        Resume::Delivered { .. }
    ));
    client.assert_matches(&venue, "after rejoining the stream");
}

#[test]
fn a_long_session_with_repeated_disconnects_still_converges() {
    for seed in [1_u64, 7, 99, 2_026] {
        let mut venue = Venue::new(RETAINED);
        venue.hub.subscribe(BOOK);
        let mut client = DepthClient::default();
        let mut traders = TraderPopulation::new(seed);
        let mut resting = Vec::new();

        // The client is offline for stretches of varying length while real
        // order flow continues. The window is generous enough that it should
        // never need a snapshot.
        let mut offline_for = 0;
        for _ in 0..2_000 {
            let command = traders.act(&mut resting);
            venue.send(command);

            if offline_for > 0 {
                offline_for -= 1;
                continue;
            }
            match client.poll(&venue, BOOK) {
                Resume::Delivered { .. } => {}
                other => panic!("seed {seed}: window was overrun: {other:?}"),
            }
            // Drop the connection every so often, for a while.
            if traders.next().is_multiple_of(10) {
                offline_for = traders.next() % 200;
            }
        }

        client.poll(&venue, BOOK);
        client.assert_matches(&venue, &format!("seed {seed}"));
        assert_eq!(
            client.snapshots, 0,
            "seed {seed}: needed an unexpected snapshot"
        );
        assert!(
            client.next > 500,
            "seed {seed}: session barely produced a feed"
        );
    }
}

#[test]
fn one_accounts_private_events_never_reach_another() {
    let mut venue = Venue::new(RETAINED);
    venue.hub.subscribe(Channel::Account(1));
    venue.hub.subscribe(Channel::Account(2));

    venue.send(limit_order(1, SYMBOL, 101, Side::Ask, 10_100, 5));
    venue.send(market_order(2, SYMBOL, 201, Side::Bid, 5));

    let mut ours = Vec::new();
    venue.hub.resume(Channel::Account(1), 0, &mut ours);
    let mut theirs = Vec::new();
    venue.hub.resume(Channel::Account(2), 0, &mut theirs);

    assert!(!ours.is_empty() && !theirs.is_empty(), "no private events");
    assert!(
        ours.iter().all(|e| e.account == 1),
        "account 1's channel carried someone else's events"
    );
    assert!(
        theirs.iter().all(|e| e.account == 2),
        "account 2's channel carried someone else's events"
    );
}

#[test]
fn the_public_tape_carries_no_identities() {
    let mut venue = Venue::new(RETAINED);
    venue.hub.subscribe(Channel::Trades(SYMBOL));

    venue.send(limit_order(1, SYMBOL, 101, Side::Ask, 10_100, 5));
    venue.send(market_order(2, SYMBOL, 201, Side::Bid, 5));

    let mut tape = Vec::new();
    venue.hub.resume(Channel::Trades(SYMBOL), 0, &mut tape);

    assert_eq!(tape.len(), 1, "one trade should have printed");
    for print in &tape {
        assert_eq!(print.kind, EventKind::Trade as u8);
        assert_eq!(print.account, 0, "the public tape leaked an account");
        assert_eq!(print.order_id, 0, "the public tape leaked an order ID");
        assert_eq!(print.counterparty_order_id, 0);
    }
    assert_eq!((tape[0].price, tape[0].quantity), (10_100, 5));
}

#[test]
fn each_channel_is_numbered_independently_so_neither_looks_gappy() {
    let mut venue = Venue::new(RETAINED);
    venue.hub.subscribe(BOOK);
    venue.hub.subscribe(Channel::Trades(SYMBOL));

    venue.send(limit_order(1, SYMBOL, 101, Side::Ask, 10_100, 5));
    venue.send(market_order(2, SYMBOL, 201, Side::Bid, 2));
    venue.send(market_order(2, SYMBOL, 202, Side::Bid, 1));

    for channel in [BOOK, Channel::Trades(SYMBOL)] {
        let mut events = Vec::new();
        venue.hub.resume(channel, 0, &mut events);
        let sequences: Vec<Sequence> = events.iter().map(|e| e.sequence).collect();
        let expected: Vec<Sequence> = (0..sequences.len() as u64).collect();
        assert_eq!(
            sequences, expected,
            "{channel:?} was not contiguous from zero"
        );
    }
}
