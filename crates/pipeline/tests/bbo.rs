//! The top-of-book feed.
//!
//! The value of this channel is entirely in what it does *not* send. A client
//! that only needs a price should pay for price changes, not for every order the
//! venue receives — so most of these tests assert that nothing was published,
//! which is the property a feed like this lives or dies by.
//!
//! The top of book is its own state, so restating it and changing it are the
//! same message. That is why there is no snapshot event here the way there is
//! for depth: two `Bbo` events describe the market completely.

mod common;

use bx_journal::MemoryLog;
use bx_pipeline::hub::{Channel, Hub, Resume};
use bx_pipeline::{Exchange, limit_order, market_order};
use bx_protocol::{Command, Event, EventKind, Sequence, Side, Ticks};
use common::{SYMBOL, cancel, funded};

struct Venue {
    exchange: Exchange<MemoryLog>,
    hub: Hub,
}

impl Venue {
    fn new() -> Self {
        let mut venue = Self {
            exchange: funded(),
            hub: Hub::new(4_096),
        };
        venue.hub.subscribe(Channel::Bbo(SYMBOL));
        venue.hub.subscribe(Channel::Book(SYMBOL));
        venue
    }

    fn send(&mut self, mut command: Command) {
        let events = self.exchange.submit(&mut command).unwrap();
        self.hub.publish(events);
    }

    /// Everything published on a channel since `from`, and where to read next.
    fn drain(&self, channel: Channel, from: Sequence) -> (Vec<Event>, Sequence) {
        let mut out = Vec::new();
        match self.hub.resume(channel, from, &mut out) {
            Resume::Delivered { next } => (out, next),
            other => panic!("the feed did not deliver: {other:?}"),
        }
    }

    fn top(&self, side: Side) -> (Ticks, u64) {
        self.exchange.book(SYMBOL).unwrap().top(side)
    }
}

/// The (side, price, quantity) each event states.
fn stated(events: &[Event]) -> Vec<(Side, Ticks, u64)> {
    events
        .iter()
        .map(|e| {
            assert_eq!(e.kind(), Some(EventKind::Bbo), "not a top-of-book event");
            (e.side().expect("a side"), e.price, e.quantity)
        })
        .collect()
}

#[test]
fn the_first_order_on_a_side_publishes_that_side_only() {
    let mut venue = Venue::new();
    venue.send(limit_order(1, SYMBOL, 1, Side::Bid, 10_500, 7));

    let (events, _) = venue.drain(Channel::Bbo(SYMBOL), 0);
    assert_eq!(
        stated(&events),
        vec![(Side::Bid, 10_500, 7)],
        "a first bid should publish the bid and say nothing about the ask"
    );
}

#[test]
fn an_order_behind_the_touch_publishes_nothing() {
    // The whole reason this channel exists. A venue's depth feed moves on every
    // order; this one must not.
    let mut venue = Venue::new();
    venue.send(limit_order(1, SYMBOL, 1, Side::Bid, 10_500, 1));
    let (_, cursor) = venue.drain(Channel::Bbo(SYMBOL), 0);

    for id in 2..=20 {
        venue.send(limit_order(
            1,
            SYMBOL,
            id,
            Side::Bid,
            10_499 - id as Ticks,
            1,
        ));
    }

    let (quiet, _) = venue.drain(Channel::Bbo(SYMBOL), cursor);
    assert!(
        quiet.is_empty(),
        "nineteen orders behind the touch published {} top-of-book events",
        quiet.len()
    );

    // And the depth feed did move, so the traffic was real.
    let (depth, _) = venue.drain(Channel::Book(SYMBOL), 0);
    assert!(
        depth.len() >= 19,
        "the depth feed saw only {} events, so the orders never landed",
        depth.len()
    );
}

#[test]
fn improving_the_price_publishes_the_new_top() {
    let mut venue = Venue::new();
    venue.send(limit_order(1, SYMBOL, 1, Side::Bid, 10_500, 1));
    let (_, cursor) = venue.drain(Channel::Bbo(SYMBOL), 0);

    venue.send(limit_order(2, SYMBOL, 2, Side::Bid, 10_501, 4));
    let (events, _) = venue.drain(Channel::Bbo(SYMBOL), cursor);
    assert_eq!(stated(&events), vec![(Side::Bid, 10_501, 4)]);
}

#[test]
fn joining_the_best_price_publishes_the_new_quantity() {
    // The price did not move but the size did, and a client quoting against the
    // touch needs to know.
    let mut venue = Venue::new();
    venue.send(limit_order(1, SYMBOL, 1, Side::Bid, 10_500, 3));
    let (_, cursor) = venue.drain(Channel::Bbo(SYMBOL), 0);

    venue.send(limit_order(2, SYMBOL, 2, Side::Bid, 10_500, 5));
    let (events, _) = venue.drain(Channel::Bbo(SYMBOL), cursor);
    assert_eq!(stated(&events), vec![(Side::Bid, 10_500, 8)]);
}

#[test]
fn cancelling_the_only_order_empties_the_side() {
    let mut venue = Venue::new();
    venue.send(limit_order(1, SYMBOL, 1, Side::Bid, 10_500, 1));
    let (_, cursor) = venue.drain(Channel::Bbo(SYMBOL), 0);

    venue.send(cancel(1, 1));
    let (events, _) = venue.drain(Channel::Bbo(SYMBOL), cursor);
    assert_eq!(
        stated(&events),
        vec![(Side::Bid, 0, 0)],
        "an emptied side must be stated, not left at its last price"
    );
    assert_eq!(venue.top(Side::Bid), (0, 0));
}

#[test]
fn cancelling_the_best_falls_back_to_the_next_price() {
    let mut venue = Venue::new();
    venue.send(limit_order(1, SYMBOL, 1, Side::Bid, 10_500, 1));
    venue.send(limit_order(1, SYMBOL, 2, Side::Bid, 10_400, 9));
    let (_, cursor) = venue.drain(Channel::Bbo(SYMBOL), 0);

    venue.send(cancel(1, 1));
    let (events, _) = venue.drain(Channel::Bbo(SYMBOL), cursor);
    assert_eq!(stated(&events), vec![(Side::Bid, 10_400, 9)]);
}

#[test]
fn cancelling_behind_the_touch_publishes_nothing() {
    let mut venue = Venue::new();
    venue.send(limit_order(1, SYMBOL, 1, Side::Bid, 10_500, 1));
    venue.send(limit_order(1, SYMBOL, 2, Side::Bid, 10_400, 1));
    let (_, cursor) = venue.drain(Channel::Bbo(SYMBOL), 0);

    venue.send(cancel(1, 2));
    let (events, _) = venue.drain(Channel::Bbo(SYMBOL), cursor);
    assert!(events.is_empty(), "a cancel behind the touch was published");
}

#[test]
fn a_trade_that_clears_the_touch_publishes_both_the_fill_side_and_nothing_else() {
    let mut venue = Venue::new();
    venue.send(limit_order(1, SYMBOL, 1, Side::Ask, 10_500, 2));
    venue.send(limit_order(1, SYMBOL, 2, Side::Ask, 10_600, 5));
    let (_, cursor) = venue.drain(Channel::Bbo(SYMBOL), 0);

    // Takes the whole best ask, so the touch moves up to the next price.
    venue.send(market_order(2, SYMBOL, 3, Side::Bid, 2));
    let (events, _) = venue.drain(Channel::Bbo(SYMBOL), cursor);
    assert_eq!(
        stated(&events),
        vec![(Side::Ask, 10_600, 5)],
        "a market order that consumed the touch did not restate it"
    );
}

#[test]
fn a_partial_fill_at_the_touch_publishes_the_remaining_size() {
    let mut venue = Venue::new();
    venue.send(limit_order(1, SYMBOL, 1, Side::Ask, 10_500, 10));
    let (_, cursor) = venue.drain(Channel::Bbo(SYMBOL), 0);

    venue.send(market_order(2, SYMBOL, 2, Side::Bid, 4));
    let (events, _) = venue.drain(Channel::Bbo(SYMBOL), cursor);
    assert_eq!(stated(&events), vec![(Side::Ask, 10_500, 6)]);
}

#[test]
fn a_crossing_order_that_moves_both_sides_publishes_both() {
    let mut venue = Venue::new();
    venue.send(limit_order(1, SYMBOL, 1, Side::Ask, 10_500, 2));
    let (_, cursor) = venue.drain(Channel::Bbo(SYMBOL), 0);

    // Takes the whole ask and rests the remainder as the new best bid, so the
    // ask empties and the bid appears in one command.
    venue.send(limit_order(2, SYMBOL, 2, Side::Bid, 10_500, 6));
    let (events, _) = venue.drain(Channel::Bbo(SYMBOL), cursor);
    let mut seen = stated(&events);
    seen.sort_by_key(|(side, _, _)| *side as u8);
    assert_eq!(seen, vec![(Side::Bid, 10_500, 4), (Side::Ask, 0, 0)]);
}

#[test]
fn a_rejected_order_publishes_nothing() {
    let mut venue = Venue::new();
    venue.send(limit_order(1, SYMBOL, 1, Side::Bid, 10_500, 1));
    let (_, cursor) = venue.drain(Channel::Bbo(SYMBOL), 0);

    // Outside the ladder, so the venue refuses it before the book is touched.
    venue.send(limit_order(1, SYMBOL, 2, Side::Bid, 9_000, 1));
    let (events, _) = venue.drain(Channel::Bbo(SYMBOL), cursor);
    assert!(events.is_empty(), "a rejected order moved the feed");
}

#[test]
fn the_feed_always_agrees_with_the_book() {
    // The property that matters: a client following only this channel and
    // applying every event ends up where the venue actually is.
    let mut venue = Venue::new();
    let mut client = [(0_i64, 0_u64); 2];
    let mut cursor = 0;

    let script: Vec<Command> = vec![
        limit_order(1, SYMBOL, 1, Side::Bid, 10_400, 5),
        limit_order(1, SYMBOL, 2, Side::Bid, 10_450, 2),
        limit_order(2, SYMBOL, 3, Side::Ask, 10_600, 4),
        limit_order(2, SYMBOL, 4, Side::Ask, 10_550, 1),
        market_order(3, SYMBOL, 5, Side::Bid, 1),
        cancel(1, 2),
        limit_order(3, SYMBOL, 6, Side::Bid, 10_500, 8),
        limit_order(1, SYMBOL, 7, Side::Ask, 10_500, 8),
        cancel(1, 1),
        cancel(2, 3),
    ];
    for command in script {
        venue.send(command);
        let (events, next) = venue.drain(Channel::Bbo(SYMBOL), cursor);
        cursor = next;
        for (side, price, quantity) in stated(&events) {
            client[side as usize] = (price, quantity);
        }
        for side in [Side::Bid, Side::Ask] {
            assert_eq!(
                client[side as usize],
                venue.top(side),
                "the client's {side:?} disagrees with the book after {} events",
                events.len()
            );
        }
    }
}

#[test]
fn the_top_feed_is_far_smaller_than_the_depth_feed() {
    // The reason to offer this channel at all, stated as a number rather than a
    // claim. A book with depth on both sides takes a great deal of traffic that
    // never reaches the touch.
    let mut venue = Venue::new();
    for id in 1..=200_u64 {
        let side = if id.is_multiple_of(2) {
            Side::Bid
        } else {
            Side::Ask
        };
        // Spread out behind the touch: only the first few on each side can
        // possibly move it.
        let price = match side {
            Side::Bid => 10_400 - (id as Ticks % 50),
            Side::Ask => 10_600 + (id as Ticks % 50),
        };
        venue.send(limit_order(1 + id % 4, SYMBOL, id, side, price, 1));
    }

    let (top, _) = venue.drain(Channel::Bbo(SYMBOL), 0);
    let (depth, _) = venue.drain(Channel::Book(SYMBOL), 0);
    assert!(
        top.len() * 8 < depth.len(),
        "the top feed sent {} events against the depth feed's {}, which is not \
         cheap enough to be worth a separate channel",
        top.len(),
        depth.len()
    );
}
