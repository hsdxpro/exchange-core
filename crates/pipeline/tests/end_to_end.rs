//! End-to-end tests.
//!
//! No market data is fabricated anywhere here. Simulated traders send real
//! orders through the real API, the real engine matches them, and a simulated
//! subscriber consumes the resulting event stream and rebuilds the book from
//! the deltas alone. The assertion is that the subscriber's reconstruction
//! matches the exchange's actual book, which is the only way to know the feed
//! is truthful.

use bx_journal::MemoryLog;
use bx_pipeline::instrument::{AssetId, Instrument, Instruments};
use bx_pipeline::{Exchange, limit_order, market_order};
use bx_protocol::{Command, CommandKind, Event, EventKind, RejectReason, Side, Ticks, TimeInForce};
use std::collections::BTreeMap;

const BTC: AssetId = 1;
const USD: AssetId = 2;
const SYMBOL: u32 = 1;
const FLOOR: Ticks = 10_000;

fn venue() -> Exchange<MemoryLog> {
    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(SYMBOL, BTC, USD, FLOOR, 1_000_000));
    Exchange::new(MemoryLog::new(), instruments).unwrap()
}

fn funded() -> Exchange<MemoryLog> {
    let mut exchange = venue();
    for account in 1..=8 {
        exchange.deposit(account, USD, 100_000_000);
        exchange.deposit(account, BTC, 10_000);
    }
    exchange
}

/// A subscriber that knows nothing except the events it receives. It rebuilds
/// the book from deltas, exactly as a real client would.
#[derive(Debug, Default)]
struct Subscriber {
    bids: BTreeMap<Ticks, u64>,
    asks: BTreeMap<Ticks, u64>,
    trades: Vec<(Ticks, u64)>,
    last_sequence: Option<u64>,
    gaps: usize,
}

impl Subscriber {
    fn consume(&mut self, events: &[Event]) {
        for event in events {
            // A real subscriber detects loss by arithmetic, not by timeout.
            if let Some(previous) = self.last_sequence
                && event.sequence != previous + 1
            {
                self.gaps += 1;
            }
            self.last_sequence = Some(event.sequence);

            match event.kind {
                k if k == EventKind::BookDelta as u8 => {
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
                k if k == EventKind::Trade as u8 => {
                    self.trades.push((event.price, event.quantity));
                }
                _ => {}
            }
        }
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
}

/// Drives the exchange and feeds every event to the subscriber.
fn send(exchange: &mut Exchange<MemoryLog>, subscriber: &mut Subscriber, mut command: Command) {
    let events = exchange.submit(&mut command).unwrap().to_vec();
    subscriber.consume(&events);
}

#[test]
fn a_subscriber_rebuilds_the_book_from_deltas_alone() {
    let mut exchange = funded();
    let mut subscriber = Subscriber::default();

    // Two traders post liquidity on both sides.
    send(
        &mut exchange,
        &mut subscriber,
        limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5),
    );
    send(
        &mut exchange,
        &mut subscriber,
        limit_order(1, SYMBOL, 102, Side::Bid, 10_090, 8),
    );
    send(
        &mut exchange,
        &mut subscriber,
        limit_order(2, SYMBOL, 201, Side::Ask, 10_110, 4),
    );
    send(
        &mut exchange,
        &mut subscriber,
        limit_order(2, SYMBOL, 202, Side::Ask, 10_120, 7),
    );

    let book = exchange.book(SYMBOL).unwrap();
    assert_eq!(
        subscriber.depth(Side::Bid, 10),
        book.depth(Side::Bid, 10),
        "subscriber's bids diverged from the real book"
    );
    assert_eq!(
        subscriber.depth(Side::Ask, 10),
        book.depth(Side::Ask, 10),
        "subscriber's asks diverged from the real book"
    );
    assert_eq!(subscriber.gaps, 0, "event sequence had a gap");
}

#[test]
fn a_market_order_produces_trades_the_subscriber_sees() {
    let mut exchange = funded();
    let mut subscriber = Subscriber::default();

    send(
        &mut exchange,
        &mut subscriber,
        limit_order(1, SYMBOL, 101, Side::Ask, 10_100, 3),
    );
    send(
        &mut exchange,
        &mut subscriber,
        limit_order(1, SYMBOL, 102, Side::Ask, 10_200, 5),
    );

    // A second trader buys 6 at market: takes 3 at 10_100 then 3 at 10_200.
    send(
        &mut exchange,
        &mut subscriber,
        market_order(2, SYMBOL, 201, Side::Bid, 6),
    );

    assert_eq!(
        subscriber.trades,
        vec![(10_100, 3), (10_200, 3)],
        "public trade prints must match what actually executed"
    );
    let book = exchange.book(SYMBOL).unwrap();
    assert_eq!(subscriber.depth(Side::Ask, 10), book.depth(Side::Ask, 10));
    assert_eq!(book.depth(Side::Ask, 10), vec![(10_200, 2)]);
}

#[test]
fn a_limit_order_that_crosses_trades_and_rests_the_remainder() {
    let mut exchange = funded();
    let mut subscriber = Subscriber::default();

    send(
        &mut exchange,
        &mut subscriber,
        limit_order(1, SYMBOL, 101, Side::Ask, 10_100, 4),
    );
    // Buy 10 at 10_100: 4 trade, 6 rest as the new best bid.
    send(
        &mut exchange,
        &mut subscriber,
        limit_order(2, SYMBOL, 201, Side::Bid, 10_100, 10),
    );

    assert_eq!(subscriber.trades, vec![(10_100, 4)]);
    let book = exchange.book(SYMBOL).unwrap();
    assert_eq!(book.depth(Side::Bid, 10), vec![(10_100, 6)]);
    assert!(book.depth(Side::Ask, 10).is_empty());
    assert_eq!(subscriber.depth(Side::Bid, 10), book.depth(Side::Bid, 10));
    assert_eq!(subscriber.depth(Side::Ask, 10), book.depth(Side::Ask, 10));
}

#[test]
fn cancelling_removes_the_level_for_subscribers_too() {
    let mut exchange = funded();
    let mut subscriber = Subscriber::default();

    send(
        &mut exchange,
        &mut subscriber,
        limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5),
    );
    assert_eq!(subscriber.depth(Side::Bid, 10), vec![(10_100, 5)]);

    let mut cancel = Command::new(
        CommandKind::Cancel,
        1,
        SYMBOL,
        101,
        Side::Bid,
        10_100,
        0,
        TimeInForce::GoodTillCancel,
    );
    let events = exchange.submit(&mut cancel).unwrap().to_vec();
    subscriber.consume(&events);

    assert!(
        subscriber.depth(Side::Bid, 10).is_empty(),
        "a cancelled level must disappear from the feed"
    );
    assert_eq!(
        subscriber.depth(Side::Bid, 10),
        exchange.book(SYMBOL).unwrap().depth(Side::Bid, 10)
    );
}

#[test]
fn balances_move_correctly_and_nothing_is_created_or_destroyed() {
    let mut exchange = funded();
    let mut subscriber = Subscriber::default();

    let usd_before = exchange.accounts().total_supply(USD);
    let btc_before = exchange.accounts().total_supply(BTC);

    send(
        &mut exchange,
        &mut subscriber,
        limit_order(1, SYMBOL, 101, Side::Ask, 10_100, 5),
    );
    send(
        &mut exchange,
        &mut subscriber,
        market_order(2, SYMBOL, 201, Side::Bid, 5),
    );

    // Seller gave 5 BTC and received 5 * 10_100 USD.
    assert_eq!(exchange.accounts().balance(1, BTC).total(), 10_000 - 5);
    assert_eq!(
        exchange.accounts().balance(1, USD).total(),
        100_000_000 + 5 * 10_100
    );
    // Buyer received 5 BTC and paid for them.
    assert_eq!(exchange.accounts().balance(2, BTC).total(), 10_000 + 5);
    assert_eq!(
        exchange.accounts().balance(2, USD).total(),
        100_000_000 - 5 * 10_100
    );

    assert_eq!(
        exchange.accounts().total_supply(USD),
        usd_before,
        "USD supply changed"
    );
    assert_eq!(
        exchange.accounts().total_supply(BTC),
        btc_before,
        "BTC supply changed"
    );
}

#[test]
fn an_order_beyond_the_balance_is_refused_and_leaves_no_trace() {
    let mut exchange = venue();
    exchange.deposit(1, USD, 1_000); // enough for well under one lot
    let mut subscriber = Subscriber::default();

    let mut command = limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5);
    let events = exchange.submit(&mut command).unwrap().to_vec();
    subscriber.consume(&events);

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, EventKind::Rejected as u8);
    assert_eq!(
        events[0].reject_reason,
        RejectReason::InsufficientBalance as u8
    );
    assert!(
        exchange
            .book(SYMBOL)
            .unwrap()
            .depth(Side::Bid, 10)
            .is_empty()
    );
    assert_eq!(
        exchange.accounts().balance(1, USD).free,
        1_000,
        "hold must be released"
    );
    assert_eq!(exchange.accounts().balance(1, USD).reserved, 0);
}

#[test]
fn one_account_cannot_reserve_the_same_balance_twice() {
    let mut exchange = venue();
    // Exactly enough for one order of 5 at 10_100, not two.
    exchange.deposit(1, USD, 5 * 10_100);
    let mut subscriber = Subscriber::default();

    send(
        &mut exchange,
        &mut subscriber,
        limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5),
    );

    let mut second = limit_order(1, SYMBOL, 102, Side::Bid, 10_100, 5);
    let events = exchange.submit(&mut second).unwrap().to_vec();
    assert_eq!(events[0].kind, EventKind::Rejected as u8);
    assert_eq!(
        events[0].reject_reason,
        RejectReason::InsufficientBalance as u8
    );
    assert_eq!(
        exchange.book(SYMBOL).unwrap().depth(Side::Bid, 10),
        vec![(10_100, 5)]
    );
}

#[test]
fn prices_outside_the_band_are_refused() {
    let mut exchange = funded();
    let mut below = limit_order(1, SYMBOL, 101, Side::Bid, FLOOR - 1, 1);
    let events = exchange.submit(&mut below).unwrap().to_vec();
    assert_eq!(
        events[0].reject_reason,
        RejectReason::OutsidePriceBand as u8
    );
}

/// A population of traders, driving the venue through the public API only.
///
/// Deterministic: the same seed produces the same order flow, so a failure is
/// reproducible from the seed alone.
struct TraderPopulation {
    state: u64,
    next_order: u64,
}

impl TraderPopulation {
    fn new(seed: u64) -> Self {
        Self {
            state: seed,
            next_order: 1,
        }
    }

    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.state >> 16
    }

    /// Produces one plausible action: post, cancel, or take.
    fn act(&mut self, resting: &mut Vec<(u64, u64)>) -> Command {
        let roll = self.next() % 100;
        let account = 1 + self.next() % 8;
        let order_id = self.next_order;
        self.next_order += 1;

        if roll < 20 && !resting.is_empty() {
            let index = (self.next() as usize) % resting.len();
            let (id, owner) = resting.remove(index);
            return Command::new(
                CommandKind::Cancel,
                owner,
                SYMBOL,
                id,
                Side::Bid,
                0,
                0,
                TimeInForce::GoodTillCancel,
            );
        }
        if roll < 35 {
            let side = if self.next().is_multiple_of(2) {
                Side::Bid
            } else {
                Side::Ask
            };
            return market_order(account, SYMBOL, order_id, side, 1 + self.next() % 3);
        }

        let side = if self.next().is_multiple_of(2) {
            Side::Bid
        } else {
            Side::Ask
        };
        // Bids below the mid, asks above, so the book is not permanently crossed.
        let price = match side {
            Side::Bid => 10_100 - (self.next() as Ticks % 20),
            Side::Ask => 10_101 + (self.next() as Ticks % 20),
        };
        resting.push((order_id, account));
        limit_order(account, SYMBOL, order_id, side, price, 1 + self.next() % 5)
    }
}

#[test]
fn a_trading_session_keeps_the_feed_and_the_book_in_agreement() {
    for seed in [1_u64, 7, 99, 2_026] {
        let mut exchange = funded();
        let mut subscriber = Subscriber::default();
        let mut traders = TraderPopulation::new(seed);
        let mut resting = Vec::new();

        let usd_before = exchange.accounts().total_supply(USD);
        let btc_before = exchange.accounts().total_supply(BTC);

        for _ in 0..2_000 {
            let command = traders.act(&mut resting);
            send(&mut exchange, &mut subscriber, command);
        }

        let book = exchange.book(SYMBOL).unwrap();
        assert_eq!(
            subscriber.depth(Side::Bid, 100),
            book.depth(Side::Bid, 100),
            "seed {seed}: subscriber's bids diverged from the book"
        );
        assert_eq!(
            subscriber.depth(Side::Ask, 100),
            book.depth(Side::Ask, 100),
            "seed {seed}: subscriber's asks diverged from the book"
        );
        assert_eq!(subscriber.gaps, 0, "seed {seed}: event sequence had a gap");
        assert_eq!(
            exchange.accounts().total_supply(USD),
            usd_before,
            "seed {seed}: USD was created or destroyed"
        );
        assert_eq!(
            exchange.accounts().total_supply(BTC),
            btc_before,
            "seed {seed}: BTC was created or destroyed"
        );
        assert!(
            subscriber.trades.len() > 10,
            "seed {seed}: session produced no trading"
        );
    }
}

#[test]
fn replaying_the_journal_reproduces_the_book_exactly() {
    for seed in [3_u64, 42, 1_234] {
        let mut exchange = funded();
        let mut subscriber = Subscriber::default();
        let mut traders = TraderPopulation::new(seed);
        let mut resting = Vec::new();

        for _ in 0..1_000 {
            let command = traders.act(&mut resting);
            send(&mut exchange, &mut subscriber, command);
        }

        let bids = exchange.book(SYMBOL).unwrap().depth(Side::Bid, 1_000);
        let asks = exchange.book(SYMBOL).unwrap().depth(Side::Ask, 1_000);
        let usd = exchange.accounts().total_supply(USD);
        let btc = exchange.accounts().total_supply(BTC);
        let open = exchange.open_orders();
        let sequences = exchange.next_sequence();

        // Restart: take the journal, build a clean exchange over it, replay.
        // Deposits are not journalled yet, so they are re-applied the way a
        // real venue would restore balances from its account store.
        let storage = exchange.into_storage();
        let mut instruments = Instruments::new();
        instruments.insert(Instrument::new(SYMBOL, BTC, USD, FLOOR, 1_000_000));
        let mut recovered = Exchange::new(storage, instruments).unwrap();
        for account in 1..=8 {
            recovered.deposit(account, USD, 100_000_000);
            recovered.deposit(account, BTC, 10_000);
        }
        let replayed = recovered.recover().unwrap();

        assert_eq!(
            replayed, sequences,
            "seed {seed}: replayed a different number of commands"
        );
        assert_eq!(
            recovered.book(SYMBOL).unwrap().depth(Side::Bid, 1_000),
            bids,
            "seed {seed}: recovered bids differ from the book that was lost"
        );
        assert_eq!(
            recovered.book(SYMBOL).unwrap().depth(Side::Ask, 1_000),
            asks,
            "seed {seed}: recovered asks differ from the book that was lost"
        );
        assert_eq!(
            recovered.accounts().total_supply(USD),
            usd,
            "seed {seed}: USD differs"
        );
        assert_eq!(
            recovered.accounts().total_supply(BTC),
            btc,
            "seed {seed}: BTC differs"
        );
        assert_eq!(
            recovered.open_orders(),
            open,
            "seed {seed}: open order count differs"
        );
    }
}

#[test]
fn a_crash_before_sync_loses_nothing_that_was_acknowledged() {
    let mut exchange = funded();
    let mut subscriber = Subscriber::default();

    for i in 0..50 {
        send(
            &mut exchange,
            &mut subscriber,
            limit_order(1, SYMBOL, 100 + i, Side::Bid, 10_000 + i as Ticks, 1),
        );
    }
    let acknowledged = exchange.next_sequence();
    let bids = exchange.book(SYMBOL).unwrap().depth(Side::Bid, 1_000);

    // Power loss. Every acknowledged command was synced, so none is lost.
    let mut storage = exchange.into_storage();
    storage.crash();

    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(SYMBOL, BTC, USD, FLOOR, 1_000_000));
    let mut recovered = Exchange::new(storage, instruments).unwrap();
    for account in 1..=8 {
        recovered.deposit(account, USD, 100_000_000);
        recovered.deposit(account, BTC, 10_000);
    }
    let replayed = recovered.recover().unwrap();

    assert_eq!(replayed, acknowledged, "an acknowledged command was lost");
    assert_eq!(
        recovered.book(SYMBOL).unwrap().depth(Side::Bid, 1_000),
        bids
    );
}
