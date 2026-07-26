//! End-to-end tests.
//!
//! No market data is fabricated anywhere here. Simulated traders send real
//! orders through the real API, the real engine matches them, and a simulated
//! subscriber consumes the resulting event stream and rebuilds the book from
//! the deltas alone. The assertion is that the subscriber's reconstruction
//! matches the exchange's actual book, which is the only way to know the feed
//! is truthful.

mod common;

use bx_journal::MemoryLog;
use bx_pipeline::{Exchange, accounting_violations, limit_order, market_order};
use bx_protocol::{Command, CommandKind, Event, EventKind, RejectReason, Side, Ticks, TimeInForce};
use common::{
    BTC, FLOOR, START_BTC, START_USD, SYMBOL, TraderPopulation, USD, cancel, funded, instruments,
    venue,
};
use std::collections::BTreeMap;

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

    let mut request = cancel(1, 101);
    let events = exchange.submit(&mut request).unwrap().to_vec();
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
    assert_eq!(
        exchange.accounts().balance(1, BTC).total(),
        u128::from(START_BTC) - 5
    );
    assert_eq!(
        exchange.accounts().balance(1, USD).total(),
        u128::from(START_USD) + 5 * 10_100
    );
    // Buyer received 5 BTC and paid for them.
    assert_eq!(
        exchange.accounts().balance(2, BTC).total(),
        u128::from(START_BTC) + 5
    );
    assert_eq!(
        exchange.accounts().balance(2, USD).total(),
        u128::from(START_USD) - 5 * 10_100
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
    exchange.deposit(1, USD, 1_000).unwrap(); // enough for well under one lot
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
    exchange.deposit(1, USD, 5 * 10_100).unwrap();
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
        assert_eq!(
            accounting_violations(),
            0,
            "seed {seed}: an accounting operation failed that should have been impossible"
        );
    }
}

#[test]
fn replaying_the_journal_restores_balances_as_well_as_orders() {
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
        // Nothing is re-applied by hand -- deposits are journalled, so the
        // money comes back from the log along with the orders.
        let storage = exchange.into_storage();
        let mut recovered = Exchange::new(storage, instruments()).unwrap();
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

    let mut recovered = Exchange::new(storage, instruments()).unwrap();
    let replayed = recovered.recover().unwrap();

    assert_eq!(replayed, acknowledged, "an acknowledged command was lost");
    assert_eq!(
        recovered.book(SYMBOL).unwrap().depth(Side::Bid, 1_000),
        bids
    );
}

#[test]
fn an_enqueued_command_is_not_durable_until_it_is_committed() {
    let mut exchange = funded();
    let committed = exchange.next_sequence();

    // Enqueued, applied, but never committed.
    let mut order = limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5);
    exchange.enqueue(&mut order).unwrap();
    assert_eq!(
        exchange.book(SYMBOL).unwrap().depth(Side::Bid, 10),
        vec![(10_100, 5)],
        "the order should be live in memory"
    );

    // Power loss before the commit.
    let mut storage = exchange.into_storage();
    storage.crash();

    let mut recovered = Exchange::new(storage, instruments()).unwrap();
    assert_eq!(
        recovered.recover().unwrap(),
        committed,
        "an uncommitted command survived, so it was acknowledged too early"
    );
    assert!(
        recovered
            .book(SYMBOL)
            .unwrap()
            .depth(Side::Bid, 10)
            .is_empty()
    );
}

#[test]
fn one_commit_makes_a_whole_group_durable() {
    let mut exchange = funded();
    for i in 0..20 {
        let mut order = limit_order(1, SYMBOL, 100 + i, Side::Bid, 10_000 + i as Ticks, 1);
        exchange.enqueue(&mut order).unwrap();
    }
    // Every event in the group is released at once, and only now.
    let released = exchange.commit().unwrap().len();
    assert!(released >= 20, "expected the whole group's events");

    let expected = exchange.next_sequence();
    let bids = exchange.book(SYMBOL).unwrap().depth(Side::Bid, 100);

    let mut storage = exchange.into_storage();
    storage.crash();
    let mut recovered = Exchange::new(storage, instruments()).unwrap();
    assert_eq!(
        recovered.recover().unwrap(),
        expected,
        "a committed command was lost"
    );
    assert_eq!(recovered.book(SYMBOL).unwrap().depth(Side::Bid, 100), bids);
}

#[test]
fn events_accumulate_across_a_group_and_reset_after_it_is_released() {
    let mut exchange = funded();
    let mut first = limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 1);
    let mut second = limit_order(1, SYMBOL, 102, Side::Bid, 10_090, 1);
    exchange.enqueue(&mut first).unwrap();
    exchange.enqueue(&mut second).unwrap();
    let group = exchange.commit().unwrap().len();

    let mut third = limit_order(1, SYMBOL, 103, Side::Bid, 10_080, 1);
    exchange.enqueue(&mut third).unwrap();
    let next = exchange.commit().unwrap().len();
    assert!(
        next < group,
        "the second group still carried the first group's events"
    );
}

#[test]
fn cancel_replace_of_an_unknown_order_must_not_create_one() {
    let mut exchange = funded();

    // Replace order 999, which does not exist. FIX semantics: the whole
    // request is rejected. It must not leave a new order resting.
    let mut request = Command::new(
        CommandKind::CancelReplace,
        1,
        SYMBOL,
        999,
        Side::Bid,
        10_100,
        5,
        TimeInForce::GoodTillCancel,
    );
    request.replacement_id = 1_000;
    let events = exchange.submit(&mut request).unwrap().to_vec();

    assert!(
        exchange
            .book(SYMBOL)
            .unwrap()
            .depth(Side::Bid, 10)
            .is_empty(),
        "a replace of a nonexistent order left something resting: {:?}",
        exchange.book(SYMBOL).unwrap().depth(Side::Bid, 10)
    );
    assert!(
        events.iter().any(|e| e.kind == EventKind::Rejected as u8),
        "the request should be rejected"
    );
    assert_eq!(
        exchange.accounts().balance(1, USD).reserved,
        0,
        "no balance should be held"
    );
}

#[test]
fn an_order_that_would_trade_with_itself_is_refused_and_the_resting_side_kept() {
    let mut exchange = funded();

    // Account 1 rests an ask, then tries to buy it back.
    let mut sell = limit_order(1, SYMBOL, 101, Side::Ask, 10_100, 5);
    exchange.submit(&mut sell).unwrap();
    let mut buy = limit_order(1, SYMBOL, 102, Side::Bid, 10_100, 5);
    let events = exchange.submit(&mut buy).unwrap().to_vec();

    assert_eq!(
        events
            .iter()
            .find(|e| e.kind == EventKind::Rejected as u8)
            .map(|e| e.reject_reason),
        Some(RejectReason::SelfMatchPrevented as u8),
        "the self-match was allowed: {events:?}"
    );
    // Cancel-newest: the incoming order is the one refused, and the resting
    // liquidity is untouched.
    assert_eq!(
        exchange.book(SYMBOL).unwrap().depth(Side::Ask, 10),
        vec![(10_100, 5)],
        "the resting order was destroyed instead of the incoming one"
    );
    assert!(
        exchange
            .book(SYMBOL)
            .unwrap()
            .depth(Side::Bid, 10)
            .is_empty(),
        "the refused order rested anyway"
    );
    // And it left no hold behind.
    assert_eq!(exchange.accounts().balance(1, USD).reserved, 0);
}

#[test]
fn an_order_that_is_filled_before_meeting_itself_still_trades() {
    let mut exchange = funded();

    // Account 2 is best; account 1 is behind it at a worse price.
    let mut theirs = limit_order(2, SYMBOL, 201, Side::Ask, 10_100, 5);
    exchange.submit(&mut theirs).unwrap();
    let mut ours = limit_order(1, SYMBOL, 101, Side::Ask, 10_200, 5);
    exchange.submit(&mut ours).unwrap();

    // Account 1 buys 5: filled entirely by account 2, never reaching its own
    // order. Refusing this would be wrong -- it is not a self-match.
    let mut buy = limit_order(1, SYMBOL, 102, Side::Bid, 10_250, 5);
    let events = exchange.submit(&mut buy).unwrap().to_vec();

    assert!(
        !events.iter().any(|e| e.kind == EventKind::Rejected as u8),
        "a trade that never met itself was refused: {events:?}"
    );
    assert!(
        events.iter().any(|e| e.kind == EventKind::Filled as u8),
        "it should have traded"
    );
    // Account 1's own ask is still resting.
    assert_eq!(
        exchange.book(SYMBOL).unwrap().depth(Side::Ask, 10),
        vec![(10_200, 5)]
    );
}

#[test]
fn a_sweep_that_would_reach_its_own_order_is_refused() {
    let mut exchange = funded();

    let mut theirs = limit_order(2, SYMBOL, 201, Side::Ask, 10_100, 2);
    exchange.submit(&mut theirs).unwrap();
    let mut ours = limit_order(1, SYMBOL, 101, Side::Ask, 10_200, 5);
    exchange.submit(&mut ours).unwrap();

    // Buying 5 takes account 2's 2 and then reaches account 1's own order.
    let mut buy = limit_order(1, SYMBOL, 102, Side::Bid, 10_250, 5);
    let events = exchange.submit(&mut buy).unwrap().to_vec();

    assert_eq!(
        events
            .iter()
            .find(|e| e.kind == EventKind::Rejected as u8)
            .map(|e| e.reject_reason),
        Some(RejectReason::SelfMatchPrevented as u8),
        "a sweep that reaches its own order was allowed: {events:?}"
    );
    // Nothing traded, so both asks are intact.
    assert_eq!(
        exchange.book(SYMBOL).unwrap().depth(Side::Ask, 10),
        vec![(10_100, 2), (10_200, 5)]
    );
}

#[test]
fn a_market_buy_cannot_spend_more_than_the_account_has() {
    let mut exchange = venue();
    // Seller has plenty of BTC.
    exchange.deposit(2, BTC, 1_000).unwrap();
    // Buyer can afford exactly 1 unit at 10_100.
    exchange.deposit(1, USD, 10_100).unwrap();

    let mut ask = limit_order(2, SYMBOL, 201, Side::Ask, 10_100, 100);
    exchange.submit(&mut ask).unwrap();

    let usd_supply = exchange.accounts().total_supply(USD);
    let btc_supply = exchange.accounts().total_supply(BTC);

    // Buy 100 units at market. The account can afford one.
    let mut buy = market_order(1, SYMBOL, 101, Side::Bid, 100);
    exchange.submit(&mut buy).unwrap();

    assert_eq!(
        exchange.accounts().total_supply(USD),
        usd_supply,
        "USD was created or destroyed by an unaffordable market buy"
    );
    assert_eq!(
        exchange.accounts().total_supply(BTC),
        btc_supply,
        "BTC was created or destroyed by an unaffordable market buy"
    );
    assert!(
        exchange.accounts().balance(1, BTC).total() <= 1,
        "the buyer received {} BTC but could only afford 1",
        exchange.accounts().balance(1, BTC).total()
    );
}
