//! Halting, stopping and flattening.
//!
//! One rule runs through all of it: a restriction stops new risk and never
//! stops reducing it. A venue that will not let a client cancel is more
//! dangerous than one that lets it keep trading, because the client cannot get
//! out of what it already holds. Every test here checks both halves -- that the
//! restriction bites, and that a cancel still gets through it.

mod common;

use bx_pipeline::snapshot::Snapshot;
use bx_pipeline::{Exchange, cancel_all, limit_order, set_account_trading, set_symbol_state};
use bx_protocol::{Event, EventKind, RejectReason, Side, TradingState};
use common::{SYMBOL, USD, funded, instruments};

const ADMIN: u64 = 1;

fn rejected_with(events: &[Event], reason: RejectReason) -> bool {
    events
        .iter()
        .any(|e| e.kind == EventKind::Rejected as u8 && e.reject_reason() == Some(reason))
}

fn accepted(events: &[Event]) -> bool {
    events.iter().any(|e| e.kind == EventKind::Received as u8)
        && !events.iter().any(|e| e.kind == EventKind::Rejected as u8)
}

#[test]
fn a_halted_symbol_takes_no_new_orders_but_still_takes_cancels() {
    let mut exchange = funded();

    // Something resting first, so there is a cancel to make.
    let mut resting = limit_order(2, SYMBOL, 10, Side::Bid, 10_050, 1);
    assert!(accepted(exchange.submit(&mut resting).unwrap()));

    let mut halt = set_symbol_state(ADMIN, SYMBOL, TradingState::Halted);
    exchange.submit(&mut halt).unwrap();
    assert_eq!(exchange.symbol_state(SYMBOL), TradingState::Halted);

    let mut new_order = limit_order(2, SYMBOL, 11, Side::Bid, 10_050, 1);
    let events = exchange.submit(&mut new_order).unwrap();
    assert!(
        rejected_with(events, RejectReason::SymbolNotTrading),
        "a halted symbol accepted a new order: {events:?}"
    );

    // The half that matters: getting out is still possible.
    let mut cancel = common::cancel(2, 10);
    let events = exchange.submit(&mut cancel).unwrap();
    assert!(
        events.iter().any(|e| e.kind == EventKind::Canceled as u8),
        "a halt blocked a cancel, trapping the client: {events:?}"
    );

    // And resuming lets orders through again.
    let mut resume = set_symbol_state(ADMIN, SYMBOL, TradingState::Trading);
    exchange.submit(&mut resume).unwrap();
    let mut after = limit_order(2, SYMBOL, 12, Side::Bid, 10_050, 1);
    assert!(accepted(exchange.submit(&mut after).unwrap()));
}

#[test]
fn cancel_only_behaves_the_same_way_as_a_halt_for_new_orders() {
    let mut exchange = funded();
    let mut resting = limit_order(2, SYMBOL, 20, Side::Bid, 10_050, 1);
    exchange.submit(&mut resting).unwrap();

    let mut state = set_symbol_state(ADMIN, SYMBOL, TradingState::CancelOnly);
    exchange.submit(&mut state).unwrap();

    let mut new_order = limit_order(2, SYMBOL, 21, Side::Bid, 10_050, 1);
    assert!(rejected_with(
        exchange.submit(&mut new_order).unwrap(),
        RejectReason::SymbolNotTrading
    ));

    let mut cancel = common::cancel(2, 20);
    let events = exchange.submit(&mut cancel).unwrap();
    assert!(events.iter().any(|e| e.kind == EventKind::Canceled as u8));
}

#[test]
fn an_unknown_symbol_cannot_be_halted() {
    let mut exchange = funded();
    let mut halt = set_symbol_state(ADMIN, 9_999, TradingState::Halted);
    let events = exchange.submit(&mut halt).unwrap();
    assert!(rejected_with(events, RejectReason::UnknownSymbol));
}

#[test]
fn a_stopped_account_cannot_open_risk_but_can_close_it() {
    let mut exchange = funded();
    let mut resting = limit_order(2, SYMBOL, 30, Side::Bid, 10_050, 1);
    exchange.submit(&mut resting).unwrap();

    let mut stop = set_account_trading(ADMIN, 2, false);
    exchange.submit(&mut stop).unwrap();
    assert!(!exchange.account_may_trade(2));

    let mut blocked = limit_order(2, SYMBOL, 31, Side::Bid, 10_050, 1);
    assert!(rejected_with(
        exchange.submit(&mut blocked).unwrap(),
        RejectReason::AccountNotTrading
    ));

    // Still able to get out.
    let mut cancel = common::cancel(2, 30);
    let events = exchange.submit(&mut cancel).unwrap();
    assert!(
        events.iter().any(|e| e.kind == EventKind::Canceled as u8),
        "a stopped account could not cancel: {events:?}"
    );

    // Everyone else is untouched: a kill switch is per account.
    let mut other = limit_order(3, SYMBOL, 32, Side::Bid, 10_050, 1);
    assert!(accepted(exchange.submit(&mut other).unwrap()));

    let mut resume = set_account_trading(ADMIN, 2, true);
    exchange.submit(&mut resume).unwrap();
    let mut after = limit_order(2, SYMBOL, 33, Side::Bid, 10_050, 1);
    assert!(accepted(exchange.submit(&mut after).unwrap()));
}

#[test]
fn cancel_all_clears_one_account_and_leaves_the_rest_of_the_book() {
    let mut exchange = funded();
    for id in [40_u64, 41, 42] {
        let mut command = limit_order(2, SYMBOL, id, Side::Bid, 10_050 + id as i64, 1);
        exchange.submit(&mut command).unwrap();
    }
    let mut theirs = limit_order(3, SYMBOL, 50, Side::Bid, 10_040, 1);
    exchange.submit(&mut theirs).unwrap();
    assert_eq!(exchange.open_orders_for(2, SYMBOL).len(), 3);

    let mut flatten = cancel_all(2, SYMBOL);
    let events = exchange.submit(&mut flatten).unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::Canceled as u8)
            .count(),
        3,
        "expected one cancel per resting order: {events:?}"
    );
    assert!(exchange.open_orders_for(2, SYMBOL).is_empty());
    assert_eq!(
        exchange.open_orders_for(3, SYMBOL).len(),
        1,
        "cancel-all reached another account's orders"
    );

    // The hold is returned, not merely the order removed.
    assert_eq!(exchange.accounts().balance(2, USD).reserved, 0);
}

#[test]
fn cancel_all_with_no_symbol_covers_every_listed_instrument() {
    let mut exchange = funded();
    let mut one = limit_order(2, SYMBOL, 60, Side::Bid, 10_050, 1);
    exchange.submit(&mut one).unwrap();

    let mut flatten = cancel_all(2, 0);
    exchange.submit(&mut flatten).unwrap();
    assert!(exchange.open_orders_for(2, SYMBOL).is_empty());
}

#[test]
fn cancel_all_on_an_account_with_nothing_resting_is_harmless() {
    let mut exchange = funded();
    let mut flatten = cancel_all(2, SYMBOL);
    let events = exchange.submit(&mut flatten).unwrap();
    assert!(
        !events.iter().any(|e| e.kind == EventKind::Canceled as u8),
        "cancelled something that was not there: {events:?}"
    );
}

/// A halt an operator set has to survive a recovery.
///
/// This is the failure that would look like success: a venue comes back after a
/// crash, replays its journal, and resumes trading a symbol somebody had
/// deliberately stopped.
#[test]
fn a_halt_and_a_stopped_account_survive_snapshot_and_replay() {
    let mut exchange = funded();
    let mut halt = set_symbol_state(ADMIN, SYMBOL, TradingState::CancelOnly);
    exchange.submit(&mut halt).unwrap();
    let mut stop = set_account_trading(ADMIN, 4, false);
    exchange.submit(&mut stop).unwrap();

    let snapshot = exchange.snapshot();
    assert_eq!(snapshot.symbol_states.len(), 1);
    assert_eq!(snapshot.stopped_accounts.len(), 1);

    // Through the file format, not just the struct.
    let mut bytes = Vec::new();
    snapshot.write_to(&mut bytes).unwrap();
    let reread = Snapshot::read_from(&mut bytes.as_slice()).unwrap();
    assert_eq!(reread, snapshot);

    let mut restored = Exchange::new(exchange.into_storage(), instruments()).unwrap();
    restored.recover_from(&reread).unwrap();
    assert_eq!(restored.symbol_state(SYMBOL), TradingState::CancelOnly);
    assert!(!restored.account_may_trade(4));

    let mut blocked = limit_order(4, SYMBOL, 70, Side::Bid, 10_050, 1);
    let events = restored.submit(&mut blocked).unwrap();
    assert!(
        events.iter().any(|e| e.kind == EventKind::Rejected as u8),
        "a recovered venue let through what was stopped: {events:?}"
    );
}

/// Replay from zero reaches the same restrictions as snapshot plus replay.
#[test]
fn both_recovery_paths_agree_on_the_restrictions() {
    let mut exchange = funded();
    let mut halt = set_symbol_state(ADMIN, SYMBOL, TradingState::Halted);
    exchange.submit(&mut halt).unwrap();
    let mut stop = set_account_trading(ADMIN, 5, false);
    exchange.submit(&mut stop).unwrap();
    let snapshot = exchange.snapshot();

    let mut from_snapshot = Exchange::new(exchange.into_storage(), instruments()).unwrap();
    from_snapshot.recover_from(&snapshot).unwrap();
    let after_snapshot = from_snapshot.snapshot();

    let mut from_zero = Exchange::new(from_snapshot.into_storage(), instruments()).unwrap();
    from_zero.recover().unwrap();
    let after_replay = from_zero.snapshot();

    assert_eq!(after_snapshot.symbol_states, after_replay.symbol_states);
    assert_eq!(
        after_snapshot.stopped_accounts,
        after_replay.stopped_accounts
    );
}

/// A state the wire does not define is refused rather than guessed at.
#[test]
fn an_unknown_trading_state_is_refused() {
    let mut exchange = funded();
    let mut command = set_symbol_state(ADMIN, SYMBOL, TradingState::Trading);
    command.quantity = 99;
    let events = exchange.submit(&mut command).unwrap();
    assert!(rejected_with(events, RejectReason::SymbolNotTrading));
    assert_eq!(
        exchange.symbol_state(SYMBOL),
        TradingState::Trading,
        "a value that did not decode still changed the state"
    );
}

/// The engine's own view is untouched by a halt.
///
/// A halt is a pipeline decision, not a book one: the matching engine knows
/// nothing about trading states, and orders already resting stay exactly where
/// they were. A halt that quietly dropped the book would destroy queue position.
#[test]
fn a_halt_leaves_the_book_exactly_as_it_stood() {
    let mut exchange = funded();
    for id in [80_u64, 81] {
        let mut command = limit_order(2, SYMBOL, id, Side::Bid, 10_050, 1);
        exchange.submit(&mut command).unwrap();
    }
    let before = exchange.book(SYMBOL).unwrap().depth(Side::Bid, 10);

    let mut halt = set_symbol_state(ADMIN, SYMBOL, TradingState::Halted);
    exchange.submit(&mut halt).unwrap();

    let after = exchange.book(SYMBOL).unwrap().depth(Side::Bid, 10);
    assert_eq!(before, after, "a halt changed the book");
}
