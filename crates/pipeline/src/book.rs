//! One symbol's book: the matching engine, plus everything needed to drive it
//! from wire types and to describe what changed afterwards.
//!
//! Two jobs the engine deliberately does not do.
//!
//! **Narrowing.** The wire carries `u64` quantities and `u64` exchange order
//! IDs. The engine wants `u32` quantities and dense `u32` slot indices. This is
//! where that conversion happens, and where anything that does not fit is
//! rejected rather than truncated.
//!
//! **Deltas.** The engine reports fills through a callback and nothing else, so
//! a depth feed cannot be built from it directly. After each command we know
//! exactly which prices could have moved: the order's own price and every price
//! that traded. Re-reading the aggregate at those few prices gives the delta
//! set, without touching the engine or paying for a full book scan.

use crate::instrument::Instrument;
use bx_engine::{L3Book, OrderError, Side as ESide, TimeInForce as ETif};
use bx_protocol::{OrderId, Quantity, RejectReason, Side, Ticks, TimeInForce};
use std::collections::HashMap;

/// A price whose aggregate quantity changed, and what it changed to.
/// A quantity of zero means the level is now empty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LevelChange {
    pub side: Side,
    pub price: Ticks,
    pub quantity: u64,
}

/// One execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Execution {
    pub resting_order: OrderId,
    pub resting_side: Side,
    pub price: Ticks,
    pub quantity: Quantity,
}

/// What one command did to one book.
#[derive(Clone, Debug, Default)]
pub struct Outcome {
    pub reject: Option<RejectReason>,
    pub executions: Vec<Execution>,
    pub level_changes: Vec<LevelChange>,
    /// Quantity that came to rest, if any.
    pub resting_quantity: Quantity,
}

impl Outcome {
    #[must_use]
    pub fn rejected(reason: RejectReason) -> Self {
        Self {
            reject: Some(reason),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn traded_quantity(&self) -> Quantity {
        self.executions.iter().map(|e| e.quantity).sum()
    }
}

/// Hands out the dense `u32` slot indices the engine needs, and maps them back
/// to the `u64` order IDs clients use.
///
/// This is the concrete cost of the engine indexing orders by a dense integer.
/// It is a hash lookup on the cancel and amend paths, which is exactly what the
/// engine's own design avoids internally.
#[derive(Debug)]
struct SlotAllocator {
    to_slot: HashMap<OrderId, u32>,
    to_order: Vec<OrderId>,
    free: Vec<u32>,
    capacity: u32,
}

impl SlotAllocator {
    fn new(capacity: u32) -> Self {
        Self {
            to_slot: HashMap::new(),
            to_order: vec![0; capacity as usize],
            free: (0..capacity).rev().collect(),
            capacity,
        }
    }

    fn allocate(&mut self, order: OrderId) -> Option<u32> {
        if self.to_slot.contains_key(&order) {
            return None;
        }
        let slot = self.free.pop()?;
        self.to_slot.insert(order, slot);
        self.to_order[slot as usize] = order;
        Some(slot)
    }

    fn slot_of(&self, order: OrderId) -> Option<u32> {
        self.to_slot.get(&order).copied()
    }

    fn order_of(&self, slot: u32) -> OrderId {
        self.to_order[slot as usize]
    }

    fn release(&mut self, order: OrderId) {
        if let Some(slot) = self.to_slot.remove(&order) {
            self.free.push(slot);
        }
    }

    fn live(&self) -> usize {
        self.capacity as usize - self.free.len()
    }
}

/// One symbol's order book.
#[derive(Debug)]
pub struct Book {
    instrument: Instrument,
    engine: L3Book,
    slots: SlotAllocator,
}

impl Book {
    #[must_use]
    pub fn new(instrument: Instrument, capacity: u32) -> Self {
        Self {
            instrument,
            engine: L3Book::new(capacity as usize, capacity as usize),
            slots: SlotAllocator::new(capacity),
        }
    }

    #[must_use]
    pub const fn instrument(&self) -> &Instrument {
        &self.instrument
    }

    #[must_use]
    pub fn live_orders(&self) -> usize {
        self.slots.live()
    }

    #[must_use]
    pub fn best_bid(&self) -> Option<Ticks> {
        let best = self.engine.best_bid();
        (best >= 0).then(|| self.instrument.to_price(best as u16))
    }

    #[must_use]
    pub fn best_ask(&self) -> Option<Ticks> {
        let best = self.engine.best_ask();
        (best >= 0 && best < bx_engine::PRICE_COUNT as i32)
            .then(|| self.instrument.to_price(best as u16))
    }

    /// Aggregate quantity resting at a price.
    #[must_use]
    pub fn level_quantity(&self, side: Side, price: Ticks) -> u64 {
        self.instrument.to_slot(price).map_or(0, |slot| {
            self.engine.level_quantity(engine_side(side), slot)
        })
    }

    #[must_use]
    pub fn contains(&self, order: OrderId) -> bool {
        self.slots
            .slot_of(order)
            .is_some_and(|s| self.engine.contains(s))
    }

    /// Depth on one side, best price first, at most `limit` levels.
    #[must_use]
    pub fn depth(&self, side: Side, limit: usize) -> Vec<(Ticks, u64)> {
        let mut out = Vec::new();
        self.engine
            .for_each_level(engine_side(side), limit, |slot, quantity| {
                out.push((self.instrument.to_price(slot), quantity));
            });
        out
    }

    /// Submits a new order. `price` is ignored for market orders.
    pub fn submit(
        &mut self,
        order: OrderId,
        side: Side,
        price: Ticks,
        quantity: Quantity,
        tif: TimeInForce,
        market: bool,
    ) -> Outcome {
        let Ok(engine_quantity) = u32::try_from(quantity) else {
            return Outcome::rejected(RejectReason::QuantityTooLarge);
        };
        if engine_quantity == 0 {
            return Outcome::rejected(RejectReason::QuantityZero);
        }

        // A market order addresses the far end of the band, so the price-band
        // check applies to limit orders only.
        let slot = if market {
            match side {
                Side::Bid => u16::MAX,
                Side::Ask => 0,
            }
        } else {
            match self.instrument.to_slot(price) {
                Some(slot) => slot,
                None => return Outcome::rejected(RejectReason::OutsidePriceBand),
            }
        };

        let Some(engine_order) = self.slots.allocate(order) else {
            return Outcome::rejected(RejectReason::DuplicateOrderId);
        };

        let mut executions = Vec::new();
        let mut touched = TouchedPrices::default();
        touched.add(side.opposite(), slot);
        touched.add(side, slot);

        let result = self.engine.submit_limit_with(
            engine_order,
            engine_side(side),
            slot,
            engine_quantity,
            engine_tif(tif),
            |fill| {
                touched.add(side.opposite(), fill.price);
                executions.push(Execution {
                    resting_order: 0, // filled in below; the closure cannot borrow self
                    resting_side: side.opposite(),
                    price: i64::from(fill.price),
                    quantity: u64::from(fill.quantity),
                });
                executions.last_mut().unwrap().resting_order = u64::from(fill.maker_order_id);
            },
        );

        match result {
            Ok(report) => {
                // Resolve maker slot indices to client order IDs, and ladder
                // slots to wire prices, now that the borrow has ended.
                for execution in &mut executions {
                    let slot = u32::try_from(execution.resting_order).unwrap_or(0);
                    execution.resting_order = self.slots.order_of(slot);
                    execution.price = self.instrument.to_price(execution.price as u16);
                }
                // Fully-filled makers are gone from the engine; free their slots.
                self.reap_dead_slots(&executions);
                if report.rested_quantity == 0 {
                    self.slots.release(order);
                }
                Outcome {
                    reject: None,
                    level_changes: touched.resolve(self),
                    resting_quantity: u64::from(report.rested_quantity),
                    executions,
                }
            }
            Err(error) => {
                self.slots.release(order);
                Outcome::rejected(map_error(error))
            }
        }
    }

    pub fn cancel(&mut self, order: OrderId) -> Outcome {
        let Some(slot) = self.slots.slot_of(order) else {
            return Outcome::rejected(RejectReason::UnknownOrderId);
        };
        let Some(view) = self.engine.order(slot) else {
            return Outcome::rejected(RejectReason::UnknownOrderId);
        };
        let side = wire_side(view.side);
        let price = view.price;

        match self.engine.cancel(slot) {
            Ok(()) => {
                self.slots.release(order);
                let mut touched = TouchedPrices::default();
                touched.add(side, price);
                Outcome {
                    level_changes: touched.resolve(self),
                    ..Outcome::default()
                }
            }
            Err(error) => Outcome::rejected(map_error(error)),
        }
    }

    pub fn amend_down(&mut self, order: OrderId, quantity: Quantity) -> Outcome {
        let Some(slot) = self.slots.slot_of(order) else {
            return Outcome::rejected(RejectReason::UnknownOrderId);
        };
        let Ok(engine_quantity) = u32::try_from(quantity) else {
            return Outcome::rejected(RejectReason::QuantityTooLarge);
        };
        let Some(view) = self.engine.order(slot) else {
            return Outcome::rejected(RejectReason::UnknownOrderId);
        };
        let side = wire_side(view.side);
        let price = view.price;

        match self.engine.amend_down(slot, engine_quantity) {
            Ok(()) => {
                if engine_quantity == 0 {
                    self.slots.release(order);
                }
                let mut touched = TouchedPrices::default();
                touched.add(side, price);
                Outcome {
                    level_changes: touched.resolve(self),
                    ..Outcome::default()
                }
            }
            Err(error) => Outcome::rejected(map_error(error)),
        }
    }

    /// Frees allocator slots for makers the engine fully consumed.
    fn reap_dead_slots(&mut self, executions: &[Execution]) {
        for execution in executions {
            if let Some(slot) = self.slots.slot_of(execution.resting_order)
                && !self.engine.contains(slot)
            {
                self.slots.release(execution.resting_order);
            }
        }
    }
}

/// The prices one command could have changed. Small and duplicated-heavy, so a
/// vector with a dedup beats a set.
#[derive(Debug, Default)]
struct TouchedPrices {
    prices: Vec<(Side, u16)>,
}

impl TouchedPrices {
    fn add(&mut self, side: Side, slot: u16) {
        if !self.prices.contains(&(side, slot)) {
            self.prices.push((side, slot));
        }
    }

    /// Re-reads the aggregate at each touched price. Prices that are still
    /// empty and were empty before produce a zero delta, which is harmless and
    /// far cheaper than tracking prior state.
    fn resolve(self, book: &Book) -> Vec<LevelChange> {
        self.prices
            .into_iter()
            .map(|(side, slot)| LevelChange {
                side,
                price: book.instrument.to_price(slot),
                quantity: book.engine.level_quantity(engine_side(side), slot),
            })
            .collect()
    }
}

const fn engine_side(side: Side) -> ESide {
    match side {
        Side::Bid => ESide::Bid,
        Side::Ask => ESide::Ask,
    }
}

const fn wire_side(side: ESide) -> Side {
    match side {
        ESide::Bid => Side::Bid,
        ESide::Ask => Side::Ask,
    }
}

const fn engine_tif(tif: TimeInForce) -> ETif {
    match tif {
        TimeInForce::GoodTillCancel => ETif::GoodTillCancel,
        TimeInForce::ImmediateOrCancel => ETif::ImmediateOrCancel,
        TimeInForce::FillOrKill => ETif::FillOrKill,
        TimeInForce::PostOnly => ETif::PostOnly,
    }
}

const fn map_error(error: OrderError) -> RejectReason {
    match error {
        OrderError::QuantityZero => RejectReason::QuantityZero,
        OrderError::OrderIdOutOfRange | OrderError::CapacityExceeded => {
            RejectReason::EngineCapacity
        }
        OrderError::DuplicateOrderId => RejectReason::DuplicateOrderId,
        OrderError::UnknownOrderId => RejectReason::UnknownOrderId,
        OrderError::WouldCross => RejectReason::WouldCross,
        OrderError::InsufficientLiquidity => RejectReason::InsufficientLiquidity,
        OrderError::QuantityIncreaseNotAllowed => RejectReason::AmendWouldIncrease,
        OrderError::ReplacementIdMustDiffer | OrderError::UnsupportedTimeInForce => {
            RejectReason::UnsupportedTimeInForce
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> Book {
        Book::new(Instrument::new(1, 10, 20, 1_000, 1_000_000), 1_024)
    }

    fn rest(book: &mut Book, order: OrderId, side: Side, price: Ticks, qty: Quantity) -> Outcome {
        book.submit(order, side, price, qty, TimeInForce::GoodTillCancel, false)
    }

    #[test]
    fn a_resting_order_shows_up_in_depth_and_as_a_level_change() {
        let mut book = book();
        let outcome = rest(&mut book, 1, Side::Bid, 1_500, 10);

        assert!(outcome.reject.is_none());
        assert_eq!(outcome.resting_quantity, 10);
        assert!(outcome.executions.is_empty());
        assert!(
            outcome.level_changes.contains(&LevelChange {
                side: Side::Bid,
                price: 1_500,
                quantity: 10
            }),
            "the level the order rested at must be reported: {:?}",
            outcome.level_changes
        );
        assert_eq!(book.depth(Side::Bid, 10), vec![(1_500, 10)]);
        assert_eq!(book.best_bid(), Some(1_500));
    }

    #[test]
    fn a_crossing_order_executes_at_the_resting_price() {
        let mut book = book();
        rest(&mut book, 1, Side::Ask, 1_500, 10);
        let outcome = rest(&mut book, 2, Side::Bid, 1_600, 4);

        assert_eq!(outcome.executions.len(), 1);
        let execution = outcome.executions[0];
        assert_eq!(execution.resting_order, 1, "maker is reported by client ID");
        assert_eq!(execution.price, 1_500, "trade prints at the maker's price");
        assert_eq!(execution.quantity, 4);
        assert_eq!(book.level_quantity(Side::Ask, 1_500), 6);
    }

    #[test]
    fn emptying_a_level_reports_it_as_zero_so_subscribers_can_remove_it() {
        let mut book = book();
        rest(&mut book, 1, Side::Ask, 1_500, 10);
        let outcome = rest(&mut book, 2, Side::Bid, 1_500, 10);

        assert!(
            outcome.level_changes.contains(&LevelChange {
                side: Side::Ask,
                price: 1_500,
                quantity: 0
            }),
            "an emptied level must be published as zero: {:?}",
            outcome.level_changes
        );
        assert!(book.depth(Side::Ask, 10).is_empty());
    }

    #[test]
    fn prices_outside_the_band_are_rejected() {
        let mut book = book();
        assert_eq!(
            rest(&mut book, 1, Side::Bid, 999, 10).reject,
            Some(RejectReason::OutsidePriceBand)
        );
        assert_eq!(
            rest(&mut book, 2, Side::Bid, 1_000 + 65_536, 10).reject,
            Some(RejectReason::OutsidePriceBand)
        );
    }

    #[test]
    fn a_quantity_the_engine_cannot_hold_is_rejected_not_truncated() {
        let mut book = book();
        let huge = u64::from(u32::MAX) + 1;
        assert_eq!(
            rest(&mut book, 1, Side::Bid, 1_500, huge).reject,
            Some(RejectReason::QuantityTooLarge)
        );
        // Nothing was left behind by the failed attempt.
        assert_eq!(book.live_orders(), 0);
        assert!(book.depth(Side::Bid, 10).is_empty());
    }

    #[test]
    fn a_duplicate_order_id_is_refused() {
        let mut book = book();
        rest(&mut book, 7, Side::Bid, 1_500, 10);
        assert_eq!(
            rest(&mut book, 7, Side::Bid, 1_501, 5).reject,
            Some(RejectReason::DuplicateOrderId)
        );
    }

    #[test]
    fn cancelling_frees_the_slot_so_the_id_space_does_not_leak() {
        let mut book = book();
        for i in 0..500 {
            rest(&mut book, i, Side::Bid, 1_500, 1);
        }
        assert_eq!(book.live_orders(), 500);
        for i in 0..500 {
            assert!(book.cancel(i).reject.is_none());
        }
        assert_eq!(book.live_orders(), 0, "slots must return to the pool");
        // And the book is genuinely empty.
        assert!(book.depth(Side::Bid, 10).is_empty());
    }

    #[test]
    fn fully_filled_makers_release_their_slots() {
        let mut book = book();
        for i in 0..100 {
            rest(&mut book, i, Side::Ask, 1_500, 1);
        }
        assert_eq!(book.live_orders(), 100);
        // Sweep every one of them.
        let outcome = rest(&mut book, 999, Side::Bid, 1_500, 100);
        assert_eq!(outcome.executions.len(), 100);
        assert_eq!(book.live_orders(), 0, "makers and taker should all be gone");
    }

    #[test]
    fn amending_down_keeps_the_order_and_reports_the_new_level() {
        let mut book = book();
        rest(&mut book, 1, Side::Bid, 1_500, 10);
        let outcome = book.amend_down(1, 4);
        assert!(outcome.reject.is_none());
        assert_eq!(book.level_quantity(Side::Bid, 1_500), 4);
        assert!(outcome.level_changes.contains(&LevelChange {
            side: Side::Bid,
            price: 1_500,
            quantity: 4
        }));
    }

    #[test]
    fn amending_up_is_refused() {
        let mut book = book();
        rest(&mut book, 1, Side::Bid, 1_500, 10);
        assert_eq!(
            book.amend_down(1, 20).reject,
            Some(RejectReason::AmendWouldIncrease)
        );
        assert_eq!(book.level_quantity(Side::Bid, 1_500), 10);
    }

    #[test]
    fn cancelling_an_unknown_order_is_refused() {
        let mut book = book();
        assert_eq!(book.cancel(404).reject, Some(RejectReason::UnknownOrderId));
    }

    #[test]
    fn a_market_order_sweeps_regardless_of_price() {
        let mut book = book();
        rest(&mut book, 1, Side::Ask, 1_500, 5);
        rest(&mut book, 2, Side::Ask, 1_600, 5);
        let outcome = book.submit(9, Side::Bid, 0, 8, TimeInForce::ImmediateOrCancel, true);
        assert_eq!(outcome.traded_quantity(), 8);
        assert_eq!(outcome.executions[0].price, 1_500, "best price first");
        assert_eq!(outcome.executions[1].price, 1_600);
    }
}
