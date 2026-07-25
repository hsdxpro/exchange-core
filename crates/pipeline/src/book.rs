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

use crate::fastmap::FastMap;
use crate::instrument::Instrument;
use bx_engine::{L3Book, OrderError, Side as ESide, TimeInForce as ETif};
use bx_protocol::{OrderId, Quantity, RejectReason, Side, Ticks, TimeInForce};

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
    /// Prices this command could have moved. Scratch, reused across commands.
    touched: Vec<(Side, u16)>,
}

impl Outcome {
    /// Empties the buffers without releasing their capacity, so a steady-state
    /// pipeline allocates nothing. Capacity settles at the high-water mark of
    /// whatever traffic actually arrived rather than a guessed reserve.
    pub fn clear(&mut self) {
        self.reject = None;
        self.executions.clear();
        self.level_changes.clear();
        self.touched.clear();
        self.resting_quantity = 0;
    }

    fn reject_with(&mut self, reason: RejectReason) {
        self.clear();
        self.reject = Some(reason);
    }

    fn touch(&mut self, side: Side, slot: u16) {
        if !self.touched.contains(&(side, slot)) {
            self.touched.push((side, slot));
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
    to_slot: FastMap<OrderId, u32>,
    to_order: Vec<OrderId>,
    free: Vec<u32>,
    capacity: u32,
}

impl SlotAllocator {
    fn new(capacity: u32) -> Self {
        Self {
            to_slot: FastMap::default(),
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

    /// Submits a new order, writing the result into `out`.
    ///
    /// The caller owns the buffer and reuses it, so a steady-state pipeline
    /// performs no allocation on this path.
    // One argument over clippy's threshold. Grouping them into a struct would
    // add a type whose only purpose is to satisfy a lint.
    #[allow(clippy::too_many_arguments)]
    pub fn submit_into(
        &mut self,
        out: &mut Outcome,
        order: OrderId,
        side: Side,
        price: Ticks,
        quantity: Quantity,
        tif: TimeInForce,
        market: bool,
    ) {
        out.clear();

        let Ok(engine_quantity) = u32::try_from(quantity) else {
            out.reject_with(RejectReason::QuantityTooLarge);
            return;
        };
        if engine_quantity == 0 {
            out.reject_with(RejectReason::QuantityZero);
            return;
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
                None => {
                    out.reject_with(RejectReason::OutsidePriceBand);
                    return;
                }
            }
        };

        let Some(engine_order) = self.slots.allocate(order) else {
            out.reject_with(RejectReason::DuplicateOrderId);
            return;
        };

        out.touch(side.opposite(), slot);
        out.touch(side, slot);

        // The fill callback needs the buffers while the engine holds `self`,
        // so they are moved out and put back. A `Vec` is three words; this
        // costs nothing and keeps the borrow checker satisfied without unsafe.
        let mut executions = std::mem::take(&mut out.executions);
        let mut touched = std::mem::take(&mut out.touched);

        let result = self.engine.submit_limit_with(
            engine_order,
            engine_side(side),
            slot,
            engine_quantity,
            engine_tif(tif),
            |fill| {
                let key = (side.opposite(), fill.price);
                if !touched.contains(&key) {
                    touched.push(key);
                }
                executions.push(Execution {
                    // Engine slot index for now; resolved to the client's order
                    // ID below, once the borrow has ended.
                    resting_order: u64::from(fill.maker_order_id),
                    resting_side: side.opposite(),
                    price: i64::from(fill.price),
                    quantity: u64::from(fill.quantity),
                });
            },
        );

        out.executions = executions;
        out.touched = touched;

        match result {
            Ok(report) => {
                for index in 0..out.executions.len() {
                    let execution = &out.executions[index];
                    let engine_slot = u32::try_from(execution.resting_order).unwrap_or(0);
                    let client_order = self.slots.order_of(engine_slot);
                    let price = self.instrument.to_price(execution.price as u16);
                    out.executions[index].resting_order = client_order;
                    out.executions[index].price = price;
                    // A maker the engine consumed entirely frees its slot.
                    if !self.engine.contains(engine_slot) {
                        self.slots.release(client_order);
                    }
                }
                if report.rested_quantity == 0 {
                    self.slots.release(order);
                }
                out.resting_quantity = u64::from(report.rested_quantity);
                self.resolve_levels(out);
            }
            Err(error) => {
                self.slots.release(order);
                out.reject_with(map_error(error));
            }
        }
    }

    pub fn cancel_into(&mut self, out: &mut Outcome, order: OrderId) {
        out.clear();
        let Some(slot) = self.slots.slot_of(order) else {
            out.reject_with(RejectReason::UnknownOrderId);
            return;
        };
        let Some(view) = self.engine.order(slot) else {
            out.reject_with(RejectReason::UnknownOrderId);
            return;
        };
        let (side, price) = (wire_side(view.side), view.price);

        match self.engine.cancel(slot) {
            Ok(()) => {
                self.slots.release(order);
                out.touch(side, price);
                self.resolve_levels(out);
            }
            Err(error) => out.reject_with(map_error(error)),
        }
    }

    pub fn amend_down_into(&mut self, out: &mut Outcome, order: OrderId, quantity: Quantity) {
        out.clear();
        let Some(slot) = self.slots.slot_of(order) else {
            out.reject_with(RejectReason::UnknownOrderId);
            return;
        };
        let Ok(engine_quantity) = u32::try_from(quantity) else {
            out.reject_with(RejectReason::QuantityTooLarge);
            return;
        };
        let Some(view) = self.engine.order(slot) else {
            out.reject_with(RejectReason::UnknownOrderId);
            return;
        };
        let (side, price) = (wire_side(view.side), view.price);

        match self.engine.amend_down(slot, engine_quantity) {
            Ok(()) => {
                if engine_quantity == 0 {
                    self.slots.release(order);
                }
                out.touch(side, price);
                self.resolve_levels(out);
            }
            Err(error) => out.reject_with(map_error(error)),
        }
    }

    /// Re-reads the aggregate at each touched price. A price that was empty and
    /// stayed empty yields a zero delta, which is harmless and far cheaper than
    /// tracking prior state.
    fn resolve_levels(&self, out: &mut Outcome) {
        for index in 0..out.touched.len() {
            let (side, slot) = out.touched[index];
            out.level_changes.push(LevelChange {
                side,
                price: self.instrument.to_price(slot),
                quantity: self.engine.level_quantity(engine_side(side), slot),
            });
        }
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
        let mut out = Outcome::default();
        book.submit_into(
            &mut out,
            order,
            side,
            price,
            qty,
            TimeInForce::GoodTillCancel,
            false,
        );
        out
    }

    fn cancel(book: &mut Book, order: OrderId) -> Outcome {
        let mut out = Outcome::default();
        book.cancel_into(&mut out, order);
        out
    }

    fn amend(book: &mut Book, order: OrderId, quantity: Quantity) -> Outcome {
        let mut out = Outcome::default();
        book.amend_down_into(&mut out, order, quantity);
        out
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
            assert!(cancel(&mut book, i).reject.is_none());
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
        let outcome = amend(&mut book, 1, 4);
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
            amend(&mut book, 1, 20).reject,
            Some(RejectReason::AmendWouldIncrease)
        );
        assert_eq!(book.level_quantity(Side::Bid, 1_500), 10);
    }

    #[test]
    fn cancelling_an_unknown_order_is_refused() {
        let mut book = book();
        assert_eq!(
            cancel(&mut book, 404).reject,
            Some(RejectReason::UnknownOrderId)
        );
    }

    #[test]
    fn a_market_order_sweeps_regardless_of_price() {
        let mut book = book();
        rest(&mut book, 1, Side::Ask, 1_500, 5);
        rest(&mut book, 2, Side::Ask, 1_600, 5);
        let mut outcome = Outcome::default();
        book.submit_into(
            &mut outcome,
            9,
            Side::Bid,
            0,
            8,
            TimeInForce::ImmediateOrCancel,
            true,
        );
        assert_eq!(outcome.traded_quantity(), 8);
        assert_eq!(outcome.executions[0].price, 1_500, "best price first");
        assert_eq!(outcome.executions[1].price, 1_600);
    }
}
