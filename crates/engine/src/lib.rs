#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]

//! A compact single-instrument L2/L3 order book built around a hierarchical
//! occupancy bitmap and strict price-time priority.
//!
//! The core is allocation-free after construction. Fill delivery uses a caller
//! callback, so the matching path does not allocate trade-report containers.

pub mod verify;

use core::fmt;
use core::mem::size_of;

pub const PRICE_COUNT: usize = 1 << 16;

/// Width of the `u64` words backing every tier of the occupancy bitmap, with
/// the derived shift and mask used to split a price into word and bit index.
const BITS_PER_WORD: usize = u64::BITS as usize;
const WORD_INDEX_SHIFT: usize = BITS_PER_WORD.trailing_zeros() as usize;
const WORD_BIT_MASK: usize = BITS_PER_WORD - 1;

/// `OrderSlot` packs side and price into one word: side in bit 16, price below.
const SIDE_SHIFT: u32 = 16;
const PRICE_MASK: u32 = (PRICE_COUNT - 1) as u32;

/// A market order is a limit order at the most aggressive representable tick.
/// These are the ends of the ladder, not sentinels: a market buy crosses every
/// ask, a market sell crosses every bid.
const MOST_AGGRESSIVE_BID_TICK: u16 = u16::MAX;
const MOST_AGGRESSIVE_ASK_TICK: u16 = 0;

/// Arbitrary non-zero seed so an empty book does not hash to `mix64(0)`.
pub(crate) const STATE_HASH_SEED: u64 = 7;
pub const INVALID_INDEX: u32 = u32::MAX;
pub const NO_BID: i32 = -1;
pub const NO_ASK: i32 = PRICE_COUNT as i32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Side {
    Bid = 0,
    Ask = 1,
}

impl Side {
    #[must_use]
    pub const fn index(self) -> usize {
        self as usize
    }

    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Bid => Self::Ask,
            Self::Ask => Self::Bid,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TimeInForce {
    GoodTillCancel,
    ImmediateOrCancel,
    FillOrKill,
    PostOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum OrderError {
    QuantityZero,
    OrderIdOutOfRange,
    DuplicateOrderId,
    UnknownOrderId,
    CapacityExceeded,
    WouldCross,
    InsufficientLiquidity,
    QuantityIncreaseNotAllowed,
    ReplacementIdMustDiffer,
    UnsupportedTimeInForce,
}

impl fmt::Display for OrderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::QuantityZero => "order quantity must be greater than zero",
            Self::OrderIdOutOfRange => "order ID is outside the configured ID space",
            Self::DuplicateOrderId => "order ID is already live",
            Self::UnknownOrderId => "order ID is not live",
            Self::CapacityExceeded => "resting-order capacity is exhausted",
            Self::WouldCross => "post-only or direct resting order would cross the book",
            Self::InsufficientLiquidity => "fill-or-kill order cannot be filled completely",
            Self::QuantityIncreaseNotAllowed => {
                "an amend cannot increase quantity; use cancel/replace"
            }
            Self::ReplacementIdMustDiffer => {
                "replacement order ID must differ from the original order ID"
            }
            Self::UnsupportedTimeInForce => "unsupported time-in-force for this order type",
        })
    }
}

impl std::error::Error for OrderError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigurationError;

impl fmt::Display for ConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("resting-order capacity exceeds the 32-bit slot index space")
    }
}

impl std::error::Error for ConfigurationError {}

#[must_use]
/// The SplitMix64 finalizer (Steele, Lea and Flood, 2014). Used for state and
/// report hashes and by the deterministic test generators; the constants are
/// the published ones, not tuned here.
pub fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Clone, Debug)]
pub struct HierarchicalBitmap {
    leaf: [u64; Self::LEAF_WORD_COUNT],
    summary: [u64; Self::SUMMARY_WORD_COUNT],
    root: u64,
}

impl HierarchicalBitmap {
    pub const LEAF_WORD_COUNT: usize = PRICE_COUNT / BITS_PER_WORD;
    pub const SUMMARY_WORD_COUNT: usize = Self::LEAF_WORD_COUNT / BITS_PER_WORD;

    #[must_use]
    pub const fn new() -> Self {
        Self {
            leaf: [0; Self::LEAF_WORD_COUNT],
            summary: [0; Self::SUMMARY_WORD_COUNT],
            root: 0,
        }
    }

    pub fn clear(&mut self) {
        self.leaf.fill(0);
        self.summary.fill(0);
        self.root = 0;
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.root == 0
    }

    #[must_use]
    pub fn contains(&self, price: u16) -> bool {
        let value = usize::from(price);
        ((self.leaf[value >> WORD_INDEX_SHIFT] >> (value & WORD_BIT_MASK)) & 1) != 0
    }

    pub fn insert(&mut self, price: u16) {
        let value = usize::from(price);
        let leaf_index = value >> WORD_INDEX_SHIFT;
        let leaf_bit = 1_u64 << (value & WORD_BIT_MASK);
        let old_leaf = self.leaf[leaf_index];
        if old_leaf & leaf_bit != 0 {
            return;
        }

        self.leaf[leaf_index] = old_leaf | leaf_bit;
        if old_leaf != 0 {
            return;
        }

        let summary_index = leaf_index >> WORD_INDEX_SHIFT;
        let summary_bit = 1_u64 << (leaf_index & WORD_BIT_MASK);
        let old_summary = self.summary[summary_index];
        self.summary[summary_index] = old_summary | summary_bit;
        if old_summary == 0 {
            self.root |= 1_u64 << summary_index;
        }
    }

    pub fn remove(&mut self, price: u16) {
        let value = usize::from(price);
        let leaf_index = value >> WORD_INDEX_SHIFT;
        let leaf_bit = 1_u64 << (value & WORD_BIT_MASK);
        let old_leaf = self.leaf[leaf_index];
        if old_leaf & leaf_bit == 0 {
            return;
        }

        let next_leaf = old_leaf & !leaf_bit;
        self.leaf[leaf_index] = next_leaf;
        if next_leaf != 0 {
            return;
        }

        let summary_index = leaf_index >> WORD_INDEX_SHIFT;
        let summary_bit = 1_u64 << (leaf_index & WORD_BIT_MASK);
        let next_summary = self.summary[summary_index] & !summary_bit;
        self.summary[summary_index] = next_summary;
        if next_summary == 0 {
            self.root &= !(1_u64 << summary_index);
        }
    }

    #[must_use]
    pub fn first(&self) -> i32 {
        if self.root == 0 {
            return NO_ASK;
        }
        let summary_index = least_bit(self.root);
        let leaf_offset = least_bit(self.summary[summary_index]);
        let leaf_index = (summary_index << 6) + leaf_offset;
        ((leaf_index << 6) + least_bit(self.leaf[leaf_index])) as i32
    }

    #[must_use]
    pub fn last(&self) -> i32 {
        if self.root == 0 {
            return NO_BID;
        }
        let summary_index = greatest_bit(self.root);
        let leaf_offset = greatest_bit(self.summary[summary_index]);
        let leaf_index = (summary_index << 6) + leaf_offset;
        ((leaf_index << 6) + greatest_bit(self.leaf[leaf_index])) as i32
    }

    #[must_use]
    pub fn next(&self, price: u16) -> i32 {
        if price == u16::MAX {
            return NO_ASK;
        }

        let value = usize::from(price) + 1;
        let mut leaf_index = value >> WORD_INDEX_SHIFT;
        let local = self.leaf[leaf_index] & (u64::MAX << (value & WORD_BIT_MASK));
        if local != 0 {
            return ((leaf_index << 6) + least_bit(local)) as i32;
        }

        let mut summary_index = leaf_index >> WORD_INDEX_SHIFT;
        let summary_start = (leaf_index & WORD_BIT_MASK) + 1;
        let mut leaves = if summary_start < BITS_PER_WORD {
            self.summary[summary_index] & (u64::MAX << summary_start)
        } else {
            0
        };
        if leaves == 0 {
            let root_start = summary_index + 1;
            let summaries = if root_start < BITS_PER_WORD {
                self.root & (u64::MAX << root_start)
            } else {
                0
            };
            if summaries == 0 {
                return NO_ASK;
            }
            summary_index = least_bit(summaries);
            leaves = self.summary[summary_index];
        }

        leaf_index = (summary_index << 6) + least_bit(leaves);
        ((leaf_index << 6) + least_bit(self.leaf[leaf_index])) as i32
    }

    #[must_use]
    pub fn previous(&self, price: u16) -> i32 {
        if price == 0 {
            return NO_BID;
        }

        let value = usize::from(price) - 1;
        let mut leaf_index = value >> WORD_INDEX_SHIFT;
        let local_bit = value & WORD_BIT_MASK;
        let local_mask = if local_bit == WORD_BIT_MASK {
            u64::MAX
        } else {
            (1_u64 << (local_bit + 1)) - 1
        };
        let local = self.leaf[leaf_index] & local_mask;
        if local != 0 {
            return ((leaf_index << 6) + greatest_bit(local)) as i32;
        }

        let mut summary_index = leaf_index >> WORD_INDEX_SHIFT;
        let summary_bit = leaf_index & WORD_BIT_MASK;
        let mut leaves = if summary_bit == 0 {
            0
        } else {
            self.summary[summary_index] & ((1_u64 << summary_bit) - 1)
        };
        if leaves == 0 {
            let summaries = if summary_index == 0 {
                0
            } else {
                self.root & ((1_u64 << summary_index) - 1)
            };
            if summaries == 0 {
                return NO_BID;
            }
            summary_index = greatest_bit(summaries);
            leaves = self.summary[summary_index];
        }

        leaf_index = (summary_index << 6) + greatest_bit(leaves);
        ((leaf_index << 6) + greatest_bit(self.leaf[leaf_index])) as i32
    }

    /// Checks that the summary and root tiers agree with the leaf words.
    ///
    /// Diagnostic only: scans all 1,024 leaf words.
    #[must_use]
    pub fn validate(&self) -> bool {
        let mut expected_root = 0_u64;
        for summary_index in 0..Self::SUMMARY_WORD_COUNT {
            let mut expected_summary = 0_u64;
            for offset in 0..BITS_PER_WORD {
                let leaf_index = (summary_index << 6) + offset;
                if self.leaf[leaf_index] != 0 {
                    expected_summary |= 1_u64 << offset;
                }
            }
            if self.summary[summary_index] != expected_summary {
                return false;
            }
            if expected_summary != 0 {
                expected_root |= 1_u64 << summary_index;
            }
        }
        self.root == expected_root
    }
}

impl Default for HierarchicalBitmap {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn least_bit(value: u64) -> usize {
    value.trailing_zeros() as usize
}

#[inline]
fn greatest_bit(value: u64) -> usize {
    BITS_PER_WORD - 1 - value.leading_zeros() as usize
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SweepResult {
    pub requested_quantity: u64,
    pub filled_quantity: u64,
    pub notional_ticks: u64,
    pub levels_visited: u32,
}

impl SweepResult {
    #[must_use]
    pub fn average_price(self) -> f64 {
        if self.filled_quantity == 0 {
            0.0
        } else {
            self.notional_ticks as f64 / self.filled_quantity as f64
        }
    }
}

#[derive(Clone, Debug)]
struct L2Side {
    quantity: Box<[u32]>,
    active: HierarchicalBitmap,
    best: i32,
    total_quantity: u64,
}

impl L2Side {
    fn new(best: i32) -> Self {
        Self {
            quantity: vec![0; PRICE_COUNT].into_boxed_slice(),
            active: HierarchicalBitmap::new(),
            best,
            total_quantity: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct L2Book {
    sides: [L2Side; 2],
}

impl L2Book {
    #[must_use]
    pub fn new() -> Self {
        Self {
            sides: [L2Side::new(NO_BID), L2Side::new(NO_ASK)],
        }
    }

    pub fn clear(&mut self) {
        self.sides[0].quantity.fill(0);
        self.sides[0].active.clear();
        self.sides[0].best = NO_BID;
        self.sides[0].total_quantity = 0;

        self.sides[1].quantity.fill(0);
        self.sides[1].active.clear();
        self.sides[1].best = NO_ASK;
        self.sides[1].total_quantity = 0;
    }

    pub fn set_level(&mut self, side: Side, price: u16, quantity: u32) {
        let state = &mut self.sides[side.index()];
        let price_index = usize::from(price);
        let old_quantity = state.quantity[price_index];
        if old_quantity == quantity {
            return;
        }

        state.quantity[price_index] = quantity;
        state.total_quantity = state.total_quantity - u64::from(old_quantity) + u64::from(quantity);

        if old_quantity == 0 && quantity != 0 {
            state.active.insert(price);
            match side {
                Side::Bid => state.best = state.best.max(i32::from(price)),
                Side::Ask => state.best = state.best.min(i32::from(price)),
            }
        } else if old_quantity != 0 && quantity == 0 {
            state.active.remove(price);
            if state.best == i32::from(price) {
                state.best = match side {
                    Side::Bid => state.active.last(),
                    Side::Ask => state.active.first(),
                };
            }
        }
    }

    #[must_use]
    pub fn quantity(&self, side: Side, price: u16) -> u32 {
        self.sides[side.index()].quantity[usize::from(price)]
    }

    #[must_use]
    pub fn total_quantity(&self, side: Side) -> u64 {
        self.sides[side.index()].total_quantity
    }

    #[must_use]
    pub fn best_bid(&self) -> i32 {
        self.sides[Side::Bid.index()].best
    }

    #[must_use]
    pub fn best_ask(&self) -> i32 {
        self.sides[Side::Ask.index()].best
    }

    pub fn for_each_top<F>(&self, side: Side, limit: usize, mut visitor: F) -> usize
    where
        F: FnMut(u16, u32),
    {
        let state = &self.sides[side.index()];
        let mut price = state.best;
        let mut visited = 0;
        while visited < limit && valid_price(price) {
            visitor(price as u16, state.quantity[price as usize]);
            visited += 1;
            price = match side {
                Side::Bid => state.active.previous(price as u16),
                Side::Ask => state.active.next(price as u16),
            };
        }
        visited
    }

    #[must_use]
    pub fn sweep(&self, side: Side, target_quantity: u64) -> SweepResult {
        let state = &self.sides[side.index()];
        let mut result = SweepResult {
            requested_quantity: target_quantity,
            ..SweepResult::default()
        };
        let mut remaining = target_quantity;
        let mut price = state.best;

        while remaining != 0 && valid_price(price) {
            let available = u64::from(state.quantity[price as usize]);
            let fill = remaining.min(available);
            remaining -= fill;
            result.filled_quantity += fill;
            result.notional_ticks += fill * price as u64;
            result.levels_visited += 1;
            price = match side {
                Side::Bid => state.active.previous(price as u16),
                Side::Ask => state.active.next(price as u16),
            };
        }
        result
    }

    #[must_use]
    pub fn top_checksum(&self, side: Side, limit: usize) -> u64 {
        let mut hash = 0_u64;
        let mut rank = 1_u64;
        self.for_each_top(side, limit, |price, quantity| {
            hash = mix64(hash ^ (u64::from(price) << 32) ^ u64::from(quantity) ^ rank);
            rank += 1;
        });
        hash
    }

    /// Hash of every occupied level, for differential comparison.
    ///
    /// Diagnostic only: walks all 65,536 prices on both sides.
    #[must_use]
    pub fn state_hash(&self) -> u64 {
        let mut hash = mix64((self.best_bid() + 1) as u64) ^ mix64((self.best_ask() + 1) as u64);
        for side in 0..2 {
            for price in 0..PRICE_COUNT {
                let quantity = self.sides[side].quantity[price];
                if quantity != 0 {
                    hash = mix64(
                        hash ^ ((side as u64) << 63) ^ ((price as u64) << 32) ^ u64::from(quantity),
                    );
                }
            }
        }
        hash
    }

    /// Checks bitmap, cached BBO, and total-quantity consistency.
    ///
    /// Diagnostic only: walks all 65,536 prices on both sides.
    #[must_use]
    pub fn validate(&self) -> bool {
        for side in 0..2 {
            let state = &self.sides[side];
            if !state.active.validate() {
                return false;
            }
            let mut total = 0_u64;
            for price in 0..PRICE_COUNT {
                let quantity = state.quantity[price];
                total += u64::from(quantity);
                if state.active.contains(price as u16) != (quantity != 0) {
                    return false;
                }
            }
            if total != state.total_quantity {
                return false;
            }
        }
        self.best_bid() == self.sides[0].active.last()
            && self.best_ask() == self.sides[1].active.first()
    }
}

impl Default for L2Book {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct OrderSlot {
    next: u32,
    previous: u32,
    id: u32,
    quantity: u32,
    free_next: u32,
    price_side: u32,
}

impl OrderSlot {
    const fn empty() -> Self {
        Self {
            next: INVALID_INDEX,
            previous: INVALID_INDEX,
            id: 0,
            quantity: 0,
            free_next: INVALID_INDEX,
            price_side: 0,
        }
    }

    #[inline]
    fn side(self) -> Side {
        debug_assert!(self.side_code() <= 1);
        if self.side_code() == 0 {
            Side::Bid
        } else {
            Side::Ask
        }
    }

    #[inline]
    fn side_code(self) -> u32 {
        self.price_side >> SIDE_SHIFT
    }

    #[inline]
    fn price(self) -> u16 {
        (self.price_side & PRICE_MASK) as u16
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct PriceLevel {
    head: u32,
    tail: u32,
    total_quantity: u64,
}

impl PriceLevel {
    const fn empty() -> Self {
        Self {
            head: INVALID_INDEX,
            tail: INVALID_INDEX,
            total_quantity: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderView {
    pub id: u32,
    pub side: Side,
    pub price: u16,
    pub quantity: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fill {
    pub maker_order_id: u32,
    pub maker_side: Side,
    pub price: u16,
    pub quantity: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecutionReport {
    pub fills: u32,
    pub rested_quantity: u32,
    pub canceled_quantity: u32,
    pub traded_quantity: u64,
    pub notional_ticks: u64,
    pub report_hash: u64,
}

impl ExecutionReport {
    #[must_use]
    pub fn average_price(self) -> f64 {
        if self.traded_quantity == 0 {
            0.0
        } else {
            self.notional_ticks as f64 / self.traded_quantity as f64
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Lookup {
    slot_index: u32,
}

#[derive(Clone, Copy, Debug, Default)]
struct LiquidityPreview {
    executable_quantity: u32,
    releases_slot: bool,
}

#[derive(Clone, Debug)]
pub struct L3Book {
    levels: [Vec<PriceLevel>; 2],
    slots: Vec<OrderSlot>,
    id_to_slot: Vec<u32>,
    active: [HierarchicalBitmap; 2],
    best: [i32; 2],
    next_unused: u32,
    free_head: u32,
    live_orders: u32,
}

impl L3Book {
    /// Builds a book with a fixed resting-order capacity and a fixed order-ID
    /// space.
    ///
    /// Order IDs index directly into a dense table, so they must be small
    /// contiguous integers in `0..max_order_ids`, not exchange-assigned 64-bit
    /// IDs. A venue with sparse or externally-assigned IDs needs an allocator
    /// in front of this that maps its IDs onto this index space.
    ///
    /// # Panics
    ///
    /// Panics if `max_orders` does not fit the 32-bit slot index space. Use
    /// [`L3Book::try_new`] to handle that as a `Result`.
    #[must_use]
    pub fn new(max_orders: usize, max_order_ids: usize) -> Self {
        Self::try_new(max_orders, max_order_ids)
            .expect("resting-order capacity must fit in the 32-bit slot index space")
    }

    pub fn try_new(max_orders: usize, max_order_ids: usize) -> Result<Self, ConfigurationError> {
        if max_orders >= INVALID_INDEX as usize {
            return Err(ConfigurationError);
        }
        Ok(Self {
            levels: [
                vec![PriceLevel::empty(); PRICE_COUNT],
                vec![PriceLevel::empty(); PRICE_COUNT],
            ],
            slots: vec![OrderSlot::empty(); max_orders],
            id_to_slot: vec![INVALID_INDEX; max_order_ids],
            active: [HierarchicalBitmap::new(), HierarchicalBitmap::new()],
            best: [NO_BID, NO_ASK],
            next_unused: 0,
            free_head: INVALID_INDEX,
            live_orders: 0,
        })
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    #[must_use]
    pub fn order_id_capacity(&self) -> usize {
        self.id_to_slot.len()
    }

    #[must_use]
    pub fn live_orders(&self) -> u32 {
        self.live_orders
    }

    #[must_use]
    pub fn best_bid(&self) -> i32 {
        self.best[Side::Bid.index()]
    }

    #[must_use]
    pub fn best_ask(&self) -> i32 {
        self.best[Side::Ask.index()]
    }

    #[must_use]
    pub fn contains(&self, id: u32) -> bool {
        self.id_to_slot
            .get(id as usize)
            .is_some_and(|&slot_index| slot_index != INVALID_INDEX)
    }

    #[must_use]
    pub fn order(&self, id: u32) -> Option<OrderView> {
        let slot_index = *self.id_to_slot.get(id as usize)?;
        if slot_index == INVALID_INDEX {
            return None;
        }
        let slot = self.slots[slot_index as usize];
        Some(OrderView {
            id: slot.id,
            side: slot.side(),
            price: slot.price(),
            quantity: slot.quantity,
        })
    }

    #[must_use]
    pub fn level_quantity(&self, side: Side, price: u16) -> u64 {
        self.levels[side.index()][usize::from(price)].total_quantity
    }

    #[must_use]
    pub fn would_cross(&self, side: Side, price: u16) -> bool {
        match side {
            Side::Bid => self.best_ask() <= i32::from(price),
            Side::Ask => self.best_bid() >= i32::from(price),
        }
    }

    pub fn add_passive(
        &mut self,
        id: u32,
        side: Side,
        price: u16,
        quantity: u32,
    ) -> Result<(), OrderError> {
        self.validate_new_order(id, quantity)?;
        if self.would_cross(side, price) {
            return Err(OrderError::WouldCross);
        }
        let slot_index = self.allocate_slot().ok_or(OrderError::CapacityExceeded)?;
        self.append(slot_index, id, side, price, quantity);
        Ok(())
    }

    pub fn cancel(&mut self, id: u32) -> Result<(), OrderError> {
        let lookup = self.lookup_order(id)?;
        self.unlink(lookup.slot_index);
        Ok(())
    }

    /// Amend down: reduces open quantity while retaining queue priority. A zero
    /// quantity cancels the order outright. An increase is rejected — that is a
    /// cancel/replace, which necessarily takes new time priority.
    pub fn amend_down(&mut self, id: u32, new_quantity: u32) -> Result<(), OrderError> {
        let lookup = self.lookup_order(id)?;
        let slot_index = lookup.slot_index as usize;
        let slot = self.slots[slot_index];
        if new_quantity > slot.quantity {
            return Err(OrderError::QuantityIncreaseNotAllowed);
        }
        if new_quantity == 0 {
            self.unlink(lookup.slot_index);
            return Ok(());
        }
        if new_quantity == slot.quantity {
            return Ok(());
        }

        let level = &mut self.levels[slot.side().index()][usize::from(slot.price())];
        level.total_quantity -= u64::from(slot.quantity - new_quantity);
        self.slots[slot_index].quantity = new_quantity;
        Ok(())
    }

    pub fn cancel_replace(
        &mut self,
        existing_id: u32,
        replacement_id: u32,
        new_price: u16,
        new_quantity: u32,
    ) -> Result<ExecutionReport, OrderError> {
        self.cancel_replace_with(existing_id, replacement_id, new_price, new_quantity, |_| {})
    }

    pub fn cancel_replace_with<F>(
        &mut self,
        existing_id: u32,
        replacement_id: u32,
        new_price: u16,
        new_quantity: u32,
        on_fill: F,
    ) -> Result<ExecutionReport, OrderError>
    where
        F: FnMut(Fill),
    {
        let existing = self.lookup_order(existing_id)?;
        if replacement_id == existing_id {
            return Err(OrderError::ReplacementIdMustDiffer);
        }
        self.validate_new_order(replacement_id, new_quantity)?;

        let side = self.slots[existing.slot_index as usize].side();
        self.unlink(existing.slot_index);
        self.submit_limit_impl(
            replacement_id,
            side,
            new_price,
            new_quantity,
            TimeInForce::GoodTillCancel,
            on_fill,
            true,
        )
    }

    pub fn submit_limit(
        &mut self,
        id: u32,
        side: Side,
        limit_price: u16,
        quantity: u32,
        time_in_force: TimeInForce,
    ) -> Result<ExecutionReport, OrderError> {
        self.submit_limit_with(id, side, limit_price, quantity, time_in_force, |_| {})
    }

    pub fn submit_limit_with<F>(
        &mut self,
        id: u32,
        side: Side,
        limit_price: u16,
        quantity: u32,
        time_in_force: TimeInForce,
        on_fill: F,
    ) -> Result<ExecutionReport, OrderError>
    where
        F: FnMut(Fill),
    {
        self.submit_limit_impl(
            id,
            side,
            limit_price,
            quantity,
            time_in_force,
            on_fill,
            false,
        )
    }

    pub fn submit_market(
        &mut self,
        id: u32,
        side: Side,
        quantity: u32,
        time_in_force: TimeInForce,
    ) -> Result<ExecutionReport, OrderError> {
        self.submit_market_with(id, side, quantity, time_in_force, |_| {})
    }

    /// Submits a market order.
    ///
    /// A market order is expressed as a limit order at the most aggressive
    /// representable price, so there is exactly one matching loop rather than a
    /// separate market path. Only IOC and FOK are accepted: a resting time in
    /// force would let an unfilled remainder rest at tick 0 or 65,535, which is
    /// never what the sender meant.
    ///
    /// This sweeps without any price protection. A production venue bounds a
    /// market order with a protection limit (a collar around the touch, or a
    /// price band) instead of the domain extreme; that policy is deliberately
    /// outside this engine.
    pub fn submit_market_with<F>(
        &mut self,
        id: u32,
        side: Side,
        quantity: u32,
        time_in_force: TimeInForce,
        on_fill: F,
    ) -> Result<ExecutionReport, OrderError>
    where
        F: FnMut(Fill),
    {
        if !matches!(
            time_in_force,
            TimeInForce::ImmediateOrCancel | TimeInForce::FillOrKill
        ) {
            return Err(OrderError::UnsupportedTimeInForce);
        }
        let market_limit = match side {
            Side::Bid => MOST_AGGRESSIVE_BID_TICK,
            Side::Ask => MOST_AGGRESSIVE_ASK_TICK,
        };
        self.submit_limit_with(id, side, market_limit, quantity, time_in_force, on_fill)
    }

    pub fn for_each_level<F>(&self, side: Side, limit: usize, mut visitor: F) -> usize
    where
        F: FnMut(u16, u64),
    {
        let side_index = side.index();
        let mut price = self.best[side_index];
        let mut visited = 0;
        while visited < limit && valid_price(price) {
            visitor(
                price as u16,
                self.levels[side_index][price as usize].total_quantity,
            );
            visited += 1;
            price = match side {
                Side::Bid => self.active[side_index].previous(price as u16),
                Side::Ask => self.active[side_index].next(price as u16),
            };
        }
        visited
    }

    pub fn for_each_order_at_level<F>(&self, side: Side, price: u16, mut visitor: F) -> usize
    where
        F: FnMut(OrderView),
    {
        let mut slot_index = self.levels[side.index()][usize::from(price)].head;
        let mut visited = 0;
        while slot_index != INVALID_INDEX {
            let slot = self.slots[slot_index as usize];
            visitor(OrderView {
                id: slot.id,
                side,
                price,
                quantity: slot.quantity,
            });
            visited += 1;
            slot_index = slot.next;
        }
        visited
    }

    /// Order-sensitive hash of every resting order, for differential and
    /// replay comparison.
    ///
    /// Diagnostic only: walks all 65,536 prices on both sides.
    #[must_use]
    pub fn state_hash(&self) -> u64 {
        // The shifts spread side, price, ID, quantity, and queue rank into
        // disjoint regions of the word so two different books cannot collide by
        // trading one field's value against another's.
        let mut hash = mix64(u64::from(self.live_orders) + STATE_HASH_SEED);
        for side in 0..2 {
            for price in 0..PRICE_COUNT {
                let mut slot_index = self.levels[side][price].head;
                let mut rank = 1_u64;
                while slot_index != INVALID_INDEX {
                    let slot = self.slots[slot_index as usize];
                    hash = mix64(
                        hash ^ ((side as u64) << 63)
                            ^ ((price as u64) << 40)
                            ^ (u64::from(slot.id) << 8)
                            ^ u64::from(slot.quantity)
                            ^ rank,
                    );
                    rank += 1;
                    slot_index = slot.next;
                }
            }
        }
        hash
    }

    /// Full structural audit: bitmap hierarchy, cached BBO, queue links,
    /// level aggregates, ID mapping, and free-list integrity.
    ///
    /// Diagnostic only. This walks all 65,536 prices on both sides and is
    /// orders of magnitude too slow for a live matching path; it exists for
    /// tests, fuzzing, and post-incident inspection.
    #[must_use]
    pub fn validate(&self) -> bool {
        if size_of::<OrderSlot>() != 24 || size_of::<PriceLevel>() != 16 {
            return false;
        }
        if !self.active[0].validate() || !self.active[1].validate() {
            return false;
        }
        if self.live_orders as usize > self.slots.len()
            || self.next_unused as usize > self.slots.len()
        {
            return false;
        }

        let mut slot_state = vec![0_u8; self.next_unused as usize];
        let mut counted_orders = 0_u64;
        for side in 0..2 {
            for price in 0..PRICE_COUNT {
                let level = self.levels[side][price];
                if (level.head == INVALID_INDEX) != (level.tail == INVALID_INDEX) {
                    return false;
                }

                let mut current = level.head;
                let mut previous = INVALID_INDEX;
                let mut quantity = 0_u64;
                while current != INVALID_INDEX {
                    if current >= self.next_unused || slot_state[current as usize] != 0 {
                        return false;
                    }
                    slot_state[current as usize] = 1;
                    let slot = self.slots[current as usize];
                    if slot.side_code() as usize != side
                        || usize::from(slot.price()) != price
                        || slot.previous != previous
                        || slot.quantity == 0
                        || slot.id as usize >= self.id_to_slot.len()
                        || self.id_to_slot[slot.id as usize] != current
                    {
                        return false;
                    }
                    quantity += u64::from(slot.quantity);
                    counted_orders += 1;
                    previous = current;
                    current = slot.next;
                }

                if previous != level.tail || quantity != level.total_quantity {
                    return false;
                }
                if level.head != INVALID_INDEX
                    && (self.slots[level.head as usize].previous != INVALID_INDEX
                        || self.slots[level.tail as usize].next != INVALID_INDEX)
                {
                    return false;
                }
                if self.active[side].contains(price as u16) != (level.head != INVALID_INDEX) {
                    return false;
                }
            }
        }

        let mut free_slots = 0_u64;
        let mut free_index = self.free_head;
        while free_index != INVALID_INDEX {
            if free_index >= self.next_unused || slot_state[free_index as usize] != 0 {
                return false;
            }
            slot_state[free_index as usize] = 2;
            free_slots += 1;
            free_index = self.slots[free_index as usize].free_next;
        }
        if slot_state.contains(&0) {
            return false;
        }

        if counted_orders != u64::from(self.live_orders)
            || counted_orders + free_slots != u64::from(self.next_unused)
        {
            return false;
        }
        for (id, &slot_index) in self.id_to_slot.iter().enumerate() {
            if slot_index != INVALID_INDEX
                && (slot_index >= self.next_unused
                    || slot_state[slot_index as usize] != 1
                    || self.slots[slot_index as usize].id as usize != id)
            {
                return false;
            }
        }
        if self.best_bid() != self.active[0].last() || self.best_ask() != self.active[1].first() {
            return false;
        }
        self.best_bid() == NO_BID || self.best_ask() == NO_ASK || self.best_bid() < self.best_ask()
    }

    fn validate_new_order(&self, id: u32, quantity: u32) -> Result<(), OrderError> {
        if quantity == 0 {
            return Err(OrderError::QuantityZero);
        }
        let Some(&slot_index) = self.id_to_slot.get(id as usize) else {
            return Err(OrderError::OrderIdOutOfRange);
        };
        if slot_index != INVALID_INDEX {
            return Err(OrderError::DuplicateOrderId);
        }
        Ok(())
    }

    fn lookup_order(&self, id: u32) -> Result<Lookup, OrderError> {
        let Some(&slot_index) = self.id_to_slot.get(id as usize) else {
            return Err(OrderError::OrderIdOutOfRange);
        };
        if slot_index == INVALID_INDEX {
            return Err(OrderError::UnknownOrderId);
        }
        Ok(Lookup { slot_index })
    }

    #[inline]
    fn slot_available(&self) -> bool {
        self.free_head != INVALID_INDEX || (self.next_unused as usize) < self.slots.len()
    }

    fn preview_liquidity(
        &self,
        taker_side: Side,
        limit_price: u16,
        requested_quantity: u32,
    ) -> LiquidityPreview {
        let mut preview = LiquidityPreview::default();
        let mut remaining = requested_quantity;
        let maker_side = taker_side.opposite();
        let maker_side_index = maker_side.index();
        let mut price = match taker_side {
            Side::Bid => self.best_ask(),
            Side::Ask => self.best_bid(),
        };

        while remaining != 0 && valid_price(price) && crosses(taker_side, limit_price, price) {
            let mut slot_index = self.levels[maker_side_index][price as usize].head;
            while remaining != 0 && slot_index != INVALID_INDEX {
                let maker = self.slots[slot_index as usize];
                let fill = remaining.min(maker.quantity);
                remaining -= fill;
                preview.executable_quantity += fill;
                preview.releases_slot |= fill == maker.quantity;
                slot_index = maker.next;
            }
            price = match taker_side {
                Side::Bid => self.active[maker_side_index].next(price as u16),
                Side::Ask => self.active[maker_side_index].previous(price as u16),
            };
        }
        preview
    }

    #[allow(clippy::too_many_arguments)]
    fn submit_limit_impl<F>(
        &mut self,
        id: u32,
        side: Side,
        limit_price: u16,
        quantity: u32,
        time_in_force: TimeInForce,
        mut on_fill: F,
        validation_already_performed: bool,
    ) -> Result<ExecutionReport, OrderError>
    where
        F: FnMut(Fill),
    {
        if !validation_already_performed {
            self.validate_new_order(id, quantity)?;
        }

        if time_in_force == TimeInForce::PostOnly {
            if self.would_cross(side, limit_price) {
                return Err(OrderError::WouldCross);
            }
            let slot_index = self.allocate_slot().ok_or(OrderError::CapacityExceeded)?;
            self.append(slot_index, id, side, limit_price, quantity);
            return Ok(ExecutionReport {
                rested_quantity: quantity,
                ..ExecutionReport::default()
            });
        }

        let preview = if time_in_force == TimeInForce::FillOrKill
            || (time_in_force == TimeInForce::GoodTillCancel && !self.slot_available())
        {
            self.preview_liquidity(side, limit_price, quantity)
        } else {
            LiquidityPreview::default()
        };

        if time_in_force == TimeInForce::FillOrKill && preview.executable_quantity < quantity {
            return Err(OrderError::InsufficientLiquidity);
        }
        if time_in_force == TimeInForce::GoodTillCancel && !self.slot_available() {
            let remainder = quantity - preview.executable_quantity;
            if remainder != 0 && !preview.releases_slot {
                return Err(OrderError::CapacityExceeded);
            }
        }

        let mut report = ExecutionReport::default();
        let mut remaining = quantity;
        let maker_side = side.opposite();
        let maker_side_index = maker_side.index();

        while remaining != 0 {
            let maker_price_value = match side {
                Side::Bid => self.best_ask(),
                Side::Ask => self.best_bid(),
            };
            if !valid_price(maker_price_value) || !crosses(side, limit_price, maker_price_value) {
                break;
            }

            let maker_price = maker_price_value as u16;
            let maker_slot_index = self.levels[maker_side_index][usize::from(maker_price)].head;
            let maker = self.slots[maker_slot_index as usize];
            let fill_quantity = remaining.min(maker.quantity);
            let fill = Fill {
                maker_order_id: maker.id,
                maker_side,
                price: maker_price,
                quantity: fill_quantity,
            };

            remaining -= fill_quantity;
            report.fills += 1;
            report.traded_quantity += u64::from(fill_quantity);
            report.notional_ticks += u64::from(maker_price) * u64::from(fill_quantity);
            // Disjoint shifts again: maker ID, price, quantity, and side must
            // not be able to alias each other in the accumulated report hash.
            report.report_hash = mix64(
                report.report_hash
                    ^ u64::from(maker.id)
                    ^ (u64::from(maker_price) << 32)
                    ^ (u64::from(fill_quantity) << 1)
                    ^ maker_side_index as u64,
            );
            if fill_quantity == maker.quantity {
                self.unlink(maker_slot_index);
            } else {
                self.slots[maker_slot_index as usize].quantity -= fill_quantity;
                self.levels[maker_side_index][usize::from(maker_price)].total_quantity -=
                    u64::from(fill_quantity);
            }

            // Publish only after the fill has been committed. If a callback
            // panics and is caught by the caller, the book remains valid.
            on_fill(fill);
        }

        if remaining != 0 {
            if time_in_force == TimeInForce::GoodTillCancel {
                let slot_index = self
                    .allocate_slot()
                    .expect("GTC capacity preflight invariant must reserve a slot");
                self.append(slot_index, id, side, limit_price, remaining);
                report.rested_quantity = remaining;
            } else {
                report.canceled_quantity = remaining;
            }
        }
        Ok(report)
    }

    fn allocate_slot(&mut self) -> Option<u32> {
        if self.free_head != INVALID_INDEX {
            let slot_index = self.free_head;
            self.free_head = self.slots[slot_index as usize].free_next;
            return Some(slot_index);
        }
        if self.next_unused as usize >= self.slots.len() {
            return None;
        }
        let slot_index = self.next_unused;
        self.next_unused += 1;
        Some(slot_index)
    }

    fn release_slot(&mut self, slot_index: u32) {
        self.slots[slot_index as usize].free_next = self.free_head;
        self.free_head = slot_index;
    }

    fn append(&mut self, slot_index: u32, id: u32, side: Side, price: u16, quantity: u32) {
        let side_index = side.index();
        let price_index = usize::from(price);
        let tail = self.levels[side_index][price_index].tail;
        let was_empty = tail == INVALID_INDEX;

        self.slots[slot_index as usize] = OrderSlot {
            next: INVALID_INDEX,
            previous: tail,
            id,
            quantity,
            free_next: INVALID_INDEX,
            price_side: ((side_index as u32) << SIDE_SHIFT) | u32::from(price),
        };

        if tail == INVALID_INDEX {
            self.levels[side_index][price_index].head = slot_index;
        } else {
            self.slots[tail as usize].next = slot_index;
        }
        let level = &mut self.levels[side_index][price_index];
        level.tail = slot_index;
        level.total_quantity += u64::from(quantity);
        self.id_to_slot[id as usize] = slot_index;
        self.live_orders += 1;

        if was_empty {
            self.active[side_index].insert(price);
            match side {
                Side::Bid => self.best[side_index] = self.best[side_index].max(i32::from(price)),
                Side::Ask => self.best[side_index] = self.best[side_index].min(i32::from(price)),
            }
        }
    }

    fn unlink(&mut self, slot_index: u32) {
        let order = self.slots[slot_index as usize];
        let side = order.side();
        let side_index = side.index();
        let price = order.price();
        let price_index = usize::from(price);

        if order.previous == INVALID_INDEX {
            self.levels[side_index][price_index].head = order.next;
        } else {
            self.slots[order.previous as usize].next = order.next;
        }
        if order.next == INVALID_INDEX {
            self.levels[side_index][price_index].tail = order.previous;
        } else {
            self.slots[order.next as usize].previous = order.previous;
        }

        self.levels[side_index][price_index].total_quantity -= u64::from(order.quantity);
        self.id_to_slot[order.id as usize] = INVALID_INDEX;
        self.live_orders -= 1;

        if self.levels[side_index][price_index].head == INVALID_INDEX {
            self.active[side_index].remove(price);
            if self.best[side_index] == i32::from(price) {
                self.best[side_index] = match side {
                    Side::Bid => self.active[side_index].last(),
                    Side::Ask => self.active[side_index].first(),
                };
            }
        }
        self.release_slot(slot_index);
    }
}

#[inline]
fn valid_price(price: i32) -> bool {
    (0..PRICE_COUNT as i32).contains(&price)
}

#[inline]
fn crosses(taker_side: Side, limit_price: u16, maker_price: i32) -> bool {
    match taker_side {
        Side::Bid => maker_price <= i32::from(limit_price),
        Side::Ask => maker_price >= i32::from(limit_price),
    }
}

const _: () = {
    assert!(size_of::<OrderSlot>() == 24);
    assert!(size_of::<PriceLevel>() == 16);
};
