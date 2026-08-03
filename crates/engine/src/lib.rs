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

/// The fixed price domain of [`L2Book`], and the window an [`L3Book`] boots
/// with before growth: 65,536 slots, 2 MiB of level table per book.
pub const PRICE_COUNT: usize = 1 << 16;

/// The L3 price domain: 31 bits, so a price and its side pack into one word
/// and an order slot stays 24 bytes. The type's limit, not a chosen number —
/// 2,147,483,648 ticks per instrument, 32,768× the old fixed ladder. The
/// window covers the slice of it that prices actually reach.
pub const PRICE_LIMIT: u32 = 1 << 31;

/// Width of the `u64` words backing every tier of the occupancy bitmap, with
/// the derived shift and mask used to split a price into word and bit index.
const BITS_PER_WORD: usize = u64::BITS as usize;
const WORD_INDEX_SHIFT: usize = BITS_PER_WORD.trailing_zeros() as usize;
const WORD_BIT_MASK: usize = BITS_PER_WORD - 1;

/// `OrderSlot` packs side and price into one word: side in bit 31, price below.
const SIDE_SHIFT: u32 = 31;
const PRICE_MASK: u32 = PRICE_LIMIT - 1;

/// A market order is a limit order at the most aggressive representable tick.
/// These are the ends of the price domain, not sentinels: a market buy crosses
/// every ask, a market sell crosses every bid — and because IOC/FOK never
/// rest, the extreme never enters the window or grows it.
const MOST_AGGRESSIVE_BID_TICK: u32 = PRICE_LIMIT - 1;
const MOST_AGGRESSIVE_ASK_TICK: u32 = 0;

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
    PriceOutOfDomain,
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
            Self::PriceOutOfDomain => "price is outside the 31-bit price domain",
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

/// Three-tier occupancy over a `span`-slot window: a bit per slot, a bit per
/// leaf word, a bit per summary word. Every tier is a `Vec`, so a window can
/// widen (append zero words, no set bit moves) or shift (rebuilt from the
/// leaf bits at their new positions). Within one root word a query is three
/// word reads and three bit scans; a gap that crosses root words pays one
/// further read per 262,144 slots it crosses.
///
/// "None below" is `-1`; "none above" is `span as i32`, one past the top —
/// the conventions [`NO_BID`] and [`NO_ASK`] encode for the fixed
/// [`PRICE_COUNT`] domain.
#[derive(Clone, Debug)]
pub struct HierarchicalBitmap {
    leaf: Vec<u64>,
    summary: Vec<u64>,
    root: Vec<u64>,
    span: usize,
}

impl HierarchicalBitmap {
    #[must_use]
    pub fn new(span: usize) -> Self {
        let mut fresh = Self {
            leaf: Vec::new(),
            summary: Vec::new(),
            root: Vec::new(),
            span: 0,
        };
        fresh.widen(span);
        fresh
    }

    /// Extends the bitmap to cover `span` slots. Appended words are zero and
    /// existing words keep their index, so no set bit moves.
    pub fn widen(&mut self, span: usize) {
        let words = |n: usize| n.div_ceil(BITS_PER_WORD).max(1);
        self.leaf.resize(words(span), 0);
        self.summary.resize(words(words(span)), 0);
        self.root.resize(words(words(words(span))), 0);
        self.span = span;
    }

    /// The same occupancy over a `span`-slot window whose slot 0 moved down
    /// by `shift`: every set bit re-set at its new position. One pass over
    /// the old leaf words — the same order as the level copy a caller pays.
    #[must_use]
    pub fn shifted(&self, shift: usize, span: usize) -> Self {
        let mut fresh = Self::new(span);
        for (word, &bits) in self.leaf.iter().enumerate() {
            let mut bits = bits;
            while bits != 0 {
                fresh.insert(((word << WORD_INDEX_SHIFT) + least_bit(bits) + shift) as u32);
                bits &= bits - 1;
            }
        }
        fresh
    }

    #[must_use]
    pub fn span(&self) -> usize {
        self.span
    }

    pub fn clear(&mut self) {
        self.leaf.fill(0);
        self.summary.fill(0);
        self.root.fill(0);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.root.iter().all(|&word| word == 0)
    }

    #[must_use]
    pub fn contains(&self, slot: u32) -> bool {
        let value = slot as usize;
        ((self.leaf[value >> WORD_INDEX_SHIFT] >> (value & WORD_BIT_MASK)) & 1) != 0
    }

    pub fn insert(&mut self, slot: u32) {
        let value = slot as usize;
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
            self.root[summary_index >> WORD_INDEX_SHIFT] |=
                1_u64 << (summary_index & WORD_BIT_MASK);
        }
    }

    pub fn remove(&mut self, slot: u32) {
        let value = slot as usize;
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
            self.root[summary_index >> WORD_INDEX_SHIFT] &=
                !(1_u64 << (summary_index & WORD_BIT_MASK));
        }
    }

    /// Resolves a hit in `summary_index` down to its lowest set slot.
    fn descend_least(&self, summary_index: usize, leaves: u64) -> i32 {
        let leaf_index = (summary_index << WORD_INDEX_SHIFT) + least_bit(leaves);
        ((leaf_index << WORD_INDEX_SHIFT) + least_bit(self.leaf[leaf_index])) as i32
    }

    /// Resolves a hit in `summary_index` down to its highest set slot.
    fn descend_greatest(&self, summary_index: usize, leaves: u64) -> i32 {
        let leaf_index = (summary_index << WORD_INDEX_SHIFT) + greatest_bit(leaves);
        ((leaf_index << WORD_INDEX_SHIFT) + greatest_bit(self.leaf[leaf_index])) as i32
    }

    #[must_use]
    pub fn first(&self) -> i32 {
        for (root_index, &word) in self.root.iter().enumerate() {
            if word != 0 {
                let summary_index = (root_index << WORD_INDEX_SHIFT) + least_bit(word);
                return self.descend_least(summary_index, self.summary[summary_index]);
            }
        }
        self.span as i32
    }

    #[must_use]
    pub fn last(&self) -> i32 {
        for (root_index, &word) in self.root.iter().enumerate().rev() {
            if word != 0 {
                let summary_index = (root_index << WORD_INDEX_SHIFT) + greatest_bit(word);
                return self.descend_greatest(summary_index, self.summary[summary_index]);
            }
        }
        NO_BID
    }

    #[must_use]
    pub fn next(&self, slot: u32) -> i32 {
        let value = slot as usize + 1;
        if value >= self.span {
            return self.span as i32;
        }
        let leaf_index = value >> WORD_INDEX_SHIFT;
        let local = self.leaf[leaf_index] & (u64::MAX << (value & WORD_BIT_MASK));
        if local != 0 {
            return ((leaf_index << WORD_INDEX_SHIFT) + least_bit(local)) as i32;
        }

        let mut summary_index = leaf_index >> WORD_INDEX_SHIFT;
        let summary_start = (leaf_index & WORD_BIT_MASK) + 1;
        let leaves = if summary_start < BITS_PER_WORD {
            self.summary[summary_index] & (u64::MAX << summary_start)
        } else {
            0
        };
        if leaves != 0 {
            return self.descend_least(summary_index, leaves);
        }

        // Root tier: the current word masked above this summary bit, then
        // whole words toward the top — one read per 262,144 slots a gap
        // crosses.
        let mut root_index = summary_index >> WORD_INDEX_SHIFT;
        let root_start = (summary_index & WORD_BIT_MASK) + 1;
        let mut summaries = if root_start < BITS_PER_WORD {
            self.root[root_index] & (u64::MAX << root_start)
        } else {
            0
        };
        while summaries == 0 {
            root_index += 1;
            match self.root.get(root_index) {
                Some(&word) => summaries = word,
                None => return self.span as i32,
            }
        }
        summary_index = (root_index << WORD_INDEX_SHIFT) + least_bit(summaries);
        self.descend_least(summary_index, self.summary[summary_index])
    }

    #[must_use]
    pub fn previous(&self, slot: u32) -> i32 {
        if slot == 0 {
            return NO_BID;
        }

        let value = slot as usize - 1;
        let leaf_index = value >> WORD_INDEX_SHIFT;
        let local_bit = value & WORD_BIT_MASK;
        let local_mask = if local_bit == WORD_BIT_MASK {
            u64::MAX
        } else {
            (1_u64 << (local_bit + 1)) - 1
        };
        let local = self.leaf[leaf_index] & local_mask;
        if local != 0 {
            return ((leaf_index << WORD_INDEX_SHIFT) + greatest_bit(local)) as i32;
        }

        let mut summary_index = leaf_index >> WORD_INDEX_SHIFT;
        let summary_bit = leaf_index & WORD_BIT_MASK;
        let leaves = if summary_bit == 0 {
            0
        } else {
            self.summary[summary_index] & ((1_u64 << summary_bit) - 1)
        };
        if leaves != 0 {
            return self.descend_greatest(summary_index, leaves);
        }

        // Mirror of the upward walk: masked root word, then whole words
        // toward the bottom.
        let mut root_index = summary_index >> WORD_INDEX_SHIFT;
        let root_bit = summary_index & WORD_BIT_MASK;
        let mut summaries = if root_bit == 0 {
            0
        } else {
            self.root[root_index] & ((1_u64 << root_bit) - 1)
        };
        while summaries == 0 {
            if root_index == 0 {
                return NO_BID;
            }
            root_index -= 1;
            summaries = self.root[root_index];
        }
        summary_index = (root_index << WORD_INDEX_SHIFT) + greatest_bit(summaries);
        self.descend_greatest(summary_index, self.summary[summary_index])
    }

    /// Checks that the summary and root tiers agree with the leaf words.
    ///
    /// Diagnostic only: scans every leaf word.
    #[must_use]
    pub fn validate(&self) -> bool {
        for (summary_index, &summary_word) in self.summary.iter().enumerate() {
            let mut expected_summary = 0_u64;
            for offset in 0..BITS_PER_WORD {
                let leaf_index = (summary_index << WORD_INDEX_SHIFT) + offset;
                if self.leaf.get(leaf_index).copied().unwrap_or(0) != 0 {
                    expected_summary |= 1_u64 << offset;
                }
            }
            if summary_word != expected_summary {
                return false;
            }
            let root_bit = (self.root[summary_index >> WORD_INDEX_SHIFT]
                >> (summary_index & WORD_BIT_MASK))
                & 1;
            if (root_bit != 0) != (expected_summary != 0) {
                return false;
            }
        }
        true
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
            active: HierarchicalBitmap::new(PRICE_COUNT),
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
            state.active.insert(u32::from(price));
            match side {
                Side::Bid => state.best = state.best.max(i32::from(price)),
                Side::Ask => state.best = state.best.min(i32::from(price)),
            }
        } else if old_quantity != 0 && quantity == 0 {
            state.active.remove(u32::from(price));
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
                Side::Bid => state.active.previous(price as u32),
                Side::Ask => state.active.next(price as u32),
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
                Side::Bid => state.active.previous(price as u32),
                Side::Ask => state.active.next(price as u32),
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
                if state.active.contains(price as u32) != (quantity != 0) {
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
    fn price(self) -> u32 {
        self.price_side & PRICE_MASK
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
    pub price: u32,
    pub quantity: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fill {
    pub maker_order_id: u32,
    pub maker_side: Side,
    pub price: u32,
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
    /// Price at window offset 0. `[base, base + window)` is the slice of the
    /// 31-bit price domain the level tables and bitmaps currently cover; it
    /// follows the prices that rest, never shrinks, and never moves while an
    /// order rests. Slots store absolute prices, so a shift moves no slot.
    base: u32,
    levels: [Vec<PriceLevel>; 2],
    slots: Vec<OrderSlot>,
    id_to_slot: Vec<u32>,
    active: [HierarchicalBitmap; 2],
    /// Best prices as window offsets: `NO_BID` (-1) for an empty bid side,
    /// the window length (one past the top) for an empty ask side. Neither
    /// sentinel passes `valid_offset`, which every consumer checks first.
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
            base: 0,
            levels: [
                vec![PriceLevel::empty(); PRICE_COUNT],
                vec![PriceLevel::empty(); PRICE_COUNT],
            ],
            slots: vec![OrderSlot::empty(); max_orders],
            id_to_slot: vec![INVALID_INDEX; max_order_ids],
            active: [
                HierarchicalBitmap::new(PRICE_COUNT),
                HierarchicalBitmap::new(PRICE_COUNT),
            ],
            best: [NO_BID, PRICE_COUNT as i32],
            next_unused: 0,
            free_head: INVALID_INDEX,
            live_orders: 0,
        })
    }

    /// Slots the window currently covers.
    #[inline]
    fn window(&self) -> usize {
        self.levels[0].len()
    }

    /// Whether a cached-best value names a real level. Both empty-side
    /// sentinels fail this, so no consumer compares against a sentinel.
    #[inline]
    fn valid_offset(&self, offset: i32) -> bool {
        offset >= 0 && (offset as usize) < self.window()
    }

    /// The window offset of an absolute price, if the window covers it.
    #[inline]
    fn offset_in_window(&self, price: u32) -> Option<usize> {
        if price >= self.base && ((price - self.base) as usize) < self.window() {
            Some((price - self.base) as usize)
        } else {
            None
        }
    }

    /// The absolute price at a window offset.
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn absolute(&self, offset: usize) -> u32 {
        self.base + offset as u32
    }

    /// Brings `price` inside the window. An empty book moves the window —
    /// every level is empty and every bitmap word zero, so re-anchoring is
    /// one field write. A resting book grows contiguously toward the price,
    /// geometrically, so a drifting market pays each copy once per doubling.
    /// Upward growth appends (no index moves); downward growth shifts the
    /// levels and rebuilds the bitmaps — slots hold absolute prices, so no
    /// slot is touched either way. Never on the steady-state path: callers
    /// come here only when the window missed, and only for orders that rest.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    fn grow_window(&mut self, price: u32) {
        debug_assert!(price < PRICE_LIMIT);
        let len = self.window();
        if self.live_orders == 0 {
            let top_base = PRICE_LIMIT - len as u32;
            self.base = price.saturating_sub(len as u32 / 2).min(top_base);
            return;
        }
        if price > self.base {
            // Upward: existing offsets keep their meaning, so the level
            // tables extend with empties and the bitmaps with zero words.
            let need = (price - self.base) as usize + 1;
            let cap = (PRICE_LIMIT - self.base) as usize;
            let new_len = need.max(len.saturating_mul(2)).min(cap);
            for side in 0..2 {
                self.levels[side].resize(new_len, PriceLevel::empty());
                self.active[side].widen(new_len);
            }
            // The empty-ask sentinel is the window length; follow it.
            if self.best[1] == len as i32 {
                self.best[1] = new_len as i32;
            }
        } else {
            // Downward: offset 0 moves, so levels shift, bitmaps rebuild at
            // the new positions, and both cached bests move by the shift —
            // the ask sentinel (old length) lands on the new length exactly.
            let need = (self.base - price) as usize;
            let floor = self.base as usize;
            let shift = need.max(len).min(floor);
            let new_len = len + shift;
            for side in 0..2 {
                let mut fresh = vec![PriceLevel::empty(); new_len];
                fresh[shift..].copy_from_slice(&self.levels[side]);
                self.levels[side] = fresh;
                self.active[side] = self.active[side].shifted(shift, new_len);
            }
            self.base -= shift as u32;
            if self.best[0] != NO_BID {
                self.best[0] += shift as i32;
            }
            self.best[1] += shift as i32;
        }
        debug_assert!(
            self.offset_in_window(price).is_some(),
            "growth must reach its price"
        );
    }

    /// Doubles the slot pool. Slots are addressed by `u32` index, so the
    /// reallocation invalidates nothing a live order holds; the untouched
    /// tail past `next_unused` simply lengthens. Amortised, off the
    /// steady-state path, refused only at the `u32` index space itself.
    fn grow_slots(&mut self) {
        let was = self.slots.len();
        let want = was
            .max(32)
            .saturating_mul(2)
            .min(INVALID_INDEX as usize - 1);
        if want <= was {
            return;
        }
        self.slots.resize(want, OrderSlot::empty());
    }

    /// Widens the dense order-ID space to `capacity`. Driven by the
    /// allocator in front of the book when it grows — the book never widens
    /// on an ID it is merely handed, so a caller naming `u32::MAX` cannot
    /// make it allocate a 16 GiB table.
    pub fn widen_order_ids(&mut self, capacity: usize) {
        if capacity > self.id_to_slot.len() {
            self.id_to_slot.resize(capacity, INVALID_INDEX);
        }
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

    /// The best bid as an absolute price, if anything rests on that side.
    #[must_use]
    pub fn best_bid(&self) -> Option<u32> {
        let offset = self.best[Side::Bid.index()];
        self.valid_offset(offset)
            .then(|| self.absolute(offset as usize))
    }

    /// The best ask as an absolute price, if anything rests on that side.
    #[must_use]
    pub fn best_ask(&self) -> Option<u32> {
        let offset = self.best[Side::Ask.index()];
        self.valid_offset(offset)
            .then(|| self.absolute(offset as usize))
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

    /// Quantity resting at an absolute price. A price the window has not
    /// allocated holds nothing, and saying so allocates nothing.
    #[must_use]
    pub fn level_quantity(&self, side: Side, price: u32) -> u64 {
        match self.offset_in_window(price) {
            Some(offset) => self.levels[side.index()][offset].total_quantity,
            None => 0,
        }
    }

    #[must_use]
    pub fn would_cross(&self, side: Side, price: u32) -> bool {
        let limit_rel = i64::from(price) - i64::from(self.base);
        match side {
            Side::Bid => {
                let best = self.best[Side::Ask.index()];
                self.valid_offset(best) && i64::from(best) <= limit_rel
            }
            Side::Ask => {
                let best = self.best[Side::Bid.index()];
                self.valid_offset(best) && i64::from(best) >= limit_rel
            }
        }
    }

    pub fn add_passive(
        &mut self,
        id: u32,
        side: Side,
        price: u32,
        quantity: u32,
    ) -> Result<(), OrderError> {
        if price >= PRICE_LIMIT {
            return Err(OrderError::PriceOutOfDomain);
        }
        self.validate_new_order(id, quantity)?;
        if self.would_cross(side, price) {
            return Err(OrderError::WouldCross);
        }
        let slot_index = self.allocate_slot().ok_or(OrderError::CapacityExceeded)?;
        if self.offset_in_window(price).is_none() {
            self.grow_window(price);
        }
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

        let offset = (slot.price() - self.base) as usize;
        let level = &mut self.levels[slot.side().index()][offset];
        level.total_quantity -= u64::from(slot.quantity - new_quantity);
        self.slots[slot_index].quantity = new_quantity;
        Ok(())
    }

    pub fn cancel_replace(
        &mut self,
        existing_id: u32,
        replacement_id: u32,
        new_price: u32,
        new_quantity: u32,
    ) -> Result<ExecutionReport, OrderError> {
        self.cancel_replace_with(existing_id, replacement_id, new_price, new_quantity, |_| {})
    }

    pub fn cancel_replace_with<F>(
        &mut self,
        existing_id: u32,
        replacement_id: u32,
        new_price: u32,
        new_quantity: u32,
        on_fill: F,
    ) -> Result<ExecutionReport, OrderError>
    where
        F: FnMut(Fill),
    {
        if new_price >= PRICE_LIMIT {
            return Err(OrderError::PriceOutOfDomain);
        }
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
        limit_price: u32,
        quantity: u32,
        time_in_force: TimeInForce,
    ) -> Result<ExecutionReport, OrderError> {
        self.submit_limit_with(id, side, limit_price, quantity, time_in_force, |_| {})
    }

    pub fn submit_limit_with<F>(
        &mut self,
        id: u32,
        side: Side,
        limit_price: u32,
        quantity: u32,
        time_in_force: TimeInForce,
        on_fill: F,
    ) -> Result<ExecutionReport, OrderError>
    where
        F: FnMut(Fill),
    {
        if limit_price >= PRICE_LIMIT {
            return Err(OrderError::PriceOutOfDomain);
        }
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

    /// Walks levels best-first while `visitor` returns true, and stops as soon
    /// as it returns false. Returns how many were visited.
    ///
    /// The counted variant below cannot stop on a condition, so a caller that
    /// only cares about levels within a price bound would have to walk every
    /// occupied level and discard most of them. Self-match prevention does
    /// exactly that check on the crossing path, where the walk is in front of
    /// matching and its cost is paid by every aggressive order.
    pub fn for_each_level_while<F>(&self, side: Side, mut visitor: F) -> usize
    where
        F: FnMut(u32, u64) -> bool,
    {
        let side_index = side.index();
        let mut offset = self.best[side_index];
        let mut visited = 0;
        while self.valid_offset(offset) {
            if !visitor(
                self.absolute(offset as usize),
                self.levels[side_index][offset as usize].total_quantity,
            ) {
                break;
            }
            visited += 1;
            offset = match side {
                Side::Bid => self.active[side_index].previous(offset as u32),
                Side::Ask => self.active[side_index].next(offset as u32),
            };
        }
        visited
    }

    pub fn for_each_level<F>(&self, side: Side, limit: usize, mut visitor: F) -> usize
    where
        F: FnMut(u32, u64),
    {
        let side_index = side.index();
        let mut offset = self.best[side_index];
        let mut visited = 0;
        while visited < limit && self.valid_offset(offset) {
            visitor(
                self.absolute(offset as usize),
                self.levels[side_index][offset as usize].total_quantity,
            );
            visited += 1;
            offset = match side {
                Side::Bid => self.active[side_index].previous(offset as u32),
                Side::Ask => self.active[side_index].next(offset as u32),
            };
        }
        visited
    }

    /// Walks a level's queue in time priority while `visitor` returns true.
    ///
    /// A level can hold every order in the book, so a caller that only needs the
    /// front of the queue must be able to stop. Without this, checking whether
    /// an order of quantity one would self-match cost a walk of the entire
    /// level: measured at 35.8 microseconds against 175 nanoseconds, a two
    /// hundred fold regression on the crossing path.
    pub fn for_each_order_at_level_while<F>(&self, side: Side, price: u32, mut visitor: F) -> usize
    where
        F: FnMut(OrderView) -> bool,
    {
        let Some(offset) = self.offset_in_window(price) else {
            return 0;
        };
        let mut slot_index = self.levels[side.index()][offset].head;
        let mut visited = 0;
        while slot_index != INVALID_INDEX {
            let slot = self.slots[slot_index as usize];
            if !visitor(OrderView {
                id: slot.id,
                side,
                price,
                quantity: slot.quantity,
            }) {
                break;
            }
            visited += 1;
            slot_index = slot.next;
        }
        visited
    }

    pub fn for_each_order_at_level<F>(&self, side: Side, price: u32, mut visitor: F) -> usize
    where
        F: FnMut(OrderView),
    {
        let Some(offset) = self.offset_in_window(price) else {
            return 0;
        };
        let mut slot_index = self.levels[side.index()][offset].head;
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
    /// replay comparison. Hashes absolute prices, so two books holding the
    /// same orders agree whatever windows they grew through — and a book
    /// whose window never left `[0, 65,536)` hashes exactly as the fixed
    /// ladder did, which is what keeps the golden replay hash valid.
    ///
    /// Diagnostic only: walks the whole window on both sides.
    #[must_use]
    pub fn state_hash(&self) -> u64 {
        // The shifts spread side, price, ID, quantity, and queue rank into
        // disjoint regions of the word so two different books cannot collide by
        // trading one field's value against another's.
        let mut hash = mix64(u64::from(self.live_orders) + STATE_HASH_SEED);
        for side in 0..2 {
            for offset in 0..self.window() {
                let mut slot_index = self.levels[side][offset].head;
                let mut rank = 1_u64;
                while slot_index != INVALID_INDEX {
                    let slot = self.slots[slot_index as usize];
                    hash = mix64(
                        hash ^ ((side as u64) << 63)
                            ^ (u64::from(self.absolute(offset)) << 40)
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
            for offset in 0..self.window() {
                let level = self.levels[side][offset];
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
                        || slot.price() != self.absolute(offset)
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
                #[allow(clippy::cast_possible_truncation)]
                if self.active[side].contains(offset as u32) != (level.head != INVALID_INDEX) {
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
        if self.best[0] != self.active[0].last() || self.best[1] != self.active[1].first() {
            return false;
        }
        !self.valid_offset(self.best[0])
            || !self.valid_offset(self.best[1])
            || self.best[0] < self.best[1]
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

    /// Whether an `allocate_slot` would succeed: a recycled slot, an unused
    /// one, or room left to grow the pool — refusal only at the `u32` index
    /// space itself.
    #[inline]
    fn slot_available(&self) -> bool {
        self.free_head != INVALID_INDEX || (self.next_unused as usize) < INVALID_INDEX as usize - 1
    }

    fn preview_liquidity(
        &self,
        taker_side: Side,
        limit_rel: i64,
        requested_quantity: u32,
    ) -> LiquidityPreview {
        let mut preview = LiquidityPreview::default();
        let mut remaining = requested_quantity;
        let maker_side = taker_side.opposite();
        let maker_side_index = maker_side.index();
        let mut offset = self.best[maker_side_index];

        while remaining != 0 && self.valid_offset(offset) && crosses(taker_side, limit_rel, offset)
        {
            let mut slot_index = self.levels[maker_side_index][offset as usize].head;
            while remaining != 0 && slot_index != INVALID_INDEX {
                let maker = self.slots[slot_index as usize];
                let fill = remaining.min(maker.quantity);
                remaining -= fill;
                preview.executable_quantity += fill;
                preview.releases_slot |= fill == maker.quantity;
                slot_index = maker.next;
            }
            offset = match taker_side {
                Side::Bid => self.active[maker_side_index].next(offset as u32),
                Side::Ask => self.active[maker_side_index].previous(offset as u32),
            };
        }
        preview
    }

    #[allow(clippy::too_many_arguments)]
    fn submit_limit_impl<F>(
        &mut self,
        id: u32,
        side: Side,
        limit_price: u32,
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

        // The limit relative to the window base, computed once: every crossing
        // comparison is then offset against offset, whatever the window covers.
        // The base only moves when a remainder rests, after the loop.
        let limit_rel = i64::from(limit_price) - i64::from(self.base);

        if time_in_force == TimeInForce::PostOnly {
            if self.would_cross(side, limit_price) {
                return Err(OrderError::WouldCross);
            }
            let slot_index = self.allocate_slot().ok_or(OrderError::CapacityExceeded)?;
            if self.offset_in_window(limit_price).is_none() {
                self.grow_window(limit_price);
            }
            self.append(slot_index, id, side, limit_price, quantity);
            return Ok(ExecutionReport {
                rested_quantity: quantity,
                ..ExecutionReport::default()
            });
        }

        let preview = if time_in_force == TimeInForce::FillOrKill
            || (time_in_force == TimeInForce::GoodTillCancel && !self.slot_available())
        {
            self.preview_liquidity(side, limit_rel, quantity)
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
            let maker_offset = self.best[maker_side_index];
            if !self.valid_offset(maker_offset) || !crosses(side, limit_rel, maker_offset) {
                break;
            }

            let maker_offset = maker_offset as usize;
            let maker_price = self.absolute(maker_offset);
            let maker_slot_index = self.levels[maker_side_index][maker_offset].head;
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
            // A 31-bit price shifted 32 still fits the word.
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
                self.levels[maker_side_index][maker_offset].total_quantity -=
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
                if self.offset_in_window(limit_price).is_none() {
                    self.grow_window(limit_price);
                }
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
            self.grow_slots();
            if self.next_unused as usize >= self.slots.len() {
                return None;
            }
        }
        let slot_index = self.next_unused;
        self.next_unused += 1;
        Some(slot_index)
    }

    fn release_slot(&mut self, slot_index: u32) {
        self.slots[slot_index as usize].free_next = self.free_head;
        self.free_head = slot_index;
    }

    /// Rests an order at the tail of its level. The caller has already
    /// brought `price` inside the window.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    fn append(&mut self, slot_index: u32, id: u32, side: Side, price: u32, quantity: u32) {
        let side_index = side.index();
        let offset = (price - self.base) as usize;
        let tail = self.levels[side_index][offset].tail;
        let was_empty = tail == INVALID_INDEX;

        self.slots[slot_index as usize] = OrderSlot {
            next: INVALID_INDEX,
            previous: tail,
            id,
            quantity,
            free_next: INVALID_INDEX,
            price_side: ((side_index as u32) << SIDE_SHIFT) | price,
        };

        if tail == INVALID_INDEX {
            self.levels[side_index][offset].head = slot_index;
        } else {
            self.slots[tail as usize].next = slot_index;
        }
        let level = &mut self.levels[side_index][offset];
        level.tail = slot_index;
        level.total_quantity += u64::from(quantity);
        self.id_to_slot[id as usize] = slot_index;
        self.live_orders += 1;

        if was_empty {
            self.active[side_index].insert(offset as u32);
            match side {
                Side::Bid => self.best[side_index] = self.best[side_index].max(offset as i32),
                Side::Ask => self.best[side_index] = self.best[side_index].min(offset as i32),
            }
        }
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    fn unlink(&mut self, slot_index: u32) {
        let order = self.slots[slot_index as usize];
        let side = order.side();
        let side_index = side.index();
        // Resting prices are always inside the window: it never shrinks and
        // never moves while an order rests.
        let offset = (order.price() - self.base) as usize;

        if order.previous == INVALID_INDEX {
            self.levels[side_index][offset].head = order.next;
        } else {
            self.slots[order.previous as usize].next = order.next;
        }
        if order.next == INVALID_INDEX {
            self.levels[side_index][offset].tail = order.previous;
        } else {
            self.slots[order.next as usize].previous = order.previous;
        }

        self.levels[side_index][offset].total_quantity -= u64::from(order.quantity);
        self.id_to_slot[order.id as usize] = INVALID_INDEX;
        self.live_orders -= 1;

        if self.levels[side_index][offset].head == INVALID_INDEX {
            self.active[side_index].remove(offset as u32);
            if self.best[side_index] == offset as i32 {
                self.best[side_index] = match side {
                    Side::Bid => self.active[side_index].last(),
                    Side::Ask => self.active[side_index].first(),
                };
            }
        }
        self.release_slot(slot_index);
    }
}

/// Validity in the fixed [`PRICE_COUNT`] domain — the [`L2Book`] convention.
#[inline]
fn valid_price(price: i32) -> bool {
    (0..PRICE_COUNT as i32).contains(&price)
}

/// Whether a maker at `maker_offset` crosses a limit expressed relative to
/// the same window base. Offsets never exceed 2^31, so `i64` never wraps.
#[inline]
fn crosses(taker_side: Side, limit_rel: i64, maker_offset: i32) -> bool {
    match taker_side {
        Side::Bid => i64::from(maker_offset) <= limit_rel,
        Side::Ask => i64::from(maker_offset) >= limit_rel,
    }
}

const _: () = {
    assert!(size_of::<OrderSlot>() == 24);
    assert!(size_of::<PriceLevel>() == 16);
};
