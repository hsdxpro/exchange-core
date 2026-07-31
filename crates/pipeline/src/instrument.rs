//! Instrument definitions, and the mapping from wire prices to engine ticks.
//!
//! The engine addresses prices as an index into a fixed 65,536-slot ladder. The
//! wire carries a signed tick price that means something to a client. An
//! instrument is what connects the two, and its ladder range *is* its price
//! band: a price the ladder cannot address is a price the venue will not
//! accept. The memory bound and the fat-finger control are therefore the same
//! thing, with no separate mechanism and no invented window size.

use bx_protocol::{Quantity, SymbolId, Ticks};

/// Slots in the engine's ladder. Fixed by the engine's design.
pub const LADDER_SLOTS: i64 = 65_536;

// A slot is carried as a `u16` everywhere: out of `to_slot`, into `to_price`,
// and back off the engine as an execution price. That holds only while the
// ladder is no larger than the `u16` range, and it is currently exactly that
// size -- no margin. Raising it without widening the slot type would not fail
// to compile and would not panic. `to_slot` would quietly reject every price in
// the upper half of each band, and prices coming back off the engine would
// truncate into a different, valid-looking price. Wrong fills, no error.
//
// So the build fails here instead.
const _: () = assert!(
    LADDER_SLOTS <= u16::MAX as i64 + 1,
    "LADDER_SLOTS exceeds the u16 slot index: widen the slot type in to_slot, \
     to_price and Book::drain before raising it"
);

/// Largest symbol ID a venue may assign.
///
/// Instruments are held in a table indexed by symbol, which is what makes
/// lookup a bounds check and an offset on the command path. The cost is that the
/// table is as long as the largest ID in it: numbering a single instrument
/// 4,294,967,295 would ask for a table of four billion entries and exhaust
/// memory before the venue accepted an order.
///
/// Symbol IDs are venue-assigned, so numbering them densely from zero costs
/// nothing. This bound keeps the table under a few megabytes and turns a
/// mistyped configuration into a refusal instead of an out-of-memory kill.
pub const MAX_SYMBOL: SymbolId = 1 << 16;

/// Largest resting-order pool the engine can address.
///
/// One below `u32::MAX`, because the engine uses that value as its
/// end-of-list sentinel and so cannot also use it as a slot index.
pub const MAX_OPEN_ORDERS_LIMIT: u32 = u32::MAX - 1;

/// An asset, for balance purposes. A spot instrument moves two of them.
pub type AssetId = u32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Instrument {
    pub symbol: SymbolId,
    /// What the client receives and pays.
    pub base: AssetId,
    /// What the client pays and receives.
    pub quote: AssetId,
    /// Wire tick price that maps to ladder slot 0. Everything the venue will
    /// accept lies in `floor_ticks .. floor_ticks + LADDER_SLOTS`.
    pub floor_ticks: Ticks,
    /// Largest single-order quantity. Bounds the arithmetic below and stops one
    /// order from consuming the whole book by accident.
    pub max_quantity: Quantity,
    /// How many orders may rest in this book at once.
    ///
    /// The engine holds resting orders in a preallocated pool addressed by a
    /// dense index, which is what makes insert, cancel and amend O(1) with no
    /// allocation on the command path. The pool has to be sized, and its size
    /// is a venue policy per instrument — a thin altcoin does not need the pool
    /// a major pair does — so it is declared here rather than assumed.
    ///
    /// This is unrelated to how many *price levels* the book has. Those come
    /// from the bitmap ladder and are always [`LADDER_SLOTS`]. It is also not a
    /// per-level limit: one price can hold every order in the pool, because a
    /// level is a head and tail index into it rather than a container.
    ///
    /// Sizing it. The pool costs **40 bytes per slot** the moment the book is
    /// built, and a further **~15 bytes per live order** for the client-ID
    /// index, so a full book is about **55 bytes per order**:
    ///
    /// | | bytes per slot |
    /// |---|---:|
    /// | engine order slot | 24 |
    /// | engine ID-to-slot table | 4 |
    /// | slot-to-order-ID table | 8 |
    /// | free list | 4 |
    /// | client-ID index, per *live* order | ~15 |
    ///
    /// On top of that every book costs a flat ~2.1 MiB for the price-level
    /// table and bitmaps, whatever this value is.
    ///
    /// The hard ceiling is [`MAX_OPEN_ORDERS_LIMIT`], set by the engine's u32
    /// slot index. Long before that the limit is memory: a million orders is
    /// ~55 MiB, so a thousand symbols at a million each is ~57 GiB.
    pub max_open_orders: u32,
}

impl Instrument {
    #[must_use]
    pub const fn new(
        symbol: SymbolId,
        base: AssetId,
        quote: AssetId,
        floor_ticks: Ticks,
        max_quantity: Quantity,
        max_open_orders: u32,
    ) -> Self {
        Self {
            symbol,
            base,
            quote,
            floor_ticks,
            max_quantity,
            max_open_orders,
        }
    }

    /// Highest wire price this instrument can represent.
    #[must_use]
    pub const fn ceiling_ticks(&self) -> Ticks {
        self.floor_ticks + LADDER_SLOTS - 1
    }

    /// Converts a wire price to a ladder slot, or `None` if it falls outside
    /// the band. This is the price-band check; there is no second one.
    #[must_use]
    pub fn to_slot(&self, price: Ticks) -> Option<u16> {
        let offset = price.checked_sub(self.floor_ticks)?;
        if (0..LADDER_SLOTS).contains(&offset) {
            u16::try_from(offset).ok()
        } else {
            None
        }
    }

    /// Converts a ladder slot back to the wire price.
    #[must_use]
    pub fn to_price(&self, slot: u16) -> Ticks {
        self.floor_ticks + Ticks::from(slot)
    }

    /// Quote-asset amount that `quantity` at `price` is worth.
    ///
    /// Returns `None` on overflow rather than wrapping, because a wrapped
    /// notional would silently under-reserve a balance.
    #[must_use]
    pub fn notional(&self, price: Ticks, quantity: Quantity) -> Option<u128> {
        let price = u128::try_from(price).ok()?;
        price.checked_mul(u128::from(quantity))
    }
}

/// Every instrument the venue trades, indexed by symbol for O(1) lookup.
#[derive(Debug, Default)]
pub struct Instruments {
    by_symbol: Vec<Option<Instrument>>,
}

impl Instruments {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// # Panics
    /// If the symbol is at or above [`MAX_SYMBOL`]. Callers that take a symbol
    /// from outside the process must check first; [`crate::instrument::MAX_SYMBOL`]
    /// is what the configuration validates against.
    pub fn insert(&mut self, instrument: Instrument) {
        assert!(
            instrument.symbol < MAX_SYMBOL,
            "symbol {} is at or above the limit of {MAX_SYMBOL}",
            instrument.symbol
        );
        let index = instrument.symbol as usize;
        if self.by_symbol.len() <= index {
            self.by_symbol.resize(index + 1, None);
        }
        self.by_symbol[index] = Some(instrument);
    }

    #[must_use]
    pub fn get(&self, symbol: SymbolId) -> Option<&Instrument> {
        self.by_symbol.get(symbol as usize)?.as_ref()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Instrument> {
        self.by_symbol.iter().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instrument() -> Instrument {
        // Band runs from 10,000 to 75,535 ticks.
        Instrument::new(1, 100, 200, 10_000, 1_000_000, 1_024)
    }

    #[test]
    fn prices_inside_the_band_map_to_slots_and_back() {
        let i = instrument();
        assert_eq!(i.to_slot(10_000), Some(0));
        assert_eq!(i.to_slot(10_500), Some(500));
        assert_eq!(i.to_slot(i.ceiling_ticks()), Some(65_535));
        for price in [10_000, 42_000, 75_535] {
            assert_eq!(i.to_price(i.to_slot(price).unwrap()), price);
        }
    }

    #[test]
    fn prices_outside_the_band_are_rejected_rather_than_wrapped() {
        let i = instrument();
        assert_eq!(i.to_slot(9_999), None, "below the floor");
        assert_eq!(i.to_slot(75_536), None, "above the ceiling");
        assert_eq!(i.to_slot(Ticks::MIN), None);
        assert_eq!(i.to_slot(Ticks::MAX), None);
    }

    #[test]
    fn notional_reports_overflow_instead_of_wrapping() {
        let i = instrument();
        assert_eq!(i.notional(100, 5), Some(500));
        // A wrapped notional would under-reserve a balance, so it must be None.
        assert_eq!(i.notional(-1, 5), None, "negative price is not a notional");
        assert!(i.notional(Ticks::MAX, Quantity::MAX).is_some());
    }

    #[test]
    fn lookup_is_by_symbol_and_missing_symbols_are_absent() {
        let mut set = Instruments::new();
        set.insert(instrument());
        set.insert(Instrument::new(9, 100, 200, 0, 10, 1_024));
        assert_eq!(set.iter().count(), 2);
        assert_eq!(set.get(1).unwrap().floor_ticks, 10_000);
        assert_eq!(set.get(9).unwrap().floor_ticks, 0);
        assert!(set.get(2).is_none());
        assert!(set.get(1_000).is_none());
    }
}
