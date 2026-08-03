//! Instrument definitions, and the mapping from wire prices to engine ticks.
//!
//! The wire carries a signed tick price that means something to a client; the
//! engine addresses a 31-bit price domain whose level tables follow the
//! prices that actually rest. An instrument connects the two, and its band
//! `[floor_ticks, ceiling_ticks]` is pure policy: the fat-finger control, and
//! the bound on the window clients can make the engine allocate — the window
//! is dense between the lowest and highest resting price, so the band an
//! operator declares is also the worst-case memory statement.

use bx_protocol::{Quantity, SymbolId, Ticks};

/// Band width [`Instrument::new`] defaults to when a configuration does not
/// state a ceiling: the engine's boot window, so a default instrument never
/// grows. A stated ceiling may widen the band to [`MAX_BAND_SLOTS`].
pub const DEFAULT_BAND_SLOTS: i64 = 65_536;

/// Widest band an instrument may declare: the engine's 31-bit price domain.
/// The type's limit, not a chosen number — 2,147,483,648 ticks.
pub const MAX_BAND_SLOTS: i64 = bx_engine::PRICE_LIMIT as i64;

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
    /// Lowest wire price the venue will accept for this instrument.
    pub floor_ticks: Ticks,
    /// Highest wire price the venue will accept. The band may span up to
    /// [`MAX_BAND_SLOTS`] ticks; it is policy, and the worst-case window the
    /// book can be made to allocate at 16 bytes a tick per side.
    pub ceiling_ticks: Ticks,
    /// Largest single-order quantity. Bounds the arithmetic below and stops one
    /// order from consuming the whole book by accident.
    pub max_quantity: Quantity,
    /// Resting-order pool the book boots with. A sizing hint, not a ceiling:
    /// the pool doubles when it fills, so this only decides the allocation a
    /// venue starts serving with. Insert, cancel and amend stay O(1); growth
    /// is amortised and off the steady-state path.
    ///
    /// This is unrelated to how many *price levels* the book has, and it is
    /// not a per-level limit: one price can hold every order in the pool,
    /// because a level is a head and tail index into it rather than a
    /// container.
    ///
    /// Cost: **48 bytes per slot** while allocated, ~25 more per *live* order
    /// for the client-ID index — about **70 bytes per order** in a full book.
    /// Each book additionally boots with a ~2.1 MiB level table that grows
    /// with the span between its lowest and highest resting prices. The hard
    /// ceiling is [`MAX_OPEN_ORDERS_LIMIT`], the engine's u32 slot index.
    pub max_open_orders: u32,
}

impl Instrument {
    /// An instrument with the default [`DEFAULT_BAND_SLOTS`]-tick band. A
    /// wider band is a policy statement made explicitly, via [`Self::banded`]
    /// or `ceiling_ticks` in configuration.
    #[must_use]
    pub const fn new(
        symbol: SymbolId,
        base: AssetId,
        quote: AssetId,
        floor_ticks: Ticks,
        max_quantity: Quantity,
        max_open_orders: u32,
    ) -> Self {
        // Checked: a floor near the top of `i64` would wrap the default
        // ceiling negative, and release builds do not trap overflow.
        let Some(ceiling_ticks) = floor_ticks.checked_add(DEFAULT_BAND_SLOTS - 1) else {
            panic!("floor_ticks leaves no room for the default band");
        };
        Self::banded(
            symbol,
            base,
            quote,
            floor_ticks,
            ceiling_ticks,
            max_quantity,
            max_open_orders,
        )
    }

    /// An instrument with an explicit band.
    ///
    /// # Panics
    /// If the band is empty or spans more than [`MAX_BAND_SLOTS`] ticks —
    /// configuration errors, refused before a book exists. The configuration
    /// parser validates the same bounds with a line number first.
    #[must_use]
    pub const fn banded(
        symbol: SymbolId,
        base: AssetId,
        quote: AssetId,
        floor_ticks: Ticks,
        ceiling_ticks: Ticks,
        max_quantity: Quantity,
        max_open_orders: u32,
    ) -> Self {
        // Prices are signed and a floor below zero is legitimate; what must
        // never happen is the pair wrapping, because release builds do not
        // trap overflow and a wrapped span passes the comparisons below.
        let Some(span) = ceiling_ticks.checked_sub(floor_ticks) else {
            panic!("instrument band is invalid: its width overflows");
        };
        assert!(span >= 0, "instrument band is empty: ceiling below floor");
        assert!(
            span < MAX_BAND_SLOTS,
            "instrument band exceeds the engine's 31-bit price domain"
        );
        Self {
            symbol,
            base,
            quote,
            floor_ticks,
            ceiling_ticks,
            max_quantity,
            max_open_orders,
        }
    }

    /// Ticks the band spans.
    #[must_use]
    pub const fn band_slots(&self) -> i64 {
        self.ceiling_ticks - self.floor_ticks + 1
    }

    /// Converts a wire price to an engine price, or `None` if it falls
    /// outside the band. This is the price-band check; there is no second one.
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn to_slot(&self, price: Ticks) -> Option<u32> {
        let offset = price.checked_sub(self.floor_ticks)?;
        if offset >= 0 && offset <= self.ceiling_ticks - self.floor_ticks {
            // The band assert bounds the offset below 2^31, so it fits.
            Some(offset as u32)
        } else {
            None
        }
    }

    /// Converts an engine price back to the wire price.
    #[must_use]
    pub fn to_price(&self, slot: u32) -> Ticks {
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
        assert_eq!(i.to_slot(i.ceiling_ticks), Some(65_535));
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
