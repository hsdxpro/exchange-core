//! Balances and reservation.
//!
//! An account's orders are all handled here before they reach a book, which is
//! what makes reserving across two symbols at once safe: there is one writer
//! per account, so a client with 100k who sends a 60k order on one symbol and a
//! 50k order on another cannot have both accepted.
//!
//! Balances are `u128` internally. A quote-side reservation is price times
//! quantity, and both are 64-bit on the wire, so the product needs the wider
//! type. Overflow returns an error rather than wrapping; a wrapped reservation
//! would let an account spend money it does not have.

use crate::instrument::AssetId;
use bx_protocol::AccountId;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Balance {
    /// Spendable right now.
    pub free: u128,
    /// Committed to resting orders. Still owned, not yet spendable.
    pub reserved: u128,
}

impl Balance {
    #[must_use]
    pub const fn total(&self) -> u128 {
        self.free + self.reserved
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BalanceError {
    Insufficient,
    /// Releasing or settling more than was reserved. Always a bug upstream, so
    /// it is surfaced rather than clamped.
    OverRelease,
    Overflow,
}

#[derive(Debug, Default)]
pub struct Accounts {
    balances: HashMap<(AccountId, AssetId), Balance>,
}

impl Accounts {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn deposit(&mut self, account: AccountId, asset: AssetId, amount: u128) {
        self.balances.entry((account, asset)).or_default().free += amount;
    }

    #[must_use]
    pub fn balance(&self, account: AccountId, asset: AssetId) -> Balance {
        self.balances
            .get(&(account, asset))
            .copied()
            .unwrap_or_default()
    }

    /// Moves `amount` from free to reserved.
    ///
    /// # Errors
    /// [`BalanceError::Insufficient`] if the free balance will not cover it.
    pub fn reserve(
        &mut self,
        account: AccountId,
        asset: AssetId,
        amount: u128,
    ) -> Result<(), BalanceError> {
        let balance = self.balances.entry((account, asset)).or_default();
        if balance.free < amount {
            return Err(BalanceError::Insufficient);
        }
        balance.free -= amount;
        balance.reserved = balance
            .reserved
            .checked_add(amount)
            .ok_or(BalanceError::Overflow)?;
        Ok(())
    }

    /// Moves `amount` back from reserved to free, when an order is cancelled or
    /// its unfilled remainder is released.
    ///
    /// # Errors
    /// [`BalanceError::OverRelease`] if more is released than was reserved.
    pub fn release(
        &mut self,
        account: AccountId,
        asset: AssetId,
        amount: u128,
    ) -> Result<(), BalanceError> {
        let balance = self.balances.entry((account, asset)).or_default();
        if balance.reserved < amount {
            return Err(BalanceError::OverRelease);
        }
        balance.reserved -= amount;
        balance.free += amount;
        Ok(())
    }

    /// Consumes a reservation because the order traded: the asset leaves the
    /// account entirely.
    ///
    /// # Errors
    /// [`BalanceError::OverRelease`] if more is settled than was reserved.
    pub fn settle_out(
        &mut self,
        account: AccountId,
        asset: AssetId,
        amount: u128,
    ) -> Result<(), BalanceError> {
        let balance = self.balances.entry((account, asset)).or_default();
        if balance.reserved < amount {
            return Err(BalanceError::OverRelease);
        }
        balance.reserved -= amount;
        Ok(())
    }

    /// Credits an account for the other side of a trade.
    ///
    /// # Errors
    /// [`BalanceError::Overflow`] if the balance would exceed `u128`.
    pub fn settle_in(
        &mut self,
        account: AccountId,
        asset: AssetId,
        amount: u128,
    ) -> Result<(), BalanceError> {
        let balance = self.balances.entry((account, asset)).or_default();
        balance.free = balance
            .free
            .checked_add(amount)
            .ok_or(BalanceError::Overflow)?;
        Ok(())
    }

    /// Total of free plus reserved across every account for one asset.
    ///
    /// Trading moves value between accounts; it never creates or destroys it.
    /// Tests assert this is invariant across a whole session, which catches a
    /// whole class of accounting bug that per-operation checks miss.
    #[must_use]
    pub fn total_supply(&self, asset: AssetId) -> u128 {
        self.balances
            .iter()
            .filter(|((_, a), _)| *a == asset)
            .map(|(_, b)| b.total())
            .sum()
    }

    #[must_use]
    pub fn accounts_holding(&self, asset: AssetId) -> Vec<AccountId> {
        let mut out: Vec<_> = self
            .balances
            .keys()
            .filter(|(_, a)| *a == asset)
            .map(|(account, _)| *account)
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const USD: AssetId = 1;
    const BTC: AssetId = 2;

    #[test]
    fn reserving_moves_free_to_reserved_without_changing_the_total() {
        let mut accounts = Accounts::new();
        accounts.deposit(1, USD, 1_000);
        accounts.reserve(1, USD, 400).unwrap();

        let balance = accounts.balance(1, USD);
        assert_eq!(balance.free, 600);
        assert_eq!(balance.reserved, 400);
        assert_eq!(balance.total(), 1_000);
    }

    #[test]
    fn a_reservation_beyond_the_free_balance_is_refused() {
        let mut accounts = Accounts::new();
        accounts.deposit(1, USD, 100);
        assert_eq!(
            accounts.reserve(1, USD, 101),
            Err(BalanceError::Insufficient)
        );
        // And the refusal leaves the balance untouched.
        assert_eq!(accounts.balance(1, USD).free, 100);
        assert_eq!(accounts.balance(1, USD).reserved, 0);
    }

    #[test]
    fn two_reservations_cannot_together_exceed_the_balance() {
        // The cross-symbol case: 100k of balance, a 60k order and a 50k order.
        let mut accounts = Accounts::new();
        accounts.deposit(1, USD, 100_000);
        accounts.reserve(1, USD, 60_000).unwrap();
        assert_eq!(
            accounts.reserve(1, USD, 50_000),
            Err(BalanceError::Insufficient),
            "the second order must not be accepted"
        );
        assert_eq!(accounts.balance(1, USD).free, 40_000);
    }

    #[test]
    fn releasing_returns_the_reservation_to_free() {
        let mut accounts = Accounts::new();
        accounts.deposit(1, USD, 1_000);
        accounts.reserve(1, USD, 400).unwrap();
        accounts.release(1, USD, 400).unwrap();
        assert_eq!(accounts.balance(1, USD).free, 1_000);
        assert_eq!(accounts.balance(1, USD).reserved, 0);
    }

    #[test]
    fn over_releasing_is_an_error_rather_than_silently_creating_value() {
        let mut accounts = Accounts::new();
        accounts.deposit(1, USD, 1_000);
        accounts.reserve(1, USD, 400).unwrap();
        assert_eq!(
            accounts.release(1, USD, 401),
            Err(BalanceError::OverRelease)
        );
        assert_eq!(
            accounts.settle_out(1, USD, 401),
            Err(BalanceError::OverRelease)
        );
        assert_eq!(accounts.balance(1, USD).total(), 1_000);
    }

    #[test]
    fn a_trade_conserves_supply_across_both_accounts() {
        let mut accounts = Accounts::new();
        accounts.deposit(1, USD, 1_000); // buyer
        accounts.deposit(2, BTC, 10); // seller

        let usd_before = accounts.total_supply(USD);
        let btc_before = accounts.total_supply(BTC);

        // Buyer reserves 500 USD, seller reserves 5 BTC, they trade.
        accounts.reserve(1, USD, 500).unwrap();
        accounts.reserve(2, BTC, 5).unwrap();
        accounts.settle_out(1, USD, 500).unwrap();
        accounts.settle_in(2, USD, 500).unwrap();
        accounts.settle_out(2, BTC, 5).unwrap();
        accounts.settle_in(1, BTC, 5).unwrap();

        assert_eq!(
            accounts.total_supply(USD),
            usd_before,
            "USD was created or destroyed"
        );
        assert_eq!(
            accounts.total_supply(BTC),
            btc_before,
            "BTC was created or destroyed"
        );
        assert_eq!(accounts.balance(1, BTC).free, 5);
        assert_eq!(accounts.balance(2, USD).free, 500);
        assert_eq!(accounts.balance(1, USD).total(), 500);
    }

    #[test]
    fn an_unknown_account_reads_as_empty_rather_than_failing() {
        let accounts = Accounts::new();
        assert_eq!(accounts.balance(999, USD), Balance::default());
        assert_eq!(accounts.total_supply(USD), 0);
    }
}
