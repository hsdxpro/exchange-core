//! Fixtures shared by the integration suites.
//!
//! Each integration test file is its own binary, so an item only one of them
//! uses looks dead to the others. That is what the allow is for; it is not
//! hiding unused code in the crate proper.
#![allow(dead_code)]

use bx_journal::MemoryLog;
use bx_pipeline::Exchange;
use bx_pipeline::instrument::{AssetId, Instrument, Instruments};
use bx_pipeline::{limit_order, market_order};
use bx_protocol::{AccountId, Command, CommandKind, Side, Ticks, TimeInForce};

pub const BTC: AssetId = 1;
pub const USD: AssetId = 2;
pub const SYMBOL: u32 = 1;
pub const FLOOR: Ticks = 10_000;
pub const MAX_QUANTITY: u64 = 1_000_000;
/// Resting-order pool for the test instrument. Large enough that no test hits
/// it by accident, which would turn a real assertion into a capacity reject.
pub const MAX_OPEN_ORDERS: u32 = 200_000;

/// Accounts the funded fixtures create, and what each starts with.
pub const ACCOUNTS: std::ops::RangeInclusive<AccountId> = 1..=8;
pub const START_USD: u64 = 100_000_000;
pub const START_BTC: u64 = 10_000;

#[must_use]
pub fn instruments() -> Instruments {
    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(
        SYMBOL,
        BTC,
        USD,
        FLOOR,
        MAX_QUANTITY,
        MAX_OPEN_ORDERS,
    ));
    instruments
}

/// A venue with the instrument listed and no money in it.
#[must_use]
pub fn venue() -> Exchange<MemoryLog> {
    Exchange::new(MemoryLog::new(), instruments()).unwrap()
}

/// A venue with every test account funded.
#[must_use]
pub fn funded() -> Exchange<MemoryLog> {
    let mut exchange = venue();
    fund(&mut exchange);
    exchange
}

/// Credits the starting balances. These are journalled, so a recovery test does
/// not re-apply them: replay reproduces them.
pub fn fund(exchange: &mut Exchange<MemoryLog>) {
    for account in ACCOUNTS {
        exchange.deposit(account, USD, START_USD).unwrap();
        exchange.deposit(account, BTC, START_BTC).unwrap();
    }
}

#[must_use]
pub fn cancel(account: AccountId, order_id: u64) -> Command {
    Command::new(
        CommandKind::Cancel,
        account,
        SYMBOL,
        order_id,
        Side::Bid,
        0,
        0,
        TimeInForce::GoodTillCancel,
    )
}

/// A population of traders driving the venue through the public API only.
///
/// Deterministic: the same seed produces the same order flow, so a failure is
/// reproducible from the seed alone.
pub struct TraderPopulation {
    state: u64,
    next_order: u64,
}

impl TraderPopulation {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed,
            next_order: 1,
        }
    }

    pub fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.state >> 16
    }

    /// Produces one plausible action: post, cancel, or take.
    pub fn act(&mut self, resting: &mut Vec<(u64, AccountId)>) -> Command {
        let roll = self.next() % 100;
        let account = 1 + self.next() % 8;
        let order_id = self.next_order;
        self.next_order += 1;

        if roll < 20 && !resting.is_empty() {
            let index = (self.next() as usize) % resting.len();
            let (id, owner) = resting.remove(index);
            return cancel(owner, id);
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
