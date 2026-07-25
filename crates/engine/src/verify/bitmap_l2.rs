//! Hierarchical-bitmap and L2 ladder checks.

use super::reference::ReferenceL2;
use super::{CheckResult, Rng, Workload, require, sparse_prices};
use crate::{HierarchicalBitmap, L2Book, NO_ASK, NO_BID, PRICE_COUNT, Side};
use std::collections::BTreeSet;
use std::ops::Bound;

pub fn bitmap_empty_and_idempotence(_workload: Workload) -> CheckResult {
    let mut bitmap = HierarchicalBitmap::new();
    require!(bitmap.is_empty());
    require!(bitmap.first() == NO_ASK);
    require!(bitmap.last() == NO_BID);
    require!(bitmap.next(0) == NO_ASK);
    require!(bitmap.previous(0) == NO_BID);
    require!(bitmap.validate());

    bitmap.remove(0);
    bitmap.remove(65_535);
    require!(bitmap.validate());

    bitmap.insert(100);
    bitmap.insert(100);
    require!(bitmap.first() == 100);
    require!(bitmap.last() == 100);
    bitmap.remove(100);
    bitmap.remove(100);
    require!(bitmap.is_empty());
    require!(bitmap.validate());
    Ok(())
}

pub fn bitmap_all_singletons(_workload: Workload) -> CheckResult {
    let mut bitmap = HierarchicalBitmap::new();
    for value in 0..PRICE_COUNT {
        let price = value as u16;
        bitmap.insert(price);
        require!(bitmap.contains(price), price);
        require!(bitmap.first() == value as i32, price);
        require!(bitmap.last() == value as i32, price);
        require!(bitmap.previous(price) == NO_BID, price);
        require!(bitmap.next(price) == NO_ASK, price);
        require!(bitmap.validate(), price);
        bitmap.remove(price);
    }
    require!(bitmap.is_empty());
    Ok(())
}

pub fn bitmap_hierarchy_boundaries(_workload: Workload) -> CheckResult {
    const PRICES: [u16; 14] = [
        0, 1, 62, 63, 64, 65, 4_094, 4_095, 4_096, 4_097, 65_470, 65_471, 65_534, 65_535,
    ];
    let mut bitmap = HierarchicalBitmap::new();
    for &price in &PRICES {
        bitmap.insert(price);
    }
    require!(bitmap.validate());
    require!(bitmap.first() == 0);
    require!(bitmap.last() == 65_535);

    for (index, &price) in PRICES.iter().enumerate() {
        let expected_previous = if index == 0 {
            NO_BID
        } else {
            i32::from(PRICES[index - 1])
        };
        let expected_next = if index + 1 == PRICES.len() {
            NO_ASK
        } else {
            i32::from(PRICES[index + 1])
        };
        require!(bitmap.previous(price) == expected_previous, price);
        require!(bitmap.next(price) == expected_next, price);
    }

    for &price in &PRICES {
        bitmap.remove(price);
        require!(bitmap.validate(), price);
    }
    require!(bitmap.is_empty());
    Ok(())
}

pub fn bitmap_full_domain_and_queries(_workload: Workload) -> CheckResult {
    let mut bitmap = HierarchicalBitmap::new();
    for value in 0..PRICE_COUNT {
        bitmap.insert(value as u16);
    }
    require!(bitmap.validate());
    require!(bitmap.first() == 0);
    require!(bitmap.last() == 65_535);

    for value in 0..PRICE_COUNT {
        let price = value as u16;
        let expected_previous = if value == 0 { NO_BID } else { value as i32 - 1 };
        let expected_next = if value == 65_535 {
            NO_ASK
        } else {
            value as i32 + 1
        };
        require!(bitmap.previous(price) == expected_previous, price);
        require!(bitmap.next(price) == expected_next, price);
    }

    for value in (0..PRICE_COUNT).step_by(2) {
        bitmap.remove(value as u16);
    }
    require!(bitmap.validate());
    require!(bitmap.first() == 1);
    require!(bitmap.last() == 65_535);
    Ok(())
}

pub fn bitmap_random_differential(workload: Workload) -> CheckResult {
    let mut bitmap = HierarchicalBitmap::new();
    let mut reference = BTreeSet::new();
    let mut rng = Rng::new(0xa174_5f9c_ca62_7123);

    for operation in 0..workload.scale(500_000) {
        let random = rng.next_u64();
        let price = random as u16;
        if (random >> 16) & 1 == 0 {
            bitmap.insert(price);
            reference.insert(price);
        } else {
            bitmap.remove(price);
            reference.remove(&price);
        }

        if operation & 255 != 255 {
            continue;
        }

        require!(bitmap.validate(), operation);
        require!(bitmap.is_empty() == reference.is_empty(), operation);
        let expected_first = reference.first().map_or(NO_ASK, |&price| i32::from(price));
        let expected_last = reference.last().map_or(NO_BID, |&price| i32::from(price));
        require!(bitmap.first() == expected_first, operation);
        require!(bitmap.last() == expected_last, operation);

        for _ in 0..64 {
            let query = rng.next_u64() as u16;
            let expected_next = reference
                .range((Bound::Excluded(query), Bound::Unbounded))
                .next()
                .map_or(NO_ASK, |&price| i32::from(price));
            let expected_previous = reference
                .range(..query)
                .next_back()
                .map_or(NO_BID, |&price| i32::from(price));
            require!(bitmap.next(query) == expected_next, operation);
            require!(bitmap.previous(query) == expected_previous, operation);
        }
    }
    Ok(())
}

pub fn l2_empty_boundaries_and_totals(_workload: Workload) -> CheckResult {
    let mut book = L2Book::new();
    require!(book.best_bid() == NO_BID);
    require!(book.best_ask() == NO_ASK);
    require!(book.total_quantity(Side::Bid) == 0);
    require!(book.total_quantity(Side::Ask) == 0);
    require!(book.validate());

    book.set_level(Side::Bid, 0, u32::MAX);
    book.set_level(Side::Bid, 65_535, 7);
    book.set_level(Side::Ask, 0, 11);
    book.set_level(Side::Ask, 65_535, u32::MAX);
    require!(book.best_bid() == 65_535);
    require!(book.best_ask() == 0);
    require!(book.total_quantity(Side::Bid) == u64::from(u32::MAX) + 7);
    require!(book.total_quantity(Side::Ask) == u64::from(u32::MAX) + 11);
    require!(book.validate());

    book.set_level(Side::Bid, 65_535, 0);
    book.set_level(Side::Ask, 0, 0);
    require!(book.best_bid() == 0);
    require!(book.best_ask() == 65_535);
    book.clear();
    require!(book.validate());
    require!(book.best_bid() == NO_BID);
    require!(book.best_ask() == NO_ASK);
    Ok(())
}

pub fn l2_top_order_and_limit(_workload: Workload) -> CheckResult {
    let mut book = L2Book::new();
    for (price, quantity) in [(100_u16, 1_u32), (90, 2), (110, 3), (95, 4)] {
        book.set_level(Side::Bid, price, quantity);
        book.set_level(Side::Ask, price, quantity + 10);
    }

    require!(book.for_each_top(Side::Bid, 0, |_, _| {}) == 0);
    let mut bids = Vec::new();
    require!(book.for_each_top(Side::Bid, 3, |price, _| bids.push(price)) == 3);
    require!(bids == [110, 100, 95]);

    let mut asks = Vec::new();
    require!(book.for_each_top(Side::Ask, 10, |price, _| asks.push(price)) == 4);
    require!(asks == [90, 95, 100, 110]);
    Ok(())
}

pub fn l2_sweep_zero_partial_exact_and_shortfall(_workload: Workload) -> CheckResult {
    let mut book = L2Book::new();
    book.set_level(Side::Ask, 100, 10);
    book.set_level(Side::Ask, 105, 20);
    book.set_level(Side::Ask, 200, 30);

    let zero = book.sweep(Side::Ask, 0);
    require!(zero.filled_quantity == 0);
    require!(zero.levels_visited == 0);

    let partial = book.sweep(Side::Ask, 5);
    require!(partial.filled_quantity == 5);
    require!(partial.notional_ticks == 500);
    require!(partial.levels_visited == 1);

    let exact = book.sweep(Side::Ask, 30);
    require!(exact.filled_quantity == 30);
    require!(exact.notional_ticks == 3_100);
    require!(exact.levels_visited == 2);

    let shortfall = book.sweep(Side::Ask, 100);
    require!(shortfall.filled_quantity == 60);
    require!(shortfall.notional_ticks == 9_100);
    require!(shortfall.levels_visited == 3);
    Ok(())
}

pub fn l2_sparse_top_1000(_workload: Workload) -> CheckResult {
    let mut book = L2Book::new();
    let bids = sparse_prices(512, 32_000, 1_000);
    let asks = sparse_prices(33_536, 65_000, 1_000);

    for index in 0..1_000 {
        book.set_level(Side::Bid, bids[index], index as u32 + 1);
        book.set_level(Side::Ask, asks[index], index as u32 + 1);
    }

    let mut actual_bids = Vec::new();
    let mut actual_asks = Vec::new();
    require!(book.for_each_top(Side::Bid, 1_000, |price, _| actual_bids.push(price)) == 1_000);
    require!(book.for_each_top(Side::Ask, 1_000, |price, _| actual_asks.push(price)) == 1_000);

    let mut expected_bids = bids;
    expected_bids.reverse();
    require!(actual_bids == expected_bids);
    require!(actual_asks == asks);

    let total = book.total_quantity(Side::Ask);
    let ask_sweep = book.sweep(Side::Ask, total);
    require!(ask_sweep.filled_quantity == total);
    require!(ask_sweep.levels_visited == 1_000);
    let average_price_is_positive = ask_sweep.average_price() > 0.0;
    require!(average_price_is_positive);
    require!(book.validate());
    Ok(())
}

pub fn l2_random_differential(workload: Workload) -> CheckResult {
    let mut fast = L2Book::new();
    let mut reference = ReferenceL2::new();
    let mut rng = Rng::new(0x54dd_19a6_e87f_1201);

    for operation in 0..workload.scale(600_000) {
        let random = rng.next_u64();
        let side = if (random >> 8) & 1 == 0 {
            Side::Bid
        } else {
            Side::Ask
        };
        let price = (random >> 16) as u16;
        let quantity = if (random >> 32).is_multiple_of(7) {
            0
        } else {
            1 + ((random >> 40) % 1_000_000) as u32
        };
        fast.set_level(side, price, quantity);
        reference.set_level(side, price, quantity);

        if operation & 2047 != 2047 {
            continue;
        }

        require!(fast.validate(), operation);
        require!(fast.best_bid() == reference.best_bid(), operation);
        require!(fast.best_ask() == reference.best_ask(), operation);
        require!(
            fast.total_quantity(Side::Bid) == reference.total_quantity(Side::Bid),
            operation
        );
        require!(
            fast.total_quantity(Side::Ask) == reference.total_quantity(Side::Ask),
            operation
        );
        require!(fast.state_hash() == reference.state_hash(), operation);
        require!(
            fast.quantity(side, price) == reference.quantity(side, price),
            operation
        );

        for query_side in [Side::Bid, Side::Ask] {
            let mut actual = Vec::new();
            fast.for_each_top(query_side, 1_000, |level_price, level_quantity| {
                actual.push((level_price, level_quantity));
            });
            require!(actual == reference.top(query_side, 1_000), operation);

            let target = rng.next_u64() % (reference.total_quantity(query_side) + 1);
            require!(
                fast.sweep(query_side, target) == reference.sweep(query_side, target),
                operation
            );
        }
    }
    Ok(())
}
