//! Hierarchical occupancy bitmap checks — the structure that finds the best
//! and next occupied price level in O(1).

use super::{CheckResult, Rng, Workload, require};
use crate::{HierarchicalBitmap, NO_ASK, NO_BID, PRICE_COUNT};
use std::collections::BTreeSet;
use std::ops::Bound;

pub fn bitmap_empty_and_idempotence(_workload: Workload) -> CheckResult {
    let mut bitmap = HierarchicalBitmap::new(PRICE_COUNT);
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
    let mut bitmap = HierarchicalBitmap::new(PRICE_COUNT);
    for value in 0..PRICE_COUNT {
        let price = value as u32;
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
    const PRICES: [u32; 14] = [
        0, 1, 62, 63, 64, 65, 4_094, 4_095, 4_096, 4_097, 65_470, 65_471, 65_534, 65_535,
    ];
    let mut bitmap = HierarchicalBitmap::new(PRICE_COUNT);
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
            PRICES[index - 1] as i32
        };
        let expected_next = if index + 1 == PRICES.len() {
            NO_ASK
        } else {
            PRICES[index + 1] as i32
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
    let mut bitmap = HierarchicalBitmap::new(PRICE_COUNT);
    for value in 0..PRICE_COUNT {
        bitmap.insert(value as u32);
    }
    require!(bitmap.validate());
    require!(bitmap.first() == 0);
    require!(bitmap.last() == 65_535);

    for value in 0..PRICE_COUNT {
        let price = value as u32;
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
        bitmap.remove(value as u32);
    }
    require!(bitmap.validate());
    require!(bitmap.first() == 1);
    require!(bitmap.last() == 65_535);
    Ok(())
}

pub fn bitmap_random_differential(workload: Workload) -> CheckResult {
    let mut bitmap = HierarchicalBitmap::new(PRICE_COUNT);
    let mut reference = BTreeSet::new();
    let mut rng = Rng::new(0xa174_5f9c_ca62_7123);

    for operation in 0..workload.scale(500_000) {
        let random = rng.next_u64();
        let price = random as u16;
        if (random >> 16) & 1 == 0 {
            bitmap.insert(u32::from(price));
            reference.insert(price);
        } else {
            bitmap.remove(u32::from(price));
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
            require!(bitmap.next(u32::from(query)) == expected_next, operation);
            require!(
                bitmap.previous(u32::from(query)) == expected_previous,
                operation
            );
        }
    }
    Ok(())
}
