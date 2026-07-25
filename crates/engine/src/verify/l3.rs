//! L3 order-book and matching-engine scenario checks.

use super::{CheckResult, Workload, require};
use crate::{
    ConfigurationError, Fill, L3Book, NO_ASK, NO_BID, OrderError, OrderSlot, PriceLevel, Side,
    TimeInForce,
};
use std::mem::size_of;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Checks the full FIFO queue at a price, including the side/price stamped on
/// every slot, which is what makes direct-ID lookup trustworthy.
fn require_queue(book: &L3Book, side: Side, price: u16, expected: &[(u32, u32)]) -> CheckResult {
    let mut actual = Vec::new();
    book.for_each_order_at_level(side, price, |order| actual.push(order));
    require!(actual.len() == expected.len(), price);
    for (order, &(id, quantity)) in actual.iter().zip(expected) {
        require!(order.id == id, price);
        require!(order.side == side, price);
        require!(order.price == price, price);
        require!(order.quantity == quantity, price);
    }
    Ok(())
}

fn collect_fills<F>(action: F) -> (Result<crate::ExecutionReport, OrderError>, Vec<Fill>)
where
    F: FnOnce(&mut dyn FnMut(Fill)) -> Result<crate::ExecutionReport, OrderError>,
{
    let mut fills = Vec::new();
    let result = {
        let mut sink = |fill: Fill| fills.push(fill);
        action(&mut sink)
    };
    (result, fills)
}

pub fn compact_layout_and_empty_state(_workload: Workload) -> CheckResult {
    require!(size_of::<OrderSlot>() == 24);
    require!(size_of::<PriceLevel>() == 16);

    let mut book = L3Book::new(0, 0);
    require!(book.capacity() == 0);
    require!(book.order_id_capacity() == 0);
    require!(book.live_orders() == 0);
    require!(book.best_bid() == NO_BID);
    require!(book.best_ask() == NO_ASK);
    require!(book.validate());
    require!(book.add_passive(0, Side::Bid, 100, 1) == Err(OrderError::OrderIdOutOfRange));
    Ok(())
}

pub fn invalid_capacity_is_rejected_before_allocation(_workload: Workload) -> CheckResult {
    require!(L3Book::try_new(crate::INVALID_INDEX as usize, 1).err() == Some(ConfigurationError));
    require!(L3Book::try_new(4, 8).is_ok());
    Ok(())
}

pub fn zero_capacity_and_empty_book_time_in_force(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(0, 8);
    let before = book.state_hash();

    require!(
        book.submit_limit(1, Side::Bid, 100, 7, TimeInForce::GoodTillCancel)
            == Err(OrderError::CapacityExceeded)
    );
    require!(
        book.submit_limit(1, Side::Bid, 100, 7, TimeInForce::PostOnly)
            == Err(OrderError::CapacityExceeded)
    );

    let ioc = book.submit_limit(1, Side::Bid, 100, 7, TimeInForce::ImmediateOrCancel)?;
    require!(ioc.traded_quantity == 0);
    require!(ioc.canceled_quantity == 7);

    require!(
        book.submit_limit(1, Side::Bid, 100, 7, TimeInForce::FillOrKill)
            == Err(OrderError::InsufficientLiquidity)
    );

    let market_ioc = book.submit_market(2, Side::Ask, 9, TimeInForce::ImmediateOrCancel)?;
    require!(market_ioc.canceled_quantity == 9);
    require!(
        book.submit_market(2, Side::Ask, 9, TimeInForce::FillOrKill)
            == Err(OrderError::InsufficientLiquidity)
    );
    require!(
        book.submit_market(2, Side::Ask, 9, TimeInForce::PostOnly)
            == Err(OrderError::UnsupportedTimeInForce)
    );
    require!(
        book.submit_market(2, Side::Ask, 9, TimeInForce::GoodTillCancel)
            == Err(OrderError::UnsupportedTimeInForce)
    );

    require!(book.state_hash() == before);
    require!(book.validate());
    Ok(())
}

pub fn passive_add_validation_and_cross_prevention(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(3, 4);
    require!(book.add_passive(0, Side::Bid, 100, 0) == Err(OrderError::QuantityZero));
    require!(book.add_passive(4, Side::Bid, 100, 1) == Err(OrderError::OrderIdOutOfRange));
    require!(book.add_passive(0, Side::Bid, 100, 10).is_ok());
    require!(book.add_passive(0, Side::Bid, 99, 10) == Err(OrderError::DuplicateOrderId));
    require!(book.add_passive(1, Side::Ask, 100, 10) == Err(OrderError::WouldCross));
    require!(book.add_passive(1, Side::Ask, 101, 10).is_ok());
    require!(book.add_passive(2, Side::Bid, 99, 10).is_ok());
    require!(book.add_passive(3, Side::Ask, 102, 10) == Err(OrderError::CapacityExceeded));
    require!(book.live_orders() == 3);
    require!(book.best_bid() == 100);
    require!(book.best_ask() == 101);
    require!(book.validate());
    Ok(())
}

pub fn boundary_prices_and_best_updates(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(8, 16);
    require!(book.add_passive(1, Side::Bid, 0, 1).is_ok());
    require!(book.add_passive(2, Side::Bid, 65_534, 2).is_ok());
    require!(book.add_passive(3, Side::Ask, 65_535, 3).is_ok());
    require!(book.best_bid() == 65_534);
    require!(book.best_ask() == 65_535);
    require!(book.cancel(2).is_ok());
    require!(book.best_bid() == 0);
    require!(book.cancel(1).is_ok());
    require!(book.best_bid() == NO_BID);
    require!(book.cancel(3).is_ok());
    require!(book.best_ask() == NO_ASK);
    require!(book.validate());
    Ok(())
}

pub fn strict_fifo_and_maker_price(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(16, 32);
    require!(book.add_passive(1, Side::Ask, 100, 10).is_ok());
    require!(book.add_passive(2, Side::Ask, 100, 20).is_ok());
    require!(book.add_passive(3, Side::Ask, 100, 30).is_ok());

    let (result, fills) = collect_fills(|sink| {
        book.submit_limit_with(10, Side::Bid, 120, 35, TimeInForce::ImmediateOrCancel, sink)
    });
    let report = result?;

    require!(report.fills == 3);
    require!(report.traded_quantity == 35);
    require!(report.notional_ticks == 3_500);
    require!(report.rested_quantity == 0);
    require!(report.canceled_quantity == 0);
    require!(
        fills
            == [
                Fill {
                    maker_order_id: 1,
                    maker_side: Side::Ask,
                    price: 100,
                    quantity: 10
                },
                Fill {
                    maker_order_id: 2,
                    maker_side: Side::Ask,
                    price: 100,
                    quantity: 20
                },
                Fill {
                    maker_order_id: 3,
                    maker_side: Side::Ask,
                    price: 100,
                    quantity: 5
                },
            ]
    );
    require_queue(&book, Side::Ask, 100, &[(3, 25)])?;
    require!(book.validate());
    Ok(())
}

pub fn panicking_fill_callback_preserves_book_invariants(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(8, 16);
    require!(book.add_passive(1, Side::Ask, 100, 3).is_ok());
    require!(book.add_passive(2, Side::Ask, 101, 4).is_ok());

    // Fills are published only after they are committed, so a callback that
    // unwinds cannot leave the book half-updated.
    let unwound = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.submit_limit_with(
            10,
            Side::Bid,
            101,
            7,
            TimeInForce::ImmediateOrCancel,
            |_| panic!("verification callback failure"),
        );
    }));

    require!(unwound.is_err());
    require!(!book.contains(1));
    require!(book.contains(2));
    require!(!book.contains(10));
    require!(book.live_orders() == 1);
    require!(book.best_ask() == 101);
    require_queue(&book, Side::Ask, 101, &[(2, 4)])?;
    require!(book.validate());
    Ok(())
}

pub fn price_priority_and_multi_level_matching(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(32, 64);
    require!(book.add_passive(1, Side::Ask, 102, 7).is_ok());
    require!(book.add_passive(2, Side::Ask, 100, 5).is_ok());
    require!(book.add_passive(3, Side::Ask, 101, 6).is_ok());
    require!(book.add_passive(4, Side::Ask, 100, 4).is_ok());

    let (result, fills) = collect_fills(|sink| {
        book.submit_limit_with(20, Side::Bid, 101, 13, TimeInForce::ImmediateOrCancel, sink)
    });
    let report = result?;

    require!(
        fills
            == [
                Fill {
                    maker_order_id: 2,
                    maker_side: Side::Ask,
                    price: 100,
                    quantity: 5
                },
                Fill {
                    maker_order_id: 4,
                    maker_side: Side::Ask,
                    price: 100,
                    quantity: 4
                },
                Fill {
                    maker_order_id: 3,
                    maker_side: Side::Ask,
                    price: 101,
                    quantity: 4
                },
            ]
    );
    require!(report.notional_ticks == 5 * 100 + 4 * 100 + 4 * 101);
    require_queue(&book, Side::Ask, 101, &[(3, 2)])?;
    require_queue(&book, Side::Ask, 102, &[(1, 7)])?;
    require!(book.best_ask() == 101);
    require!(book.validate());
    Ok(())
}

pub fn limit_price_stops_matching_and_fok_preview(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(8, 16);
    require!(book.add_passive(1, Side::Ask, 100, 2).is_ok());
    require!(book.add_passive(2, Side::Ask, 101, 3).is_ok());
    let before = book.state_hash();

    require!(
        book.submit_limit(10, Side::Bid, 100, 3, TimeInForce::FillOrKill)
            == Err(OrderError::InsufficientLiquidity)
    );
    require!(book.state_hash() == before);

    let (result, fills) = collect_fills(|sink| {
        book.submit_limit_with(10, Side::Bid, 100, 4, TimeInForce::ImmediateOrCancel, sink)
    });
    let report = result?;
    require!(report.traded_quantity == 2);
    require!(report.canceled_quantity == 2);
    require!(
        fills
            == [Fill {
                maker_order_id: 1,
                maker_side: Side::Ask,
                price: 100,
                quantity: 2
            }]
    );
    require_queue(&book, Side::Ask, 101, &[(2, 3)])?;
    require!(book.best_ask() == 101);
    require!(book.validate());
    Ok(())
}

pub fn sell_side_symmetry(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(16, 32);
    require!(book.add_passive(1, Side::Bid, 100, 3).is_ok());
    require!(book.add_passive(2, Side::Bid, 102, 4).is_ok());
    require!(book.add_passive(3, Side::Bid, 101, 5).is_ok());

    let (result, fills) = collect_fills(|sink| {
        book.submit_limit_with(10, Side::Ask, 100, 8, TimeInForce::ImmediateOrCancel, sink)
    });
    result?;
    require!(
        fills
            == [
                Fill {
                    maker_order_id: 2,
                    maker_side: Side::Bid,
                    price: 102,
                    quantity: 4
                },
                Fill {
                    maker_order_id: 3,
                    maker_side: Side::Bid,
                    price: 101,
                    quantity: 4
                },
            ]
    );
    require_queue(&book, Side::Bid, 101, &[(3, 1)])?;
    require_queue(&book, Side::Bid, 100, &[(1, 3)])?;
    require!(book.best_bid() == 101);
    require!(book.validate());
    Ok(())
}

pub fn market_sell_side_symmetry(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(8, 16);
    require!(book.add_passive(1, Side::Bid, 10, 2).is_ok());
    require!(book.add_passive(2, Side::Bid, 65_535, 3).is_ok());

    let (result, fills) = collect_fills(|sink| {
        book.submit_market_with(10, Side::Ask, 4, TimeInForce::ImmediateOrCancel, sink)
    });
    let report = result?;
    require!(report.traded_quantity == 4);
    require!(
        fills
            == [
                Fill {
                    maker_order_id: 2,
                    maker_side: Side::Bid,
                    price: 65_535,
                    quantity: 3
                },
                Fill {
                    maker_order_id: 1,
                    maker_side: Side::Bid,
                    price: 10,
                    quantity: 1
                },
            ]
    );
    require_queue(&book, Side::Bid, 10, &[(1, 1)])?;
    require!(book.best_bid() == 10);
    require!(book.validate());
    Ok(())
}

pub fn gtc_rests_and_crosses_then_rests(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(16, 32);
    let passive = book.submit_limit(1, Side::Bid, 99, 10, TimeInForce::GoodTillCancel)?;
    require!(passive.rested_quantity == 10);
    require_queue(&book, Side::Bid, 99, &[(1, 10)])?;

    require!(book.add_passive(2, Side::Ask, 101, 5).is_ok());
    let crossing = book.submit_limit(3, Side::Bid, 101, 12, TimeInForce::GoodTillCancel)?;
    require!(crossing.traded_quantity == 5);
    require!(crossing.rested_quantity == 7);
    require!(!book.contains(2));
    require_queue(&book, Side::Bid, 101, &[(3, 7)])?;
    require!(book.best_bid() == 101);
    require!(book.best_ask() == NO_ASK);
    require!(book.validate());
    Ok(())
}

pub fn ioc_semantics(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(8, 16);
    let empty = book.submit_limit(1, Side::Bid, 100, 10, TimeInForce::ImmediateOrCancel)?;
    require!(empty.traded_quantity == 0);
    require!(empty.canceled_quantity == 10);
    require!(!book.contains(1));

    require!(book.add_passive(2, Side::Ask, 100, 4).is_ok());
    let partial = book.submit_limit(3, Side::Bid, 100, 10, TimeInForce::ImmediateOrCancel)?;
    require!(partial.traded_quantity == 4);
    require!(partial.canceled_quantity == 6);
    require!(book.live_orders() == 0);
    require!(book.validate());
    Ok(())
}

pub fn fok_is_atomic(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(16, 32);
    require!(book.add_passive(1, Side::Ask, 100, 4).is_ok());
    require!(book.add_passive(2, Side::Ask, 101, 6).is_ok());
    let before = book.state_hash();

    let (rejected, rejected_fills) = collect_fills(|sink| {
        book.submit_limit_with(10, Side::Bid, 101, 11, TimeInForce::FillOrKill, sink)
    });
    require!(rejected == Err(OrderError::InsufficientLiquidity));
    require!(rejected_fills.is_empty());
    require!(book.state_hash() == before);

    let (result, fills) = collect_fills(|sink| {
        book.submit_limit_with(11, Side::Bid, 101, 10, TimeInForce::FillOrKill, sink)
    });
    let filled = result?;
    require!(filled.traded_quantity == 10);
    require!(filled.canceled_quantity == 0);
    require!(filled.rested_quantity == 0);
    require!(
        fills
            == [
                Fill {
                    maker_order_id: 1,
                    maker_side: Side::Ask,
                    price: 100,
                    quantity: 4
                },
                Fill {
                    maker_order_id: 2,
                    maker_side: Side::Ask,
                    price: 101,
                    quantity: 6
                },
            ]
    );
    require!(book.live_orders() == 0);
    require!(book.validate());
    Ok(())
}

pub fn post_only_is_atomic(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(4, 8);
    require!(book.add_passive(1, Side::Ask, 100, 5).is_ok());
    let before = book.state_hash();

    require!(
        book.submit_limit(2, Side::Bid, 100, 3, TimeInForce::PostOnly)
            == Err(OrderError::WouldCross)
    );
    require!(book.state_hash() == before);

    let accepted = book.submit_limit(2, Side::Bid, 99, 3, TimeInForce::PostOnly)?;
    require!(accepted.rested_quantity == 3);
    require_queue(&book, Side::Bid, 99, &[(2, 3)])?;
    require!(book.validate());
    Ok(())
}

pub fn cancel_head_middle_tail_and_only_order(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(16, 32);
    for id in 1..=4_u32 {
        require!(book.add_passive(id, Side::Ask, 100, id * 10).is_ok(), id);
    }
    require!(book.cancel(1).is_ok());
    require_queue(&book, Side::Ask, 100, &[(2, 20), (3, 30), (4, 40)])?;
    require!(book.cancel(3).is_ok());
    require_queue(&book, Side::Ask, 100, &[(2, 20), (4, 40)])?;
    require!(book.cancel(4).is_ok());
    require_queue(&book, Side::Ask, 100, &[(2, 20)])?;
    require!(book.cancel(2).is_ok());
    require!(book.best_ask() == NO_ASK);
    require!(book.cancel(2) == Err(OrderError::UnknownOrderId));
    require!(book.cancel(99) == Err(OrderError::OrderIdOutOfRange));
    require!(book.validate());
    Ok(())
}

pub fn amend_down_preserves_priority(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(16, 32);
    require!(book.add_passive(1, Side::Ask, 100, 10).is_ok());
    require!(book.add_passive(2, Side::Ask, 100, 20).is_ok());
    require!(book.add_passive(3, Side::Ask, 100, 30).is_ok());

    require!(book.amend_down(2, 15).is_ok());
    require_queue(&book, Side::Ask, 100, &[(1, 10), (2, 15), (3, 30)])?;
    require!(book.level_quantity(Side::Ask, 100) == 55);
    require!(book.amend_down(2, 15).is_ok());
    require!(book.amend_down(2, 16) == Err(OrderError::QuantityIncreaseNotAllowed));
    require!(book.amend_down(2, 0).is_ok());
    require_queue(&book, Side::Ask, 100, &[(1, 10), (3, 30)])?;
    require!(book.amend_down(2, 0) == Err(OrderError::UnknownOrderId));
    require!(book.amend_down(99, 0) == Err(OrderError::OrderIdOutOfRange));
    require!(book.validate());
    Ok(())
}

pub fn cancel_replace_gets_new_priority(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(16, 64);
    require!(book.add_passive(1, Side::Ask, 100, 10).is_ok());
    require!(book.add_passive(2, Side::Ask, 100, 20).is_ok());
    require!(book.add_passive(3, Side::Ask, 100, 30).is_ok());

    book.cancel_replace(2, 20, 100, 25)?;
    require!(!book.contains(2));
    require!(book.contains(20));
    require_queue(&book, Side::Ask, 100, &[(1, 10), (3, 30), (20, 25)])?;

    require!(book.add_passive(4, Side::Bid, 99, 7).is_ok());
    let (result, fills) = collect_fills(|sink| book.cancel_replace_with(4, 21, 100, 12, sink));
    let crossing = result?;
    require!(crossing.traded_quantity == 12);
    require!(
        fills
            == [
                Fill {
                    maker_order_id: 1,
                    maker_side: Side::Ask,
                    price: 100,
                    quantity: 10
                },
                Fill {
                    maker_order_id: 3,
                    maker_side: Side::Ask,
                    price: 100,
                    quantity: 2
                },
            ]
    );
    require!(!book.contains(4));
    require!(!book.contains(21));
    require_queue(&book, Side::Ask, 100, &[(3, 28), (20, 25)])?;
    require!(book.validate());
    Ok(())
}

pub fn cancel_replace_crosses_then_rests(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(2, 16);
    require!(book.add_passive(1, Side::Ask, 100, 5).is_ok());
    require!(book.add_passive(2, Side::Bid, 99, 10).is_ok());

    let (result, fills) = collect_fills(|sink| book.cancel_replace_with(2, 3, 100, 12, sink));
    let report = result?;
    require!(report.traded_quantity == 5);
    require!(report.rested_quantity == 7);
    require!(
        fills
            == [Fill {
                maker_order_id: 1,
                maker_side: Side::Ask,
                price: 100,
                quantity: 5
            }]
    );
    require!(!book.contains(1));
    require!(!book.contains(2));
    require_queue(&book, Side::Bid, 100, &[(3, 7)])?;
    require!(book.best_bid() == 100);
    require!(book.best_ask() == NO_ASK);
    require!(book.validate());
    Ok(())
}

pub fn invalid_cancel_replace_is_atomic(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(8, 16);
    require!(book.add_passive(1, Side::Bid, 100, 10).is_ok());
    require!(book.add_passive(2, Side::Bid, 100, 20).is_ok());
    let before = book.state_hash();

    require!(book.cancel_replace(9, 3, 101, 1) == Err(OrderError::UnknownOrderId));
    require!(book.cancel_replace(1, 1, 101, 1) == Err(OrderError::ReplacementIdMustDiffer));
    require!(book.cancel_replace(1, 2, 101, 1) == Err(OrderError::DuplicateOrderId));
    require!(book.cancel_replace(1, 20, 101, 1) == Err(OrderError::OrderIdOutOfRange));
    require!(book.cancel_replace(1, 3, 101, 0) == Err(OrderError::QuantityZero));
    require!(book.state_hash() == before);
    require_queue(&book, Side::Bid, 100, &[(1, 10), (2, 20)])?;
    require!(book.validate());
    Ok(())
}

pub fn slot_reuse_and_free_list_integrity(workload: Workload) -> CheckResult {
    let mut book = L3Book::new(3, 64);
    for cycle in 0..workload.scale(10_000) as u32 {
        let base = (cycle % 20) * 3;
        require!(book.add_passive(base, Side::Bid, 99, 1).is_ok(), cycle);
        require!(book.add_passive(base + 1, Side::Ask, 101, 1).is_ok(), cycle);
        require!(book.add_passive(base + 2, Side::Ask, 102, 1).is_ok(), cycle);
        require!(book.cancel(base + 1).is_ok(), cycle);
        require!(book.cancel(base).is_ok(), cycle);
        require!(book.cancel(base + 2).is_ok(), cycle);
        require!(book.live_orders() == 0, cycle);
        require!(book.validate(), cycle);
    }
    Ok(())
}

pub fn full_capacity_aggressive_orders(_workload: Workload) -> CheckResult {
    {
        // Taker consumes the only maker exactly: the released slot is not needed.
        let mut book = L3Book::new(1, 8);
        require!(book.add_passive(1, Side::Ask, 100, 10).is_ok());
        let report = book.submit_limit(2, Side::Bid, 100, 10, TimeInForce::GoodTillCancel)?;
        require!(report.traded_quantity == 10);
        require!(report.rested_quantity == 0);
        require!(book.live_orders() == 0);
        require!(book.validate());
    }
    {
        // Remainder rests into the slot the fully-filled maker just released.
        let mut book = L3Book::new(1, 8);
        require!(book.add_passive(1, Side::Ask, 100, 10).is_ok());
        let report = book.submit_limit(2, Side::Bid, 100, 15, TimeInForce::GoodTillCancel)?;
        require!(report.traded_quantity == 10);
        require!(report.rested_quantity == 5);
        require_queue(&book, Side::Bid, 100, &[(2, 5)])?;
        require!(book.validate());
    }
    {
        // Nothing crosses, so no slot is released and the GTC must be rejected.
        let mut book = L3Book::new(1, 8);
        require!(book.add_passive(1, Side::Ask, 101, 10).is_ok());
        let before = book.state_hash();
        require!(
            book.submit_limit(2, Side::Bid, 100, 5, TimeInForce::GoodTillCancel)
                == Err(OrderError::CapacityExceeded)
        );
        require!(book.state_hash() == before);
        require!(book.validate());
    }
    {
        // IOC never rests, so capacity pressure does not apply.
        let mut book = L3Book::new(1, 8);
        require!(book.add_passive(1, Side::Ask, 101, 10).is_ok());
        let report = book.submit_limit(2, Side::Bid, 100, 5, TimeInForce::ImmediateOrCancel)?;
        require!(report.canceled_quantity == 5);
        require!(book.live_orders() == 1);
        require!(book.validate());
    }
    Ok(())
}

pub fn incoming_validation_is_atomic(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(4, 8);
    require!(book.add_passive(1, Side::Ask, 100, 10).is_ok());
    let before = book.state_hash();

    require!(
        book.submit_limit(2, Side::Bid, 100, 0, TimeInForce::GoodTillCancel)
            == Err(OrderError::QuantityZero)
    );
    require!(
        book.submit_limit(8, Side::Bid, 100, 1, TimeInForce::GoodTillCancel)
            == Err(OrderError::OrderIdOutOfRange)
    );
    require!(
        book.submit_limit(1, Side::Bid, 100, 1, TimeInForce::GoodTillCancel)
            == Err(OrderError::DuplicateOrderId)
    );
    // The C++ suite also casts an out-of-range integer to TimeInForce here.
    // Rust's enum makes that unrepresentable, so the equivalent reachable path
    // is an unsupported TIF on the market-order entry point.
    require!(
        book.submit_market(2, Side::Bid, 1, TimeInForce::PostOnly)
            == Err(OrderError::UnsupportedTimeInForce)
    );
    require!(book.state_hash() == before);
    require!(book.validate());
    Ok(())
}

pub fn market_ioc_and_fok(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(16, 32);
    require!(book.add_passive(1, Side::Ask, 0, 2).is_ok());
    require!(book.add_passive(2, Side::Ask, 65_535, 3).is_ok());

    require!(
        book.submit_market(10, Side::Bid, 1, TimeInForce::GoodTillCancel)
            == Err(OrderError::UnsupportedTimeInForce)
    );

    let before = book.state_hash();
    require!(
        book.submit_market(10, Side::Bid, 6, TimeInForce::FillOrKill)
            == Err(OrderError::InsufficientLiquidity)
    );
    require!(book.state_hash() == before);

    let ioc = book.submit_market(10, Side::Bid, 4, TimeInForce::ImmediateOrCancel)?;
    require!(ioc.traded_quantity == 4);
    // Two units at tick 0 contribute nothing; two at tick 65,535 carry the notional.
    require!(ioc.notional_ticks == 2 * 65_535);
    require_queue(&book, Side::Ask, 65_535, &[(2, 1)])?;
    require!(book.validate());
    Ok(())
}

pub fn maximum_quantity_and_notional_are_safe(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(4, 8);
    require!(book.add_passive(1, Side::Ask, 65_535, u32::MAX).is_ok());
    let report = book.submit_market(2, Side::Bid, u32::MAX, TimeInForce::FillOrKill)?;
    require!(report.traded_quantity == u64::from(u32::MAX));
    require!(report.notional_ticks == 65_535 * u64::from(u32::MAX));
    require!(book.live_orders() == 0);
    require!(book.validate());
    Ok(())
}

pub fn aggregate_quantity_uses_64_bits(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(4, 8);
    require!(book.add_passive(1, Side::Ask, 65_535, u32::MAX).is_ok());
    require!(book.add_passive(2, Side::Ask, 65_535, u32::MAX).is_ok());
    require!(book.level_quantity(Side::Ask, 65_535) == 2 * u64::from(u32::MAX));

    let report = book.submit_market(3, Side::Bid, u32::MAX, TimeInForce::FillOrKill)?;
    require!(report.traded_quantity == u64::from(u32::MAX));
    require!(report.notional_ticks == 65_535 * u64::from(u32::MAX));
    require_queue(&book, Side::Ask, 65_535, &[(2, u32::MAX)])?;
    require!(book.level_quantity(Side::Ask, 65_535) == u64::from(u32::MAX));
    require!(book.validate());
    Ok(())
}

pub fn level_iteration_and_order_lookup(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(16, 32);
    require!(book.add_passive(1, Side::Bid, 98, 1).is_ok());
    require!(book.add_passive(2, Side::Bid, 100, 2).is_ok());
    require!(book.add_passive(3, Side::Bid, 99, 3).is_ok());
    require!(book.add_passive(4, Side::Bid, 100, 4).is_ok());
    require!(book.add_passive(5, Side::Ask, 102, 5).is_ok());
    require!(book.add_passive(6, Side::Ask, 101, 6).is_ok());

    let mut bid_levels = Vec::new();
    require!(
        book.for_each_level(Side::Bid, 2, |price, quantity| bid_levels
            .push((price, quantity)))
            == 2
    );
    require!(bid_levels == [(100_u16, 6_u64), (99, 3)]);

    let mut ask_prices = Vec::new();
    require!(book.for_each_level(Side::Ask, 8, |price, _| ask_prices.push(price)) == 2);
    require!(ask_prices == [101, 102]);
    require!(book.for_each_level(Side::Ask, 0, |_, _| {}) == 0);

    let order = book.order(4).ok_or("order 4 must be live")?;
    require!(order.id == 4);
    require!(order.side == Side::Bid);
    require!(order.price == 100);
    require!(order.quantity == 4);
    require!(book.order(31).is_none());
    require_queue(&book, Side::Bid, 100, &[(2, 2), (4, 4)])?;
    require!(book.validate());
    Ok(())
}

pub fn sparse_1000_level_sweep(_workload: Workload) -> CheckResult {
    let mut book = L3Book::new(1_100, 2_000);
    for index in 0..1_000_u32 {
        let price = (30_000 + index * 31) as u16;
        require!(
            book.add_passive(index + 1, Side::Ask, price, 1).is_ok(),
            index
        );
    }

    let (result, fills) = collect_fills(|sink| {
        book.submit_limit_with(
            1_500,
            Side::Bid,
            65_535,
            1_000,
            TimeInForce::FillOrKill,
            sink,
        )
    });
    let report = result?;
    require!(report.fills == 1_000);
    require!(report.traded_quantity == 1_000);
    require!(fills.len() == 1_000);
    for (index, fill) in fills.iter().enumerate() {
        require!(fill.maker_order_id == index as u32 + 1, index);
        require!(fill.price == (30_000 + index as u32 * 31) as u16, index);
        require!(fill.quantity == 1, index);
    }
    require!(book.live_orders() == 0);
    require!(book.validate());
    require_queue(&book, Side::Ask, 30_000, &[])?;
    Ok(())
}
