//! Public API surface that the ported C++ groups do not reach.
//!
//! The C++ suite has no equivalent of this group: these are Rust-specific
//! surfaces (trait impls, `Default`) plus `top_checksum`, which exists for the
//! benchmark and would otherwise ship with no test at all.

use super::{CheckResult, Workload, require};
use crate::{
    ConfigurationError, ExecutionReport, HierarchicalBitmap, L2Book, NO_ASK, NO_BID, OrderError,
    Side, SweepResult, TimeInForce,
};

const ALL_ERRORS: [OrderError; 10] = [
    OrderError::QuantityZero,
    OrderError::OrderIdOutOfRange,
    OrderError::DuplicateOrderId,
    OrderError::UnknownOrderId,
    OrderError::CapacityExceeded,
    OrderError::WouldCross,
    OrderError::InsufficientLiquidity,
    OrderError::QuantityIncreaseNotAllowed,
    OrderError::ReplacementIdMustDiffer,
    OrderError::UnsupportedTimeInForce,
];

pub fn public_api_surface(_workload: Workload) -> CheckResult {
    // Every rejection reason must render as its own non-empty message: these
    // strings are what a gateway would log, so duplicates would be misleading.
    let mut messages = Vec::new();
    for error in ALL_ERRORS {
        let message = error.to_string();
        require!(!message.is_empty(), format!("{error:?}"));
        messages.push(message);
    }
    messages.sort_unstable();
    let distinct = messages.len();
    messages.dedup();
    require!(messages.len() == distinct);
    require!(!ConfigurationError.to_string().is_empty());

    // `Default` must agree with `new` rather than deriving a zeroed state.
    require!(HierarchicalBitmap::default().is_empty());
    require!(L2Book::default().best_bid() == NO_BID);
    require!(L2Book::default().best_ask() == NO_ASK);

    // Average price is defined as zero when nothing traded, so callers do not
    // have to guard against a division by zero.
    require!(SweepResult::default().average_price() == 0.0);
    require!(ExecutionReport::default().average_price() == 0.0);

    let sweep = SweepResult {
        requested_quantity: 10,
        filled_quantity: 4,
        notional_ticks: 404,
        levels_visited: 2,
    };
    require!(sweep.average_price() == 101.0);

    let report = ExecutionReport {
        fills: 2,
        traded_quantity: 5,
        notional_ticks: 500,
        ..ExecutionReport::default()
    };
    require!(report.average_price() == 100.0);

    // `top_checksum` must depend on price, quantity, and rank, or it would not
    // detect a reordered book.
    let mut book = L2Book::new();
    book.set_level(Side::Bid, 100, 5);
    book.set_level(Side::Bid, 99, 7);
    let baseline = book.top_checksum(Side::Bid, 10);
    require!(baseline != 0);
    require!(book.top_checksum(Side::Bid, 10) == baseline);
    require!(book.top_checksum(Side::Bid, 1) != baseline);
    require!(book.top_checksum(Side::Ask, 10) != baseline);

    book.set_level(Side::Bid, 99, 8);
    require!(book.top_checksum(Side::Bid, 10) != baseline);

    // Swapping the two quantities between the two prices must change the
    // checksum, proving rank is mixed in rather than the multiset of levels.
    let mut swapped = L2Book::new();
    swapped.set_level(Side::Bid, 100, 7);
    swapped.set_level(Side::Bid, 99, 5);
    require!(swapped.top_checksum(Side::Bid, 10) != baseline);

    require!(Side::Bid.opposite() == Side::Ask);
    require!(Side::Ask.opposite() == Side::Bid);
    require!(Side::Bid.index() == 0);
    require!(Side::Ask.index() == 1);
    require!(TimeInForce::GoodTillCancel != TimeInForce::PostOnly);
    Ok(())
}
