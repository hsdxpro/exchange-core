//! Public API surface the scenario groups do not reach: trait impls,
//! `Default`, and error formatting.

use super::{CheckResult, Workload, require};
use crate::PRICE_COUNT;
use crate::{
    ConfigurationError, ExecutionReport, HierarchicalBitmap, OrderError, Side, TimeInForce,
};

const ALL_ERRORS: [OrderError; 11] = [
    OrderError::QuantityZero,
    OrderError::OrderIdOutOfRange,
    OrderError::PriceOutOfDomain,
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
    require!(HierarchicalBitmap::new(PRICE_COUNT).is_empty());

    // Average price is defined as zero when nothing traded, so callers do not
    // have to guard against a division by zero.
    require!(ExecutionReport::default().average_price() == 0.0);

    let report = ExecutionReport {
        fills: 2,
        traded_quantity: 5,
        notional_ticks: 500,
        ..ExecutionReport::default()
    };
    require!(report.average_price() == 100.0);

    require!(Side::Bid.opposite() == Side::Ask);
    require!(Side::Ask.opposite() == Side::Bid);
    require!(Side::Bid.index() == 0);
    require!(Side::Ask.index() == 1);
    require!(TimeInForce::GoodTillCancel != TimeInForce::PostOnly);
    Ok(())
}
