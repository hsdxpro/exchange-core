//! Executable verification suite for the engine.
//!
//! Every named check in [`CHECKS`] is a port of the corresponding C++23 test
//! group. The suite is compiled into the library (not behind `cfg(test)`) so
//! the shipped binary can verify itself before reporting any benchmark number,
//! and so `cargo test` and the binary run byte-for-byte the same checks.

pub mod api;
pub mod bitmap_l2;
pub mod differential;
pub mod l3;
pub mod reference;

use crate::OrderError;
use core::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Carries the source location and the expression that failed, mirroring the
/// `BX_REQUIRE` macro on the C++ side.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Failure(pub String);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<&str> for Failure {
    fn from(detail: &str) -> Self {
        Self(detail.to_owned())
    }
}

/// Lets a check use `?` on an engine call it expects to succeed.
impl From<OrderError> for Failure {
    fn from(error: OrderError) -> Self {
        Self(format!("unexpected rejection: {error}"))
    }
}

pub type CheckResult = Result<(), Failure>;

/// Randomized checks are sized by workload so that a debug `cargo test` and an
/// interactive `--quick` run stay fast without changing what is being checked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Workload {
    Quick,
    Full,
}

impl Workload {
    /// Scales a full-run operation count for the selected workload.
    #[must_use]
    pub const fn scale(self, full_count: usize) -> usize {
        match self {
            Self::Full => full_count,
            Self::Quick => {
                let reduced = full_count / 50;
                if reduced == 0 { 1 } else { reduced }
            }
        }
    }
}

macro_rules! require {
    ($condition:expr) => {
        if !$condition {
            return Err($crate::verify::Failure(format!(
                "{}:{}: requirement failed: {}",
                file!(),
                line!(),
                stringify!($condition)
            )));
        }
    };
    ($condition:expr, $detail:expr) => {
        if !$condition {
            return Err($crate::verify::Failure(format!(
                "{}:{}: requirement failed: {} (at {})",
                file!(),
                line!(),
                stringify!($condition),
                $detail
            )));
        }
    };
}

pub(crate) use require;

/// Deterministic SplitMix64, identical to the C++ test generator so both
/// implementations replay the exact same command streams.
#[derive(Clone, Copy, Debug)]
pub struct Rng {
    state: u64,
}

impl Rng {
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        crate::mix64(self.state)
    }
}

/// Builds exactly `count` prices spread across `[low, high]` so traversal never
/// benefits from dense, adjacent occupancy. Shared with the benchmark so both
/// measure and verify the same sparse shape.
///
/// # Panics
///
/// Panics if `[low, high]` cannot supply `count` distinct prices.
#[must_use]
pub fn sparse_prices(low: u16, high: u16, count: usize) -> Vec<u16> {
    let span = u32::from(high) - u32::from(low);
    let mut prices: Vec<u16> = (0..count)
        .map(|index| {
            let base = u32::from(low) + ((index as u64 * u64::from(span)) / count as u64) as u32;
            (base + (index % 3) as u32) as u16
        })
        .collect();
    prices.sort_unstable();
    prices.dedup();

    let mut candidate = u32::from(low);
    while prices.len() < count && candidate <= u32::from(high) {
        let price = candidate as u16;
        if prices.binary_search(&price).is_err() {
            prices.push(price);
            prices.sort_unstable();
        }
        candidate += 1;
    }
    assert_eq!(
        prices.len(),
        count,
        "price range cannot supply {count} prices"
    );
    prices
}

#[derive(Clone, Copy, Debug)]
pub struct Check {
    pub group: &'static str,
    pub name: &'static str,
    pub run: fn(Workload) -> CheckResult,
}

#[derive(Clone, Debug)]
pub struct Outcome {
    pub group: &'static str,
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
    pub elapsed: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct Summary {
    pub outcomes: Vec<Outcome>,
    pub elapsed: Duration,
}

impl Summary {
    #[must_use]
    pub fn passed(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|outcome| outcome.passed)
            .count()
    }

    #[must_use]
    pub fn failed(&self) -> usize {
        self.outcomes.len() - self.passed()
    }

    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.failed() == 0
    }
}

static PANIC_DETAIL: Mutex<Option<String>> = Mutex::new(None);

fn install_capturing_panic_hook() -> Box<dyn Fn(&panic::PanicHookInfo<'_>) + Sync + Send + 'static>
{
    let previous = panic::take_hook();
    panic::set_hook(Box::new(|info| {
        let location = info
            .location()
            .map_or_else(|| "unknown location".to_owned(), ToString::to_string);
        let payload = info
            .payload()
            .downcast_ref::<&str>()
            .map(|text| (*text).to_owned())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "non-string panic payload".to_owned());
        if let Ok(mut slot) = PANIC_DETAIL.lock() {
            *slot = Some(format!("{location}: panicked: {payload}"));
        }
    }));
    previous
}

/// Runs a single check, converting an unexpected panic into a failed outcome
/// so one broken invariant cannot take down the whole report.
fn run_check(check: &Check, workload: Workload) -> Outcome {
    if let Ok(mut slot) = PANIC_DETAIL.lock() {
        *slot = None;
    }
    let started = Instant::now();
    let result = panic::catch_unwind(AssertUnwindSafe(|| (check.run)(workload)));
    let elapsed = started.elapsed();

    let (passed, detail) = match result {
        Ok(Ok(())) => (true, String::new()),
        Ok(Err(failure)) => (false, failure.0),
        Err(_) => {
            let detail = PANIC_DETAIL
                .lock()
                .ok()
                .and_then(|slot| slot.clone())
                .unwrap_or_else(|| "panicked".to_owned());
            (false, detail)
        }
    };

    Outcome {
        group: check.group,
        name: check.name,
        passed,
        detail,
        elapsed,
    }
}

/// Runs the whole suite, reporting each outcome as it completes.
pub fn run<F>(workload: Workload, mut on_outcome: F) -> Summary
where
    F: FnMut(&Outcome),
{
    let previous_hook = install_capturing_panic_hook();
    let started = Instant::now();
    let mut summary = Summary::default();

    for check in CHECKS {
        let outcome = run_check(check, workload);
        on_outcome(&outcome);
        summary.outcomes.push(outcome);
    }

    summary.elapsed = started.elapsed();
    panic::set_hook(previous_hook);
    summary
}

macro_rules! register_checks {
    ($($module:ident :: $function:ident, $group:literal, $name:literal;)*) => {
        /// Every named check, in execution order.
        pub const CHECKS: &[Check] = &[
            $(Check { group: $group, name: $name, run: $module::$function },)*
        ];

        #[cfg(test)]
        mod suite {
            use super::{Workload, CHECKS};

            fn workload() -> Workload {
                if cfg!(debug_assertions) { Workload::Quick } else { Workload::Full }
            }

            $(
                #[test]
                fn $function() {
                    if let Err(detail) = super::$module::$function(workload()) {
                        panic!("{detail}");
                    }
                }
            )*

            #[test]
            fn every_check_is_registered_once() {
                let mut names: Vec<&str> = CHECKS.iter().map(|check| check.name).collect();
                names.sort_unstable();
                let total = names.len();
                names.dedup();
                assert_eq!(names.len(), total, "duplicate check name registered");
                assert_eq!(
                    total, 43,
                    "expected the 42 ported C++ groups plus the Rust-only API-surface group"
                );
            }
        }
    };
}

register_checks! {
    bitmap_l2::bitmap_empty_and_idempotence, "bitmap/L2", "bitmap empty state and idempotence";
    bitmap_l2::bitmap_all_singletons, "bitmap/L2", "bitmap every singleton price";
    bitmap_l2::bitmap_hierarchy_boundaries, "bitmap/L2", "bitmap word and hierarchy boundaries";
    bitmap_l2::bitmap_full_domain_and_queries, "bitmap/L2", "bitmap full-domain next/previous";
    bitmap_l2::bitmap_random_differential, "bitmap/L2", "bitmap randomized differential model";
    bitmap_l2::l2_empty_boundaries_and_totals, "bitmap/L2", "L2 empty, boundary prices, and 64-bit totals";
    bitmap_l2::l2_top_order_and_limit, "bitmap/L2", "L2 top-N ordering and limits";
    bitmap_l2::l2_sweep_zero_partial_exact_and_shortfall, "bitmap/L2", "L2 sweep zero, partial, exact, and shortfall";
    bitmap_l2::l2_sparse_top_1000, "bitmap/L2", "L2 sparse top-1000 traversal and VWAP";
    bitmap_l2::l2_random_differential, "bitmap/L2", "L2 randomized differential model";

    l3::compact_layout_and_empty_state, "L3", "compact L3 layout and empty state";
    l3::invalid_capacity_is_rejected_before_allocation, "L3", "invalid slot capacity rejected before allocation";
    l3::zero_capacity_and_empty_book_time_in_force, "L3", "zero-capacity and empty-book TIF behavior";
    l3::passive_add_validation_and_cross_prevention, "L3", "passive add validation and crossed-book prevention";
    l3::boundary_prices_and_best_updates, "L3", "boundary prices and best-price updates";
    l3::strict_fifo_and_maker_price, "L3", "strict FIFO and maker-price execution";
    l3::panicking_fill_callback_preserves_book_invariants, "L3", "panicking fill callback preserves invariants";
    l3::price_priority_and_multi_level_matching, "L3", "price priority and multi-level matching";
    l3::limit_price_stops_matching_and_fok_preview, "L3", "limit-price boundary and FOK preview";
    l3::sell_side_symmetry, "L3", "sell-side matching symmetry";
    l3::market_sell_side_symmetry, "L3", "market sell-side symmetry";
    l3::gtc_rests_and_crosses_then_rests, "L3", "GTC passive and match-then-rest behavior";
    l3::ioc_semantics, "L3", "IOC cancellation semantics";
    l3::fok_is_atomic, "L3", "FOK atomicity";
    l3::post_only_is_atomic, "L3", "post-only atomicity";
    l3::cancel_head_middle_tail_and_only_order, "L3", "cancel head, middle, tail, and only order";
    l3::amend_down_preserves_priority, "L3", "amend down preserves queue priority";
    l3::cancel_replace_gets_new_priority, "L3", "cancel/replace assigns a new ID and new priority";
    l3::cancel_replace_crosses_then_rests, "L3", "cancel/replace crosses then rests the remainder";
    l3::invalid_cancel_replace_is_atomic, "L3", "invalid cancel/replace leaves the original untouched";
    l3::slot_reuse_and_free_list_integrity, "L3", "slot reuse and free-list integrity";
    l3::full_capacity_aggressive_orders, "L3", "full-capacity aggressive GTC and IOC";
    l3::incoming_validation_is_atomic, "L3", "incoming-order rejection is atomic";
    l3::market_ioc_and_fok, "L3", "market IOC and FOK";
    l3::maximum_quantity_and_notional_are_safe, "L3", "maximum quantity and notional safety";
    l3::aggregate_quantity_uses_64_bits, "L3", "64-bit aggregate level quantity";
    l3::level_iteration_and_order_lookup, "L3", "level iteration and direct order lookup";
    l3::sparse_1000_level_sweep, "L3", "sparse 1000-level matching sweep";

    differential::randomized_differential_large_capacity, "differential", "randomized differential model: large capacity";
    differential::randomized_differential_capacity_pressure, "differential", "randomized differential model: capacity pressure";
    differential::exhaustive_fifo_quantities_and_tif, "differential", "exhaustive FIFO quantities across all TIF modes";
    differential::deterministic_replay_is_reproducible, "differential", "deterministic replay reproducibility";

    api::public_api_surface, "API surface", "public API surface and error formatting";
}
