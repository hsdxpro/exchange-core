//! Randomized and exhaustive differential verification against the reference
//! models, plus deterministic replay reproducibility.

use super::reference::ReferenceL3;
use super::{CheckResult, Rng, Workload, require};
use crate::{Fill, L3Book, PRICE_COUNT, Side, TimeInForce, mix64};

fn random_quantity(random: u64) -> u32 {
    match (random >> 48) & 31 {
        0 => 0,
        1 => u32::MAX,
        _ => 1 + ((random >> 24) % 10_000) as u32,
    }
}

fn random_time_in_force(random: u64) -> TimeInForce {
    match (random >> 56) & 3 {
        0 => TimeInForce::GoodTillCancel,
        1 => TimeInForce::ImmediateOrCancel,
        2 => TimeInForce::FillOrKill,
        _ => TimeInForce::PostOnly,
    }
}

/// Compares engine and model. The deep check walks every live level and queue,
/// so it is run periodically rather than on every operation.
fn require_equivalent(
    fast: &L3Book,
    reference: &ReferenceL3,
    operation: usize,
    deep_check: bool,
) -> CheckResult {
    require!(fast.best_bid() == reference.best_bid(), operation);
    require!(fast.best_ask() == reference.best_ask(), operation);
    require!(fast.live_orders() == reference.live_orders(), operation);
    if !deep_check {
        return Ok(());
    }

    require!(fast.validate(), operation);
    require!(fast.state_hash() == reference.state_hash(), operation);

    for side in [Side::Bid, Side::Ask] {
        let mut levels = Vec::new();
        fast.for_each_level(side, PRICE_COUNT, |price, quantity| {
            levels.push((price, quantity));
        });
        for (price, quantity) in levels {
            require!(reference.level_quantity(side, price) == quantity, operation);
            let mut fast_queue = Vec::new();
            fast.for_each_order_at_level(side, price, |order| fast_queue.push(order));
            require!(
                fast_queue == reference.orders_at_level(side, price),
                operation
            );
        }
    }
    Ok(())
}

fn randomized_model_for_seed(
    seed: u64,
    capacity: usize,
    order_id_capacity: usize,
    operation_count: usize,
) -> CheckResult {
    let mut fast = L3Book::new(capacity, order_id_capacity);
    let mut reference = ReferenceL3::new(capacity, order_id_capacity);
    let mut rng = Rng::new(seed);
    let id_range = (order_id_capacity + 8) as u64;

    for operation in 0..operation_count {
        let random = rng.next_u64();
        let action = random % 100;
        let id = ((random >> 8) % id_range) as u32;
        let side = if (random >> 20) & 1 == 0 {
            Side::Bid
        } else {
            Side::Ask
        };
        let price = (random >> 24) as u16;
        let quantity = random_quantity(random);

        if action < 22 {
            require!(
                fast.add_passive(id, side, price, quantity)
                    == reference.add_passive(id, side, price, quantity),
                operation
            );
        } else if action < 37 {
            require!(fast.cancel(id) == reference.cancel(id), operation);
        } else if action < 52 {
            let new_quantity = match reference.order(id) {
                Some(current) => match (random >> 52) & 3 {
                    0 => 0,
                    1 => current.quantity,
                    2 => {
                        if current.quantity == 1 {
                            1
                        } else {
                            current.quantity / 2
                        }
                    }
                    _ => current.quantity.saturating_add(1),
                },
                None => quantity,
            };
            require!(
                fast.amend_down(id, new_quantity) == reference.amend_down(id, new_quantity),
                operation
            );
        } else if action < 67 {
            let replacement_id = ((random >> 40) % id_range) as u32;
            let mut fast_fills = Vec::new();
            let mut reference_fills = Vec::new();
            let left = fast.cancel_replace_with(id, replacement_id, price, quantity, |fill| {
                fast_fills.push(fill);
            });
            let right =
                reference.cancel_replace_with(id, replacement_id, price, quantity, |fill| {
                    reference_fills.push(fill);
                });
            require!(left == right, operation);
            require!(fast_fills == reference_fills, operation);
        } else {
            let time_in_force = random_time_in_force(random);
            let mut fast_fills: Vec<Fill> = Vec::new();
            let mut reference_fills: Vec<Fill> = Vec::new();
            let left = fast.submit_limit_with(id, side, price, quantity, time_in_force, |fill| {
                fast_fills.push(fill);
            });
            let right =
                reference.submit_limit_with(id, side, price, quantity, time_in_force, |fill| {
                    reference_fills.push(fill);
                });
            require!(left == right, operation);
            require!(fast_fills == reference_fills, operation);
        }

        require_equivalent(&fast, &reference, operation, operation & 511 == 511)?;
    }
    require_equivalent(&fast, &reference, operation_count, true)
}

pub fn randomized_differential_large_capacity(workload: Workload) -> CheckResult {
    const SEEDS: [u64; 4] = [
        0x0ff1_ce42_aa51_9911,
        0x6f02_a911_34c8_d77b,
        0x94d0_49bb_1331_11eb,
        0xc001_d00d_5eed_1234,
    ];
    for seed in SEEDS {
        randomized_model_for_seed(seed, 4_096, 8_192, workload.scale(250_000))?;
    }
    Ok(())
}

pub fn randomized_differential_capacity_pressure(workload: Workload) -> CheckResult {
    const CONFIGURATIONS: [(usize, usize); 5] = [(0, 8), (1, 8), (2, 16), (7, 32), (31, 128)];
    let mut seed = 0x5a17_cafe_1234_9001;
    for (capacity, id_capacity) in CONFIGURATIONS {
        randomized_model_for_seed(seed, capacity, id_capacity, workload.scale(120_000))?;
        seed = mix64(seed);
    }
    Ok(())
}

pub fn exhaustive_fifo_quantities_and_tif(_workload: Workload) -> CheckResult {
    const TIME_IN_FORCE: [TimeInForce; 4] = [
        TimeInForce::GoodTillCancel,
        TimeInForce::ImmediateOrCancel,
        TimeInForce::FillOrKill,
        TimeInForce::PostOnly,
    ];

    for first in 1..=3_u32 {
        for second in 1..=3_u32 {
            for third in 1..=3_u32 {
                let total = first + second + third;
                for taker_quantity in 1..=total + 2 {
                    for time_in_force in TIME_IN_FORCE {
                        let mut fast = L3Book::new(8, 16);
                        let mut reference = ReferenceL3::new(8, 16);
                        for (id, quantity) in [(1_u32, first), (2, second), (3, third)] {
                            require!(
                                fast.add_passive(id, Side::Ask, 100, quantity)
                                    == reference.add_passive(id, Side::Ask, 100, quantity)
                            );
                        }

                        let mut fast_fills = Vec::new();
                        let mut reference_fills = Vec::new();
                        let left = fast.submit_limit_with(
                            10,
                            Side::Bid,
                            100,
                            taker_quantity,
                            time_in_force,
                            |fill| fast_fills.push(fill),
                        );
                        let right = reference.submit_limit_with(
                            10,
                            Side::Bid,
                            100,
                            taker_quantity,
                            time_in_force,
                            |fill| reference_fills.push(fill),
                        );
                        require!(left == right, taker_quantity);
                        require!(fast_fills == reference_fills, taker_quantity);
                        require!(fast.validate(), taker_quantity);
                        require!(fast.state_hash() == reference.state_hash(), taker_quantity);
                    }
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReplaySummary {
    command_hash: u64,
    state_hash: u64,
    live_orders: u32,
}

fn deterministic_replay(
    seed: u64,
    operation_count: usize,
) -> Result<ReplaySummary, super::Failure> {
    let mut book = L3Book::new(256, 1_024);
    let mut rng = Rng::new(seed);
    let mut command_hash = 0_u64;

    for operation in 0..operation_count {
        let random = rng.next_u64();
        let id = ((random >> 8) % 1_024) as u32;
        let side = if (random >> 20) & 1 == 0 {
            Side::Bid
        } else {
            Side::Ask
        };
        let price = (random >> 24) as u16;
        let quantity = 1 + ((random >> 40) % 100) as u32;

        let result_hash = match random % 4 {
            0 => reject_code(book.add_passive(id, side, price, quantity)),
            1 => reject_code(book.cancel(id)),
            2 => reject_code(book.amend_down(id, quantity)),
            _ => match book.submit_limit(id, side, price, quantity, TimeInForce::ImmediateOrCancel)
            {
                Ok(report) => {
                    report.report_hash
                        ^ report.traded_quantity
                        ^ u64::from(report.canceled_quantity)
                }
                Err(error) => (error as u64 + 1) << 56,
            },
        };
        command_hash = mix64(command_hash ^ random ^ result_hash ^ operation as u64);
    }

    require!(book.validate());
    Ok(ReplaySummary {
        command_hash,
        state_hash: book.state_hash(),
        live_orders: book.live_orders(),
    })
}

fn reject_code(result: Result<(), crate::OrderError>) -> u64 {
    match result {
        Ok(()) => 0,
        Err(error) => error as u64 + 1,
    }
}

/// Golden summary of the full 100,000-command replay.
///
/// Re-running the same seed twice only proves the engine is deterministic,
/// which safe Rust makes close to tautological. Pinning the result turns this
/// into a regression detector: any unintended change to matching, rejection
/// codes, or book state moves these numbers. Update them only alongside a
/// deliberate, reviewed behavior change.
const REPLAY_GOLDEN: ReplaySummary = ReplaySummary {
    command_hash: 0x2a51_914c_9685_e43e,
    state_hash: 0xfbcc_a825_7a5a_5188,
    live_orders: 201,
};

pub fn deterministic_replay_is_reproducible(workload: Workload) -> CheckResult {
    let operation_count = workload.scale(100_000);
    let first = deterministic_replay(0x7af3_0021_9124_beef, operation_count)?;
    let second = deterministic_replay(0x7af3_0021_9124_beef, operation_count)?;
    require!(first == second);
    require!(first.live_orders <= 256);

    // The golden values describe the full-length stream only.
    if workload == Workload::Full {
        require!(first == REPLAY_GOLDEN, format!("{first:?}"));
    }
    Ok(())
}
