use bitmap_exchange::verify::{self, Rng, Workload, sparse_prices};
use bitmap_exchange::{
    ExecutionReport, L2Book, L3Book, OrderError, OrderSlot, PriceLevel, Side, TimeInForce, mix64,
};
use std::cmp::min;
use std::hint::black_box;
use std::mem::size_of;
use std::process::ExitCode;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
struct Stats {
    p50: f64,
    p99: f64,
    p999: f64,
}

#[derive(Clone, Copy, Debug)]
struct BenchResult {
    scenario: &'static str,
    /// Plain-English statement of the work one operation actually performs.
    does: &'static str,
    /// The thing one unit of work produces, e.g. `fill` in `ns/fill`.
    unit: &'static str,
    stats: Stats,
    work_per_operation: f64,
    samples: usize,
}

#[derive(Clone, Copy, Debug)]
struct L2Update {
    side: Side,
    price: u16,
    quantity: u32,
}

#[derive(Clone, Copy, Debug)]
struct RestingOrder {
    id: u32,
    side: Side,
    price: u16,
    quantity: u32,
}

#[derive(Clone, Copy, Debug)]
enum CommandType {
    Add,
    Cancel,
    Amend,
    Replace,
    Aggressive,
}

#[derive(Clone, Copy, Debug)]
struct Command {
    command_type: CommandType,
    id: u32,
    replacement_id: u32,
    side: Side,
    price: u16,
    quantity: u32,
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted[index]
}

fn summarize(mut samples: Vec<f64>) -> Stats {
    samples.sort_unstable_by(f64::total_cmp);
    Stats {
        p50: percentile(&samples, 0.50),
        p99: percentile(&samples, 0.99),
        p999: percentile(&samples, 0.999),
    }
}

fn measure_batches<State, Setup, Work, Finish>(
    operation_count: usize,
    batch_size: usize,
    runs: usize,
    mut setup: Setup,
    mut work: Work,
    mut finish: Finish,
    sink: &mut u64,
) -> (Stats, usize)
where
    Setup: FnMut() -> State,
    Work: FnMut(&mut State, usize, usize) -> u64,
    Finish: FnMut(&State) -> u64,
{
    let mut samples = Vec::with_capacity(operation_count.div_ceil(batch_size) * runs);
    for _ in 0..runs {
        let mut state = setup();
        let mut begin = 0;
        while begin < operation_count {
            let end = min(begin + batch_size, operation_count);
            let started = Instant::now();
            let local_hash = work(&mut state, begin, end);
            let elapsed_ns = started.elapsed().as_secs_f64() * 1_000_000_000.0;
            black_box(local_hash);
            *sink = mix64(*sink ^ local_hash);
            samples.push(elapsed_ns / (end - begin) as f64);
            begin = end;
        }
        *sink = mix64(*sink ^ finish(&state));
    }
    let sample_count = samples.len();
    (summarize(samples), sample_count)
}

fn make_l2_updates(count: usize) -> Vec<L2Update> {
    let bids = sparse_prices(512, 32_000, 2_048);
    let asks = sparse_prices(33_536, 65_000, 2_048);
    let mut rng = Rng::new(0x3f27_5c81_117a_902d);
    let mut updates = Vec::with_capacity(count);
    for _ in 0..count {
        let random = rng.next_u64();
        let side = if random & 1 == 0 {
            Side::Bid
        } else {
            Side::Ask
        };
        let prices = if side == Side::Bid { &bids } else { &asks };
        let price = prices[((random >> 8) as usize) % prices.len()];
        let quantity = if (random >> 32).is_multiple_of(11) {
            0
        } else {
            1 + ((random >> 40) % 10_000) as u32
        };
        updates.push(L2Update {
            side,
            price,
            quantity,
        });
    }
    updates
}

fn make_sparse_l2() -> L2Book {
    let mut book = L2Book::new();
    let bids = sparse_prices(512, 32_000, 1_000);
    let asks = sparse_prices(33_536, 65_000, 1_000);
    for index in 0..1_000 {
        book.set_level(Side::Bid, bids[index], 100 + (index % 100) as u32);
        book.set_level(Side::Ask, asks[index], 100 + ((index * 7) % 100) as u32);
    }
    book
}

fn make_resting_orders(count: usize) -> Vec<RestingOrder> {
    let bids = sparse_prices(8_192, 32_000, 2_048);
    let asks = sparse_prices(33_536, 57_344, 2_048);
    let mut orders = Vec::with_capacity(count);
    for index in 0..count {
        let side = if index & 1 == 0 { Side::Bid } else { Side::Ask };
        let prices = if side == Side::Bid { &bids } else { &asks };
        orders.push(RestingOrder {
            id: index as u32 + 1,
            side,
            price: prices[(index.wrapping_mul(2_654_435_761)) % prices.len()],
            quantity: 1 + (index % 1_000) as u32,
        });
    }
    orders
}

fn seeded_l3_book(orders: &[RestingOrder]) -> L3Book {
    let mut book = L3Book::new(orders.len() + 1, orders.len() * 2 + 8);
    for order in orders {
        book.add_passive(order.id, order.side, order.price, order.quantity)
            .expect("benchmark seed book must be valid");
    }
    book
}

fn result_hash(result: Result<ExecutionReport, OrderError>) -> u64 {
    match result {
        Ok(report) => {
            report.report_hash
                ^ report.traded_quantity
                ^ (u64::from(report.rested_quantity) << 32)
                ^ u64::from(report.canceled_quantity)
        }
        Err(error) => 1_u64 << (error as u8),
    }
}

fn make_mixed_commands(count: usize) -> Vec<Command> {
    let mut model = L3Book::new(count + 4_096, count * 2 + 8_192);
    let mut commands = Vec::with_capacity(count);
    let mut created = Vec::with_capacity(count);
    let mut side_by_id = vec![Side::Bid; count * 2 + 8_192];
    let mut rng = Rng::new(0x3db1_0914_a2fc_8557);
    let mut next_id = 1_u32;

    while commands.len() < count {
        let random = rng.next_u64();
        let mut action = (random % 100) as u32;
        if created.is_empty() {
            action = 0;
        }

        let choose_live = |model: &L3Book, created: &[u32], random: u64| -> Option<u32> {
            if created.is_empty() {
                return None;
            }
            for attempt in 0..16 {
                let index = ((random.rotate_left((attempt * 3) as u32)) as usize) % created.len();
                let id = created[index];
                if model.contains(id) {
                    return Some(id);
                }
            }
            None
        };

        if action < 50 {
            let side = if (random >> 8) & 1 == 0 {
                Side::Bid
            } else {
                Side::Ask
            };
            let price = match side {
                Side::Bid => 32_000 - ((random >> 16) & 2_047) as u16,
                Side::Ask => 33_536 + ((random >> 16) & 2_047) as u16,
            };
            let quantity = 1 + ((random >> 32) % 100) as u32;
            let id = next_id;
            next_id += 1;
            if model.add_passive(id, side, price, quantity).is_ok() {
                commands.push(Command {
                    command_type: CommandType::Add,
                    id,
                    replacement_id: 0,
                    side,
                    price,
                    quantity,
                });
                created.push(id);
                side_by_id[id as usize] = side;
            }
        } else if action < 70 {
            if let Some(id) = choose_live(&model, &created, random) {
                model
                    .cancel(id)
                    .expect("generated cancel must target a live order");
                commands.push(Command {
                    command_type: CommandType::Cancel,
                    id,
                    replacement_id: 0,
                    side: side_by_id[id as usize],
                    price: 0,
                    quantity: 0,
                });
            }
        } else if action < 80 {
            if let Some(id) = choose_live(&model, &created, random) {
                let current = model
                    .order(id)
                    .expect("generated reduction must target a live order");
                let quantity = (current.quantity / 2).max(1);
                model
                    .amend_down(id, quantity)
                    .expect("generated reduction must be valid");
                commands.push(Command {
                    command_type: CommandType::Amend,
                    id,
                    replacement_id: 0,
                    side: current.side,
                    price: 0,
                    quantity,
                });
            }
        } else if action < 90 {
            if let Some(id) = choose_live(&model, &created, random) {
                let side = side_by_id[id as usize];
                let price = match side {
                    Side::Bid => 32_000 - ((random >> 20) & 2_047) as u16,
                    Side::Ask => 33_536 + ((random >> 20) & 2_047) as u16,
                };
                let quantity = 1 + ((random >> 40) % 200) as u32;
                let replacement_id = next_id;
                next_id += 1;
                if model
                    .cancel_replace(id, replacement_id, price, quantity)
                    .is_ok()
                {
                    commands.push(Command {
                        command_type: CommandType::Replace,
                        id,
                        replacement_id,
                        side,
                        price,
                        quantity,
                    });
                    created.push(replacement_id);
                    side_by_id[replacement_id as usize] = side;
                }
            }
        } else {
            let side = if (random >> 9) & 1 == 0 {
                Side::Bid
            } else {
                Side::Ask
            };
            let price = if side == Side::Bid { u16::MAX } else { 0 };
            let quantity = 1 + ((random >> 32) % 150) as u32;
            let id = next_id;
            next_id += 1;
            let _ = model.submit_limit(id, side, price, quantity, TimeInForce::ImmediateOrCancel);
            commands.push(Command {
                command_type: CommandType::Aggressive,
                id,
                replacement_id: 0,
                side,
                price,
                quantity,
            });
        }
    }
    commands
}

const RULE_WIDTH: usize = 92;

/// Widths chosen so the widest possible cell still fits: a `"1234.56 us"`
/// reading plus the longest unit noun (`"/message"`).
const SCENARIO_WIDTH: usize = 40;
const LATENCY_WIDTH: usize = 11;
const PER_ITEM_WIDTH: usize = 19;

fn print_header(cpu: Option<usize>, runs: usize) {
    println!("\nBitmap Exchange — Rust 1.97.1 / Edition 2024");
    println!(
        "Single-instrument L2/L3 order book and price-time FIFO matching engine over a\n\
         65,536-tick ladder with a 3-tier occupancy bitmap. OrderSlot={} B, PriceLevel={} B,\n\
         no allocation after construction, no third-party crates, #![forbid(unsafe_code)].",
        size_of::<OrderSlot>(),
        size_of::<PriceLevel>()
    );
    match cpu {
        Some(core) => println!("Benchmark runs: {runs} | pinned to Linux CPU {core}"),
        None => println!("Benchmark runs: {runs} | CPU pinning unavailable on this platform"),
    }
}

/// Runs the verification suite first and prints one line per named check.
/// Benchmark numbers from an engine that fails a check are meaningless, so a
/// failure stops the run.
fn run_verification(workload: Workload) -> bool {
    println!("\nSTEP 1 — Verification: {} checks", verify::CHECKS.len());
    println!(
        "Deterministic scenarios plus randomized differential comparison against independent\n\
         BTreeMap/FIFO reference models. Workload: {}.",
        match workload {
            Workload::Full => "full",
            Workload::Quick => "quick (reduced randomized operation counts)",
        }
    );
    println!("{}", "-".repeat(RULE_WIDTH));

    let mut current_group = "";
    let summary = verify::run(workload, |outcome| {
        if outcome.group != current_group {
            current_group = outcome.group;
            println!("  [{current_group}]");
        }
        println!(
            "  {}  {:<52}{:>8.1} ms",
            if outcome.passed { "PASS" } else { "FAIL" },
            outcome.name,
            outcome.elapsed.as_secs_f64() * 1_000.0,
        );
        if !outcome.passed {
            println!("        {}", outcome.detail);
        }
    });

    println!("{}", "-".repeat(RULE_WIDTH));
    println!(
        "  {} passed, {} failed  ({:.2} s)",
        summary.passed(),
        summary.failed(),
        summary.elapsed.as_secs_f64(),
    );
    summary.is_ok()
}

fn print_section(title: &str, results: &[BenchResult]) {
    println!("\n{title}");
    println!("{}", "-".repeat(RULE_WIDTH));
    println!(
        "{:<SCENARIO_WIDTH$}{:>LATENCY_WIDTH$}{:>LATENCY_WIDTH$}{:>LATENCY_WIDTH$}{:>PER_ITEM_WIDTH$}",
        "Scenario", "p50", "p99", "p99.9", "per item"
    );
    println!("{}", "-".repeat(RULE_WIDTH));
    for result in results {
        println!(
            "{:<SCENARIO_WIDTH$}{:>LATENCY_WIDTH$}{:>LATENCY_WIDTH$}{:>LATENCY_WIDTH$}{:>PER_ITEM_WIDTH$}",
            result.scenario,
            format_ns(result.stats.p50),
            format_ns(result.stats.p99),
            format_ns(result.stats.p999),
            format!(
                "{}/{}",
                format_ns(result.stats.p50 / result.work_per_operation),
                result.unit
            ),
        );
        println!("    {} ({} samples)", result.does, result.samples);
    }
}

/// Keeps nanosecond and microsecond scales readable in the same column.
fn format_ns(nanoseconds: f64) -> String {
    if nanoseconds >= 1_000.0 {
        format!("{:.2} us", nanoseconds / 1_000.0)
    } else {
        format!("{nanoseconds:.2} ns")
    }
}

fn main() -> ExitCode {
    let quick = std::env::args().any(|argument| argument == "--quick");
    let skip_verification = std::env::args().any(|argument| argument == "--bench-only");
    let runs = if quick { 1 } else { 3 };
    let l2_update_count = if quick { 300_000 } else { 2_000_000 };
    let l3_count = if quick { 80_000 } else { 300_000 };
    let mut sink = 0_u64;
    let pinned_cpu = pin_first_allowed_cpu();
    print_header(pinned_cpu, runs);

    if !skip_verification {
        let workload = if quick {
            Workload::Quick
        } else {
            Workload::Full
        };
        if !run_verification(workload) {
            println!("\nVerification failed. Benchmark numbers are not reported.");
            return ExitCode::FAILURE;
        }
    }

    println!("\nSTEP 2 — Benchmark");
    println!(
        "Batch-normalized throughput-equivalent service times, not end-to-end exchange latency."
    );
    if cfg!(debug_assertions) {
        println!(
            "WARNING: this is an unoptimized debug build. The timings below are several times\n\
             slower than the release build and should not be quoted. Use --release."
        );
    }

    let l2_updates = make_l2_updates(l2_update_count);
    let (l2_update_stats, l2_update_samples) = measure_batches(
        l2_update_count,
        4_096,
        runs,
        L2Book::new,
        |book, begin, end| {
            let mut hash = 0_u64;
            for update in &l2_updates[begin..end] {
                book.set_level(update.side, update.price, update.quantity);
                hash = mix64(
                    hash ^ (book.best_bid() + 1) as u64 ^ (((book.best_ask() + 1) as u64) << 32),
                );
            }
            hash
        },
        L2Book::state_hash,
        &mut sink,
    );

    let top10_operations = if quick { 20_000 } else { 150_000 };
    let (top10_stats, top10_samples) = measure_batches(
        top10_operations,
        128,
        runs,
        make_sparse_l2,
        |book, begin, end| {
            let mut hash = 0_u64;
            for index in begin..end {
                let side = if index & 1 == 0 { Side::Bid } else { Side::Ask };
                hash = mix64(hash ^ book.top_checksum(side, 10));
            }
            hash
        },
        L2Book::state_hash,
        &mut sink,
    );

    let top1000_operations = if quick { 1_000 } else { 12_000 };
    let (top1000_stats, top1000_samples) = measure_batches(
        top1000_operations,
        8,
        runs,
        make_sparse_l2,
        |book, begin, end| {
            let mut hash = 0_u64;
            for index in begin..end {
                let side = if index & 1 == 0 { Side::Bid } else { Side::Ask };
                hash = mix64(hash ^ book.top_checksum(side, 1_000));
            }
            hash
        },
        L2Book::state_hash,
        &mut sink,
    );

    let (vwap1000_stats, vwap1000_samples) = measure_batches(
        top1000_operations,
        8,
        runs,
        make_sparse_l2,
        |book, begin, end| {
            let mut hash = 0_u64;
            for index in begin..end {
                let side = if index & 1 == 0 { Side::Bid } else { Side::Ask };
                let result = book.sweep(side, book.total_quantity(side));
                hash = mix64(
                    hash ^ result.notional_ticks
                        ^ result.filled_quantity
                        ^ u64::from(result.levels_visited),
                );
            }
            hash
        },
        L2Book::state_hash,
        &mut sink,
    );

    print_section(
        "L2 bitmap ladder — sparse occupancy",
        &[
            BenchResult {
                scenario: "set level + cached BBO",
                does: "write one price level, then read both best prices back",
                unit: "update",
                stats: l2_update_stats,
                work_per_operation: 1.0,
                samples: l2_update_samples,
            },
            BenchResult {
                scenario: "top 10 sparse levels",
                does: "walk the 10 best levels of a book whose prices are far apart",
                unit: "level",
                stats: top10_stats,
                work_per_operation: 10.0,
                samples: top10_samples,
            },
            BenchResult {
                scenario: "top 1,000 sparse levels",
                does: "walk 1,000 occupied levels spread across the full 65,536-tick domain",
                unit: "level",
                stats: top1000_stats,
                work_per_operation: 1_000.0,
                samples: top1000_samples,
            },
            BenchResult {
                scenario: "VWAP across 1,000 sparse levels",
                does: "sweep the same 1,000 levels accumulating filled quantity and notional",
                unit: "level",
                stats: vwap1000_stats,
                work_per_operation: 1_000.0,
                samples: vwap1000_samples,
            },
        ],
    );

    let orders = make_resting_orders(l3_count);

    let (add_stats, add_samples) = measure_batches(
        l3_count,
        2_048,
        runs,
        || L3Book::new(orders.len() + 1, orders.len() + 2),
        |book, begin, end| {
            let mut hash = 0_u64;
            for order in &orders[begin..end] {
                hash ^= match book.add_passive(order.id, order.side, order.price, order.quantity) {
                    Ok(()) => 0,
                    Err(error) => 1_u64 << (error as u8),
                };
                hash = mix64(hash ^ (book.best_bid() + 1) as u64 ^ (book.best_ask() + 1) as u64);
            }
            hash
        },
        L3Book::state_hash,
        &mut sink,
    );

    let (reduce_stats, reduce_samples) = measure_batches(
        l3_count,
        2_048,
        runs,
        || seeded_l3_book(&orders),
        |book, begin, end| {
            let mut hash = 0_u64;
            for order in &orders[begin..end] {
                let quantity = (order.quantity / 2).max(1);
                let code = book
                    .amend_down(order.id, quantity)
                    .map_or_else(|error| 1_u64 << (error as u8), |()| 0);
                hash = mix64(hash ^ code ^ u64::from(book.live_orders()));
            }
            hash
        },
        L3Book::state_hash,
        &mut sink,
    );

    let (replace_stats, replace_samples) = measure_batches(
        l3_count,
        2_048,
        runs,
        || seeded_l3_book(&orders),
        |book, begin, end| {
            let mut hash = 0_u64;
            for order in &orders[begin..end] {
                let replacement_id = orders.len() as u32 + order.id;
                hash = mix64(
                    hash ^ result_hash(book.cancel_replace(
                        order.id,
                        replacement_id,
                        order.price ^ 31,
                        order.quantity + 1,
                    )) ^ u64::from(book.live_orders()),
                );
            }
            hash
        },
        L3Book::state_hash,
        &mut sink,
    );

    let (cancel_stats, cancel_samples) = measure_batches(
        l3_count,
        2_048,
        runs,
        || seeded_l3_book(&orders),
        |book, begin, end| {
            let mut hash = 0_u64;
            for order in &orders[begin..end] {
                let code = book
                    .cancel(order.id)
                    .map_or_else(|error| 1_u64 << (error as u8), |()| 0);
                hash = mix64(hash ^ code ^ u64::from(book.live_orders()));
            }
            hash
        },
        L3Book::state_hash,
        &mut sink,
    );

    let match_one_count = if quick { 50_000 } else { 250_000 };
    let (match_one_stats, match_one_samples) = measure_batches(
        match_one_count,
        1_024,
        runs,
        || {
            let mut book = L3Book::new(match_one_count + 1, match_one_count * 2 + 8);
            for index in 0..match_one_count {
                book.add_passive(index as u32 + 1, Side::Ask, 40_000, 1)
                    .unwrap();
            }
            book
        },
        |book, begin, end| {
            let mut hash = 0_u64;
            for index in begin..end {
                hash = mix64(
                    hash ^ result_hash(book.submit_limit(
                        match_one_count as u32 + index as u32 + 1,
                        Side::Bid,
                        40_000,
                        1,
                        TimeInForce::ImmediateOrCancel,
                    )),
                );
            }
            hash
        },
        L3Book::state_hash,
        &mut sink,
    );

    let match64_count = if quick { 1_024 } else { 6_144 };
    let maker_count = match64_count * 64;
    let (match64_stats, match64_samples) = measure_batches(
        match64_count,
        32,
        runs,
        || {
            let mut book = L3Book::new(maker_count + 1, maker_count + match64_count + 8);
            for index in 0..maker_count {
                book.add_passive(index as u32 + 1, Side::Ask, 40_000, 1)
                    .unwrap();
            }
            book
        },
        |book, begin, end| {
            let mut hash = 0_u64;
            for index in begin..end {
                hash = mix64(
                    hash ^ result_hash(book.submit_limit(
                        maker_count as u32 + index as u32 + 1,
                        Side::Bid,
                        40_000,
                        64,
                        TimeInForce::ImmediateOrCancel,
                    )),
                );
            }
            hash
        },
        L3Book::state_hash,
        &mut sink,
    );

    let sweep_prices = sparse_prices(33_536, 65_000, 1_000);
    let sparse_samples_count = 32 * runs;
    let mut sparse_samples = Vec::with_capacity(sparse_samples_count);
    for _ in 0..sparse_samples_count {
        let mut book = L3Book::new(1_001, 2_100);
        for (index, &price) in sweep_prices.iter().enumerate() {
            book.add_passive(index as u32 + 1, Side::Ask, price, 1)
                .unwrap();
        }
        let started = Instant::now();
        let report = book
            .submit_limit(2_000, Side::Bid, u16::MAX, 1_000, TimeInForce::FillOrKill)
            .unwrap();
        sparse_samples.push(started.elapsed().as_secs_f64() * 1_000_000_000.0);
        sink = mix64(sink ^ report.report_hash ^ book.state_hash());
    }
    let sparse_stats = summarize(sparse_samples);

    let mixed_count = if quick { 80_000 } else { 300_000 };
    let commands = make_mixed_commands(mixed_count);
    let (mixed_stats, mixed_samples) = measure_batches(
        mixed_count,
        2_048,
        runs,
        || L3Book::new(commands.len() + 4_096, commands.len() * 2 + 8_192),
        |book, begin, end| {
            let mut hash = 0_u64;
            for command in &commands[begin..end] {
                let command_hash = match command.command_type {
                    CommandType::Add => book
                        .add_passive(command.id, command.side, command.price, command.quantity)
                        .map_or_else(|error| 1_u64 << (error as u8), |()| 0),
                    CommandType::Cancel => book
                        .cancel(command.id)
                        .map_or_else(|error| 1_u64 << (error as u8), |()| 0),
                    CommandType::Amend => book
                        .amend_down(command.id, command.quantity)
                        .map_or_else(|error| 1_u64 << (error as u8), |()| 0),
                    CommandType::Replace => result_hash(book.cancel_replace(
                        command.id,
                        command.replacement_id,
                        command.price,
                        command.quantity,
                    )),
                    CommandType::Aggressive => result_hash(book.submit_limit(
                        command.id,
                        command.side,
                        command.price,
                        command.quantity,
                        TimeInForce::ImmediateOrCancel,
                    )),
                };
                hash = mix64(hash ^ command_hash ^ u64::from(book.live_orders()));
            }
            hash
        },
        L3Book::state_hash,
        &mut sink,
    );

    print_section(
        "L3 FIFO order book + matching engine",
        &[
            BenchResult {
                scenario: "passive insert + BBO read",
                does: "insert one passive order and read both best prices back",
                unit: "order",
                stats: add_stats,
                work_per_operation: 1.0,
                samples: add_samples,
            },
            BenchResult {
                scenario: "amend down, priority retained",
                does: "reduce a resting order's quantity in place, retaining queue priority",
                unit: "order",
                stats: reduce_stats,
                work_per_operation: 1.0,
                samples: reduce_samples,
            },
            BenchResult {
                scenario: "cancel/replace, new priority",
                does: "cancel a resting order and re-enter it with a new ID at the queue tail",
                unit: "order",
                stats: replace_stats,
                work_per_operation: 1.0,
                samples: replace_samples,
            },
            BenchResult {
                scenario: "cancel by order ID",
                does: "look a live order up by ID and unlink it from its FIFO queue",
                unit: "order",
                stats: cancel_stats,
                work_per_operation: 1.0,
                samples: cancel_samples,
            },
            BenchResult {
                scenario: "aggress 1 resting maker",
                does: "execute an aggressive order against the queue head, at the maker price",
                unit: "fill",
                stats: match_one_stats,
                work_per_operation: 1.0,
                samples: match_one_samples,
            },
            BenchResult {
                scenario: "aggress 64 resting makers",
                does: "execute against 64 makers in strict arrival order at one price level",
                unit: "fill",
                stats: match64_stats,
                work_per_operation: 64.0,
                samples: match64_samples,
            },
            BenchResult {
                scenario: "sweep 1,000 sparse levels",
                does: "execute against 1,000 makers spread across the full price domain in one order",
                unit: "fill",
                stats: sparse_stats,
                work_per_operation: 1_000.0,
                samples: sparse_samples_count,
            },
            BenchResult {
                scenario: "mixed order-entry stream",
                does: "replay a stream of new, cancel, amend, cancel/replace and aggressive orders",
                unit: "message",
                stats: mixed_stats,
                work_per_operation: 1.0,
                samples: mixed_samples,
            },
        ],
    );

    println!("{}", "-".repeat(RULE_WIDTH));
    println!(
        "Samples are per-batch averages over {runs} run(s); anti-optimization sink 0x{sink:016x}."
    );
    ExitCode::SUCCESS
}

#[cfg(target_os = "linux")]
fn pin_first_allowed_cpu() -> Option<usize> {
    #[repr(C)]
    struct CpuSet {
        bits: [u64; 16],
    }

    unsafe extern "C" {
        fn sched_getaffinity(pid: i32, cpusetsize: usize, mask: *mut CpuSet) -> i32;
        fn sched_setaffinity(pid: i32, cpusetsize: usize, mask: *const CpuSet) -> i32;
    }

    let mut allowed = CpuSet { bits: [0; 16] };
    // SAFETY: `allowed` is a valid writable `CpuSet`, and the size matches the object.
    if unsafe { sched_getaffinity(0, size_of::<CpuSet>(), &mut allowed) } != 0 {
        return None;
    }
    let cpu = allowed
        .bits
        .iter()
        .enumerate()
        .find_map(|(word_index, &word)| {
            (word != 0).then_some(word_index * 64 + word.trailing_zeros() as usize)
        })?;
    let mut selected = CpuSet { bits: [0; 16] };
    selected.bits[cpu / 64] = 1_u64 << (cpu % 64);
    // SAFETY: `selected` is a valid readable `CpuSet`, and the size matches the object.
    if unsafe { sched_setaffinity(0, size_of::<CpuSet>(), &selected) } != 0 {
        return None;
    }
    Some(cpu)
}

#[cfg(not(target_os = "linux"))]
fn pin_first_allowed_cpu() -> Option<usize> {
    None
}
