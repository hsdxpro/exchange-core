//! Pipeline latency: what one command costs end to end.
//!
//! This measures the whole path a client command takes — sequence, journal
//! append, journal sync, balance reservation, match, event emission — not the
//! matching engine alone. The engine's own benchmark reports tens of
//! nanoseconds; the numbers here are larger, and the gap is the point. It is
//! what the venue actually costs on top of the book.
//!
//! Method follows the engine's benchmark: long runs, several repetitions, and
//! the minimum reported. Contention only ever adds time, so the smallest
//! observation is the best estimate of the uncontended cost. On a loaded
//! desktop the median moves by a factor of two or more between runs; the
//! minimum barely moves.

use bx_journal::{FileLog, MemoryLog};
use bx_pipeline::instrument::{AssetId, Instrument, Instruments};
use bx_pipeline::{Exchange, limit_order, market_order};
use bx_protocol::{Command, CommandKind, Side, Ticks, TimeInForce};
use std::hint::black_box;
use std::time::Instant;

const BTC: AssetId = 1;
const USD: AssetId = 2;
const SYMBOL: u32 = 1;
const FLOOR: Ticks = 10_000;
const REPS: usize = 7;
/// Bigger than any run below places, so no measurement is polluted by orders
/// being rejected for want of a slot.
const MAX_OPEN_ORDERS: u32 = 400_000;

fn venue() -> Exchange<MemoryLog> {
    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(
        SYMBOL,
        BTC,
        USD,
        FLOOR,
        1_000_000_000,
        MAX_OPEN_ORDERS,
    ));
    let mut exchange = Exchange::new(MemoryLog::new(), instruments).unwrap();
    for account in 1..=16 {
        exchange.deposit(account, USD, u64::MAX).unwrap();
        exchange.deposit(account, BTC, u64::MAX).unwrap();
    }
    exchange
}

fn report(name: &str, per_op_ns: &mut [f64], unit: &str) {
    per_op_ns.sort_by(f64::total_cmp);
    let min = per_op_ns[0];
    let median = per_op_ns[per_op_ns.len() / 2];
    println!("{name:<38}{min:>9.0} ns{median:>12.0} ns   {unit}");
}

/// Passive limit orders that never cross, so this is the pure resting path.
fn resting_orders(sink: &mut u64) -> Vec<f64> {
    const COUNT: u64 = 200_000;
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let mut exchange = venue();
        let commands: Vec<Command> = (0..COUNT)
            .map(|i| {
                let price = FLOOR + 1_000 + (i % 4_000) as Ticks;
                limit_order(1 + i % 16, SYMBOL, i + 1, Side::Bid, price, 1)
            })
            .collect();

        let started = Instant::now();
        for mut command in commands {
            let events = exchange.submit(&mut command).unwrap();
            *sink = sink.wrapping_add(events.len() as u64);
        }
        samples.push(started.elapsed().as_secs_f64() * 1e9 / COUNT as f64);
        black_box(&exchange);
    }
    samples
}

/// Every order crosses one resting maker, so this is the matching path with
/// settlement on both sides.
fn crossing_orders(sink: &mut u64) -> Vec<f64> {
    const COUNT: u64 = 100_000;
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let mut exchange = venue();
        // Seed one maker per taker, all at the same price.
        for i in 0..COUNT {
            let mut maker = limit_order(1, SYMBOL, i + 1, Side::Ask, FLOOR + 500, 1);
            exchange.submit(&mut maker).unwrap();
        }

        let started = Instant::now();
        for i in 0..COUNT {
            let mut taker = market_order(2, SYMBOL, COUNT + i + 1, Side::Bid, 1);
            let events = exchange.submit(&mut taker).unwrap();
            *sink = sink.wrapping_add(events.len() as u64);
        }
        samples.push(started.elapsed().as_secs_f64() * 1e9 / COUNT as f64);
        black_box(&exchange);
    }
    samples
}

/// Cancel by client order ID, which pays for the hash lookup the engine's dense
/// index avoids internally.
fn cancels(sink: &mut u64) -> Vec<f64> {
    const COUNT: u64 = 100_000;
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let mut exchange = venue();
        for i in 0..COUNT {
            let price = FLOOR + 1_000 + (i % 4_000) as Ticks;
            let mut order = limit_order(1 + i % 16, SYMBOL, i + 1, Side::Bid, price, 1);
            exchange.submit(&mut order).unwrap();
        }

        let started = Instant::now();
        for i in 0..COUNT {
            let mut cancel = Command::new(
                CommandKind::Cancel,
                1 + i % 16,
                SYMBOL,
                i + 1,
                Side::Bid,
                0,
                0,
                TimeInForce::GoodTillCancel,
            );
            let events = exchange.submit(&mut cancel).unwrap();
            *sink = sink.wrapping_add(events.len() as u64);
        }
        samples.push(started.elapsed().as_secs_f64() * 1e9 / COUNT as f64);
        black_box(&exchange);
    }
    samples
}

/// A realistic mix: mostly posting and cancelling, some taking.
fn mixed_stream(sink: &mut u64) -> Vec<f64> {
    const COUNT: u64 = 200_000;
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let mut exchange = venue();
        let mut state = 0x2026_u64;
        let mut next = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            state >> 16
        };
        let commands: Vec<Command> = (0..COUNT)
            .map(|i| {
                let account = 1 + next() % 16;
                let roll = next() % 100;
                if roll < 20 && i > 100 {
                    Command::new(
                        CommandKind::Cancel,
                        account,
                        SYMBOL,
                        i - 100,
                        Side::Bid,
                        0,
                        0,
                        TimeInForce::GoodTillCancel,
                    )
                } else if roll < 30 {
                    market_order(account, SYMBOL, i + 1, Side::Bid, 1)
                } else {
                    let side = if next().is_multiple_of(2) {
                        Side::Bid
                    } else {
                        Side::Ask
                    };
                    let price = match side {
                        Side::Bid => FLOOR + 1_000 - (next() % 50) as Ticks,
                        Side::Ask => FLOOR + 1_001 + (next() % 50) as Ticks,
                    };
                    limit_order(account, SYMBOL, i + 1, side, price, 1)
                }
            })
            .collect();

        let started = Instant::now();
        for mut command in commands {
            let events = exchange.submit(&mut command).unwrap();
            *sink = sink.wrapping_add(events.len() as u64);
        }
        samples.push(started.elapsed().as_secs_f64() * 1e9 / COUNT as f64);
        black_box(&exchange);
    }
    samples
}

/// The same mixed traffic, submitted in batches. One journal sync covers the
/// whole batch, which is the difference between being sync-bound and being
/// compute-bound.
fn batched_stream(sink: &mut u64, batch: usize) -> Vec<f64> {
    const COUNT: u64 = 200_000;
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let mut exchange = venue();
        let mut state = 0x2026_u64;
        let mut next = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            state >> 16
        };
        let mut commands: Vec<Command> = (0..COUNT)
            .map(|i| {
                let account = 1 + next() % 16;
                let roll = next() % 100;
                if roll < 20 && i > 100 {
                    Command::new(
                        CommandKind::Cancel,
                        account,
                        SYMBOL,
                        i - 100,
                        Side::Bid,
                        0,
                        0,
                        TimeInForce::GoodTillCancel,
                    )
                } else if roll < 30 {
                    market_order(account, SYMBOL, i + 1, Side::Bid, 1)
                } else {
                    let side = if next().is_multiple_of(2) {
                        Side::Bid
                    } else {
                        Side::Ask
                    };
                    let price = match side {
                        Side::Bid => FLOOR + 1_000 - (next() % 50) as Ticks,
                        Side::Ask => FLOOR + 1_001 + (next() % 50) as Ticks,
                    };
                    limit_order(account, SYMBOL, i + 1, side, price, 1)
                }
            })
            .collect();

        let started = Instant::now();
        for chunk in commands.chunks_mut(batch) {
            let events = exchange.submit_batch(chunk).unwrap();
            *sink = sink.wrapping_add(events.len() as u64);
        }
        samples.push(started.elapsed().as_secs_f64() * 1e9 / COUNT as f64);
        black_box(&exchange);
    }
    samples
}

/// The same mixed traffic against a real file, at several batch sizes.
///
/// This is the number that decides the venue's throughput. An fsync is tens of
/// microseconds; the in-memory figures above are five hundred times smaller and
/// say nothing about it. Durability is per batch, and no command in a batch is
/// acknowledged until every one of them is on disk.
fn on_disk(sink: &mut u64, batch: usize) -> Vec<f64> {
    const COUNT: u64 = 20_000;
    let mut samples = Vec::with_capacity(3);
    for run in 0..3 {
        let path = std::env::temp_dir().join(format!(
            "bx-latency-{}-{batch}-{run}.log",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);

        let mut instruments = Instruments::new();
        instruments.insert(Instrument::new(
            SYMBOL,
            BTC,
            USD,
            FLOOR,
            1_000_000_000,
            MAX_OPEN_ORDERS,
        ));
        let mut exchange = Exchange::new(FileLog::open(&path).unwrap(), instruments).unwrap();
        for account in 1..=16 {
            exchange.deposit(account, USD, u64::MAX).unwrap();
            exchange.deposit(account, BTC, u64::MAX).unwrap();
        }

        let mut commands: Vec<Command> = (0..COUNT)
            .map(|i| {
                let price = FLOOR + 1_000 + (i % 4_000) as Ticks;
                limit_order(1 + i % 16, SYMBOL, i + 1, Side::Bid, price, 1)
            })
            .collect();

        let started = Instant::now();
        for chunk in commands.chunks_mut(batch) {
            let events = exchange.submit_batch(chunk).unwrap();
            *sink = sink.wrapping_add(events.len() as u64);
        }
        samples.push(started.elapsed().as_secs_f64() * 1e9 / COUNT as f64);
        drop(exchange);
        let _ = std::fs::remove_file(&path);
    }
    samples
}

/// How long a restart takes, with and without a snapshot.
///
/// Recovery time is an availability number, not a throughput one: it is how
/// long the venue is down after a crash. Replaying from zero grows without
/// bound as the journal does, which is the whole reason snapshots exist.
///
/// The saving depends entirely on the ratio between the journal and the resting
/// book, because restoring a snapshot costs one insert per resting order. Flow
/// here posts and later cancels, so the book stays near a thousand orders while
/// the journal runs to two hundred thousand — which is the shape of a real
/// venue, where commands vastly outnumber live orders. A journal where nothing
/// is ever cancelled saturates the book and the snapshot saves almost nothing.
fn recovery() -> (f64, f64, u64) {
    const COUNT: u64 = 200_000;
    const SNAPSHOT_AFTER: u64 = 190_000;
    /// How long an order rests before its owner cancels it.
    const LIFETIME: u64 = 2_001;

    let mut exchange = venue();
    let mut snapshot = None;
    for i in 0..COUNT {
        let mut command = if i.is_multiple_of(2) {
            let price = FLOOR + 1_000 + (i % 4_000) as Ticks;
            limit_order(1 + i % 16, SYMBOL, i + 1, Side::Bid, price, 1)
        } else if i > LIFETIME {
            // Cancels the order posted LIFETIME commands ago, so the resting
            // book stays bounded while the journal keeps growing.
            Command::new(
                CommandKind::Cancel,
                1 + i % 16,
                SYMBOL,
                i - LIFETIME + 1,
                Side::Bid,
                0,
                0,
                TimeInForce::GoodTillCancel,
            )
        } else {
            let price = FLOOR + 1_000 + (i % 4_000) as Ticks;
            limit_order(1 + i % 16, SYMBOL, i + 1, Side::Bid, price, 1)
        };
        exchange.submit(&mut command).unwrap();
        if i + 1 == SNAPSHOT_AFTER {
            snapshot = Some(exchange.snapshot());
        }
    }
    let snapshot = snapshot.unwrap();
    let storage = exchange.into_storage();

    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(
        SYMBOL,
        BTC,
        USD,
        FLOOR,
        1_000_000_000,
        MAX_OPEN_ORDERS,
    ));
    let mut full = Exchange::new(storage, instruments).unwrap();
    for account in 1..=16 {
        full.deposit(account, USD, u64::MAX).unwrap();
        full.deposit(account, BTC, u64::MAX).unwrap();
    }
    let started = Instant::now();
    let replayed = full.recover().unwrap();
    let full_ms = started.elapsed().as_secs_f64() * 1e3;

    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(
        SYMBOL,
        BTC,
        USD,
        FLOOR,
        1_000_000_000,
        MAX_OPEN_ORDERS,
    ));
    let mut partial = Exchange::new(full.into_storage(), instruments).unwrap();
    let started = Instant::now();
    let after = partial.recover_from(&snapshot).unwrap();
    let snapshot_ms = started.elapsed().as_secs_f64() * 1e3;

    assert_eq!(replayed, COUNT);
    assert_eq!(after, COUNT - SNAPSHOT_AFTER);
    (full_ms, snapshot_ms, snapshot.orders.len() as u64)
}

fn main() {
    println!("\nExchange pipeline latency");
    println!("Full path per command: sequence, journal append and sync, reserve, match, emit.");
    println!("Journal is in memory, so this excludes real disk. Minimum of {REPS} runs.\n");
    println!("{:<38}{:>12}{:>15}   unit", "Path", "min", "median");
    println!("{}", "-".repeat(80));

    let mut sink = 0_u64;
    report(
        "passive limit order",
        &mut resting_orders(&mut sink),
        "per order",
    );
    report(
        "crossing order, one fill",
        &mut crossing_orders(&mut sink),
        "per order",
    );
    report("cancel by order id", &mut cancels(&mut sink), "per cancel");
    report("mixed stream", &mut mixed_stream(&mut sink), "per command");
    report(
        "mixed stream, batch 64",
        &mut batched_stream(&mut sink, 64),
        "per command",
    );
    report(
        "mixed stream, batch 1024",
        &mut batched_stream(&mut sink, 1_024),
        "per command",
    );

    println!();
    println!("Against a real file, where the fsync dominates:");
    println!("{}", "-".repeat(80));
    for batch in [1_usize, 16, 256, 4_096] {
        report(
            &format!("on disk, batch {batch}"),
            &mut on_disk(&mut sink, batch),
            "per command",
        );
    }

    println!();
    println!("Restart time for a 200,000 command journal, snapshot taken at 190,000:");
    println!("{}", "-".repeat(80));
    let (full_ms, snapshot_ms, orders) = recovery();
    println!("{:<38}{full_ms:>9.1} ms", "replay all 200,000");
    println!("{:<38}{snapshot_ms:>9.1} ms", "snapshot + replay 10,000");
    println!(
        "{:<38}{:>9.1}x   ({orders} orders in the snapshot)",
        "speedup",
        full_ms / snapshot_ms
    );

    println!("{}", "-".repeat(80));
    println!("sink {sink:#x}");
}
