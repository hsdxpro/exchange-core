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

use bx_journal::{FileLog, MemoryLog, Replica, ReplicatedLog};
use bx_pipeline::hub::{Channel, Hub};
use bx_pipeline::instrument::{AssetId, Instrument, Instruments};
use bx_pipeline::{Exchange, limit_order, market_order};
use bx_protocol::{Command, CommandKind, Side, Ticks, TimeInForce};
use std::hint::black_box;
use std::net::TcpListener;
use std::time::Instant;

const BTC: AssetId = 1;
const USD: AssetId = 2;
const SYMBOL: u32 = 1;
const FLOOR: Ticks = 10_000;
const REPS: usize = 5;
/// How long the leader waits for a follower to confirm. Loopback here, so this
/// only needs to be longer than a local round trip.
const ACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
/// Bigger than any run below places, so no measurement is polluted by orders
/// being rejected for want of a slot -- and no bigger, because the pool is
/// allocated up front and a needlessly large one only costs cache.
const MAX_OPEN_ORDERS: u32 = 150_000;

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

/// Reports a durability figure as the throughput it implies, because "can this
/// keep up with a million orders a second" is the question and nanoseconds per
/// command is only the answer once divided into a second.
fn report_throughput(name: &str, per_op_ns: &mut [f64]) {
    per_op_ns.sort_by(f64::total_cmp);
    let best = per_op_ns[0];
    let per_second = 1e9 / best;
    println!("{name:<38}{best:>12.0} ns{:>16.0} cmd/sec", per_second);
}

/// Passive limit orders that never cross, so this is the pure resting path.
fn resting_orders(sink: &mut u64) -> Vec<f64> {
    const COUNT: u64 = 100_000;
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
        assert_eq!(
            exchange.open_orders() as u64,
            COUNT,
            "orders were rejected, so this measured the reject path"
        );
        black_box(&exchange);
    }
    samples
}

/// Every order crosses one resting maker, so this is the matching path with
/// settlement on both sides.
fn crossing_orders(sink: &mut u64) -> Vec<f64> {
    const COUNT: u64 = 50_000;
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
        assert_eq!(
            exchange.open_orders(),
            0,
            "every maker should have been consumed"
        );
        black_box(&exchange);
    }
    samples
}

/// The same crossing path, but for an account that is also resting on the
/// symbol, so the self-match check actually runs.
///
/// `crossing_orders` above measures a taker with nothing resting, which is the
/// common case and skips the check in one lookup. That leaves the expensive
/// branch unmeasured, and an unmeasured branch is where the last two hundred
/// fold regression lived. Here the taker keeps a resting order of its own two
/// levels away, so every order pays for the walk.
fn crossing_with_self_check(sink: &mut u64) -> Vec<f64> {
    const COUNT: u64 = 50_000;
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let mut exchange = venue();
        // Account 2 takes, and also rests an ask far behind the best.
        let mut own = limit_order(2, SYMBOL, u64::MAX, Side::Ask, FLOOR + 900, 1);
        exchange.submit(&mut own).unwrap();
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
        assert_eq!(exchange.open_orders(), 0, "a cancel did not take effect");
        black_box(&exchange);
    }
    samples
}

/// Posts and cancels, so the resting book stays near `LIFETIME` orders however
/// many commands are sent.
///
/// A durable-throughput run needs millions of commands to amortise a sync across
/// a large group. Posting all of them would saturate the order pool and start
/// measuring rejections, which is cheaper than real work and would report a
/// throughput the venue cannot reach.
fn bounded_commands(count: u64) -> Vec<Command> {
    const LIFETIME: u64 = 2_001;
    (0..count)
        .map(|i| {
            if i.is_multiple_of(2) || i <= LIFETIME {
                let price = FLOOR + 1_000 + (i % 4_000) as Ticks;
                limit_order(1 + i % 16, SYMBOL, i + 1, Side::Bid, price, 1)
            } else {
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
            }
        })
        .collect()
}

/// A realistic mix: mostly posting and cancelling, some taking.
///
/// One generator for every stream measurement, so they all price the same work
/// and a difference between them is the thing being measured rather than a
/// difference in traffic. Deterministic, so runs are comparable.
fn mixed_commands(count: u64) -> Vec<Command> {
    let mut state = 0x2026_u64;
    let mut next = || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        state >> 16
    };
    (0..count)
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
        .collect()
}

fn mixed_stream(sink: &mut u64) -> Vec<f64> {
    const COUNT: u64 = 100_000;
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let mut exchange = venue();
        let commands = mixed_commands(COUNT);

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

/// The same mixed traffic, with subscribers attached.
///
/// Fan-out is on the event path, so it is part of what a command costs. Three
/// channels are subscribed -- depth, tape, and one private account -- which is
/// what a real connected client set looks like from the venue's side.
fn mixed_stream_with_subscribers(sink: &mut u64) -> Vec<f64> {
    const COUNT: u64 = 100_000;
    const RETAINED: usize = 8_192;
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let mut exchange = venue();
        let mut hub = Hub::new(RETAINED);
        hub.subscribe(Channel::Book(SYMBOL));
        hub.subscribe(Channel::Trades(SYMBOL));
        hub.subscribe(Channel::Account(1));
        let commands = mixed_commands(COUNT);

        let started = Instant::now();
        for mut command in commands {
            let events = exchange.submit(&mut command).unwrap();
            hub.publish(events);
            *sink = sink.wrapping_add(events.len() as u64);
        }
        samples.push(started.elapsed().as_secs_f64() * 1e9 / COUNT as f64);
        black_box(&exchange);
        black_box(&hub);
    }
    samples
}

/// The same mixed traffic, submitted in batches. One journal sync covers the
/// whole batch, which is the difference between being sync-bound and being
/// compute-bound.
fn batched_stream(sink: &mut u64, batch: usize) -> Vec<f64> {
    const COUNT: u64 = 100_000;
    let mut samples = Vec::with_capacity(REPS);
    for _ in 0..REPS {
        let mut exchange = venue();
        let mut commands = mixed_commands(COUNT);

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
    // Fixed number of *syncs*, not of commands. An fsync costs milliseconds and
    // everything else costs nanoseconds, so a fixed command count would make
    // batch 1 do twenty thousand syncs and take a minute while batch 4,096 did
    // five and took nothing. Holding the syncs constant gives every batch size
    // the same wall-clock budget and the same number of samples of the thing
    // that actually varies.
    const SYNCS: u64 = 48;
    /// Enough samples of the sync, but never so much traffic that a large batch
    /// takes minutes.
    const MOST: u64 = 400_000;
    let count: u64 = (SYNCS * batch as u64).min(MOST.max(batch as u64));
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

        let mut commands = bounded_commands(count);

        let started = Instant::now();
        for chunk in commands.chunks_mut(batch) {
            let events = exchange.submit_batch(chunk).unwrap();
            *sink = sink.wrapping_add(events.len() as u64);
        }
        samples.push(started.elapsed().as_secs_f64() * 1e9 / count as f64);
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
    const COUNT: u64 = 100_000;
    const SNAPSHOT_AFTER: u64 = 95_000;
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
    // Deposits are journalled too, so the log is longer than the order flow.
    let journalled = exchange.next_sequence();
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
    // No re-funding: deposits are journalled, so replay restores the money.
    let mut full = Exchange::new(storage, instruments).unwrap();
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

    assert_eq!(replayed, journalled);
    assert_eq!(after, journalled - snapshot.sequence);
    (full_ms, snapshot_ms, snapshot.orders.len() as u64)
}

/// The same traffic, acknowledged by a quorum instead of by an fsync.
///
/// This is the comparison the whole durability design rests on. `on_disk` above
/// waits for the platter; this waits for another process to confirm it holds the
/// group. The follower here is on loopback, so this is the floor rather than a
/// realistic network -- a real LAN adds tens of microseconds. The point is the
/// shape: the leader stops waiting on its own disk.
fn replicated(sink: &mut u64, batch: usize) -> Vec<f64> {
    const SYNCS: u64 = 48;
    /// Enough samples of the sync, but never so much traffic that a large batch
    /// takes minutes.
    const MOST: u64 = 400_000;
    let count: u64 = (SYNCS * batch as u64).min(MOST.max(batch as u64));
    let mut samples = Vec::with_capacity(3);

    for run in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let follower = std::thread::spawn(move || {
            let mut replica = Replica::new(MemoryLog::new(), false);
            let _ = replica.serve_one(&listener);
        });

        let path = std::env::temp_dir().join(format!(
            "bx-replicated-{}-{batch}-{run}.log",
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
        let local = FileLog::open(&path).unwrap();
        let log =
            ReplicatedLog::connect(local, std::slice::from_ref(&address), ACK_TIMEOUT).unwrap();
        let mut exchange = Exchange::new(log, instruments).unwrap();
        for account in 1..=16 {
            exchange.deposit(account, USD, u64::MAX).unwrap();
            exchange.deposit(account, BTC, u64::MAX).unwrap();
        }

        let mut commands = bounded_commands(count);

        let started = Instant::now();
        for chunk in commands.chunks_mut(batch) {
            let events = exchange.submit_batch(chunk).unwrap();
            *sink = sink.wrapping_add(events.len() as u64);
        }
        samples.push(started.elapsed().as_secs_f64() * 1e9 / count as f64);

        drop(exchange);
        let _ = follower.join();
        let _ = std::fs::remove_file(&path);
    }
    samples
}

fn main() {
    let whole_run = Instant::now();
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
    report(
        "crossing, self-match check runs",
        &mut crossing_with_self_check(&mut sink),
        "per order",
    );
    report("cancel by order id", &mut cancels(&mut sink), "per cancel");
    report("mixed stream", &mut mixed_stream(&mut sink), "per command");
    report(
        "mixed stream, 3 subscribers",
        &mut mixed_stream_with_subscribers(&mut sink),
        "per command",
    );
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
    println!("Durable throughput. How fast can the venue acknowledge?");
    println!("{}", "-".repeat(80));
    for batch in [1_usize, 16, 256, 4_096, 16_384] {
        report_throughput(
            &format!("local fsync, group of {batch}"),
            &mut on_disk(&mut sink, batch),
        );
    }
    println!();
    for batch in [1_usize, 16, 256, 4_096, 16_384] {
        report_throughput(
            &format!("quorum on loopback, group of {batch}"),
            &mut replicated(&mut sink, batch),
        );
    }

    println!();
    println!("Restart time for a 100,000 command journal, snapshot taken at 95,000:");
    println!("{}", "-".repeat(80));
    let (full_ms, snapshot_ms, orders) = recovery();
    println!("{:<38}{full_ms:>9.1} ms", "replay all 100,000");
    println!("{:<38}{snapshot_ms:>9.1} ms", "snapshot + replay 5,000");
    println!(
        "{:<38}{:>9.1}x   ({orders} orders in the snapshot)",
        "speedup",
        full_ms / snapshot_ms
    );

    println!("{}", "-".repeat(80));
    println!(
        "completed in {:.1} s   sink {sink:#x}",
        whole_run.elapsed().as_secs_f64()
    );
}
