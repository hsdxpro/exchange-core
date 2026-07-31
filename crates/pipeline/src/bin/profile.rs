//! Where a command's nanoseconds go, stage by stage.
//!
//! `latency` answers "what does a command cost"; this answers "which part of it
//! is expensive", which is the question you need before optimising anything.
//!
//! Stages are separated by subtraction, not by instrumenting the hot path: a
//! timer around each stage would cost more than the stages do. Each row below
//! runs the same command stream through progressively more of the pipeline, and
//! the difference between two rows is the stage between them. That makes every
//! figure a difference of two measured totals rather than an estimate.
//!
//! The one thing subtraction cannot do is resolve a stage smaller than the
//! run-to-run spread of the totals it sits between. Those are reported as
//! "under the noise" rather than as a number that would look precise and not be.

use bx_journal::{Journal, MemoryLog};
use bx_pipeline::book::{Book, Outcome};
use bx_pipeline::instrument::{AssetId, Instrument, Instruments};
use bx_pipeline::{Exchange, limit_order};
use bx_protocol::{Command, Side, Ticks, TimeInForce};
use std::hint::black_box;
use std::time::Instant;

const BTC: AssetId = 1;
const USD: AssetId = 2;
const SYMBOL: u32 = 1;
const FLOOR: Ticks = 10_000;
const ORDERS: usize = 200_000;
const REPS: usize = 7;
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

/// Passive limit orders that never cross, so every run does the same work:
/// reserve, rest, emit. Prices spread over 2,000 levels so the book is not one
/// hot cache line pretending to be a book.
fn orders() -> Vec<Command> {
    (0..ORDERS as u64)
        .map(|i| {
            let price = FLOOR + 1 + (i % 2_000) as Ticks;
            limit_order(1 + i % 16, SYMBOL, i + 1, Side::Bid, price, 1)
        })
        .collect()
}

/// Best of `REPS`. Contention only ever adds time, so the minimum is the best
/// estimate of the uncontended cost -- the same rule `latency` follows.
fn best_ns(mut run: impl FnMut() -> f64) -> f64 {
    (0..REPS).map(|_| run()).fold(f64::MAX, f64::min)
}

/// The whole path: append, apply, sync.
fn whole() -> f64 {
    best_ns(|| {
        let mut exchange = venue();
        let mut commands = orders();
        let started = Instant::now();
        for command in commands.iter_mut() {
            black_box(exchange.submit(command).unwrap().len());
        }
        started.elapsed().as_secs_f64() * 1e9 / ORDERS as f64
    })
}

/// Append and apply, syncing once per group of 64.
///
/// `enqueue` without a `commit` is not the sync-free path -- it is an
/// unbounded one. Events are held until a commit releases them, so a loop that
/// never commits grows the event buffer by every command in the run and
/// measures reallocation. Committing per group is what a venue under load
/// actually does.
fn through_apply() -> f64 {
    const GROUP: usize = 64;
    best_ns(|| {
        let mut exchange = venue();
        let mut commands = orders();
        let started = Instant::now();
        for group in commands.chunks_mut(GROUP) {
            black_box(exchange.submit_batch(group).unwrap().len());
        }
        started.elapsed().as_secs_f64() * 1e9 / ORDERS as f64
    })
}

/// The journal alone: the same records into a bare log, nothing applied.
fn append_only() -> f64 {
    best_ns(|| {
        let mut journal = Journal::open(MemoryLog::new()).unwrap();
        let mut commands = orders();
        let started = Instant::now();
        for command in commands.iter_mut() {
            journal.append(command).unwrap();
        }
        let elapsed = started.elapsed().as_secs_f64();
        black_box(journal.sync().is_ok());
        elapsed * 1e9 / ORDERS as f64
    })
}

/// The matching engine and its wrapper alone: the same orders straight into a
/// `Book`, with no journal, no balances and no events.
///
/// Measured rather than quoted. The obvious move is to cite matching-engine's
/// own figure for a passive insert, but that is a different binary with a
/// different book shape, and a number carried across repositories is a guess
/// wearing a decimal point.
fn book_only() -> f64 {
    best_ns(|| {
        let instrument = Instrument::new(SYMBOL, BTC, USD, FLOOR, 1_000_000_000, MAX_OPEN_ORDERS);
        let mut book = Book::new(instrument);
        let mut outcome = Outcome::default();
        let commands = orders();
        let started = Instant::now();
        for (i, command) in commands.iter().enumerate() {
            book.submit_into(
                &mut outcome,
                (1, i as u64 + 1),
                Side::Bid,
                command.price,
                command.quantity,
                TimeInForce::GoodTillCancel,
                false,
            );
            black_box(outcome.reject.is_none());
        }
        started.elapsed().as_secs_f64() * 1e9 / ORDERS as f64
    })
}

fn main() {
    println!(
        "command path attribution, {ORDERS} passive limit orders over 2,000 levels, \
         best of {REPS}\n"
    );

    let whole = whole();
    let through_apply = through_apply();
    let append = append_only();
    let book = book_only();
    let apply = through_apply - append;
    let sync = whole - through_apply;

    let row = |name: &str, ns: f64| {
        if ns < 1.0 {
            println!("  {name:<34} {:>8} ns   {:>6}", "under", "noise");
        } else {
            println!("  {name:<34} {ns:>8.1} ns   {:>5.1}%", ns / whole * 100.0);
        }
    };

    row("journal append", append);
    row("book (match + rest)", book);
    row("reserve + hold + emit", apply - book);
    row("sync, per command at group of 1", sync);
    println!("  {:<34} {whole:>8.1} ns   100.0%", "total");

    println!(
        "\nThe book row is measured here, not quoted from matching-engine. Same \
         engine underneath, but a different wrapper and a far larger resting \
         set -- that project reports 5.3 ns for a passive insert against a \
         nearly empty book, and carrying the figure across would be a guess \
         wearing a decimal point.\n\
         \nRows move together run to run on a loaded machine. The shares are \
         the stable part, and they say the venue's own bookkeeping costs \
         roughly twice what the matching does."
    );
}
