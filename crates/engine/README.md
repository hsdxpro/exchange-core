# Bitmap Exchange

A single-instrument order book, aggregated by price (MBP/L2) and by order (MBO/L3),
with a price-time FIFO matching engine. Safe, dependency-free Rust.

One design throughout: a fixed 65,536-tick ladder with a three-tier occupancy bitmap.
Empty prices are never scanned, so traversal cost tracks how many levels are *occupied*,
not how far apart they sit.

The library is `#![forbid(unsafe_code)]` and has no third-party crates. It allocates
nothing after construction.

## Performance

Rust 1.97.1 release build, Windows 11 desktop, no CPU pinning, minimum of three runs:

| Scenario | p50 | Per item |
|---|---:|---:|
| L2 set level + cached BBO | **6.32 ns** | 6.32 ns/update |
| L2 top 10 sparse levels | 42.97 ns | **4.30 ns/level** |
| L2 top 1,000 sparse levels | 4.90 µs | **4.90 ns/level** |
| L2 VWAP across 1,000 sparse levels | 4.36 µs | **4.36 ns/level** |
| L3 passive insert + BBO read | 10.16 ns | 10.16 ns/order |
| L3 amend down, priority retained | **6.35 ns** | 6.35 ns/order |
| L3 cancel/replace, new priority | 32.96 ns | 32.96 ns/order |
| L3 cancel by order ID | **9.23 ns** | 9.23 ns/order |
| L3 aggress 1 resting maker | 19.63 ns | **19.63 ns/fill** |
| L3 aggress 64 resting makers | 753.12 ns | **11.77 ns/fill** |
| L3 sweep 1,000 sparse levels | 21.90 µs | **21.90 ns/fill** |
| Mixed order-entry stream | 35.99 ns | 35.99 ns/message |

Read the **per item** column. Walking 1,000 levels spread across the full 65,536-tick
domain costs 4.90 ns each, against 4.30 ns for 10 adjacent levels. Spreading the book out
costs almost nothing, which is what the bitmap buys. Matching behaves the same way: one
fill costs 19.63 ns, 64 fills cost 11.77 ns each.

These are batch-normalized throughput-equivalent service times, not tail latency and not
end-to-end exchange latency, and the working sets are cache-resident. They came off a
loaded developer desktop; a pinned core on an isolated host does materially better. Run
it yourself instead of trusting a table. Method in
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

## Build and run

```bash
cargo run --release --bin bx-bench
```

That is the whole thing. It runs all 43 named verification groups, prints each as `PASS`
or `FAIL` with a total, and only then measures and prints latency. A failed check aborts
before any number is reported, because a benchmark from an engine that fails a
correctness check is worthless.

Add `--quick` for a one-second run, or `--bench-only` to skip verification.

**Requirements:** [rustup](https://rustup.rs), nothing else. `rust-toolchain.toml` pins
Rust 1.97.1 and rustup fetches it on first use. There are no dependencies to download.

Full gate:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --release --locked
```

`cargo test` reports each of the 43 groups as an individually named test.

## What it demonstrates

- Hierarchical bitmap best-price discovery and sparse next/previous traversal.
- Cached best bid/ask with constant-time reads.
- L2 update, remove, top-N depth, sweep, and VWAP.
- L3 strict price-time FIFO queues.
- GTC, IOC, FOK, post-only, and market-order behavior.
- Maker-price execution, partial fills, multi-fill and multi-level sweeps.
- Direct order-ID lookup for cancel, amend, and cancel/replace.
- Amend-down retaining queue priority; cancel/replace assigning a new ID and new priority.
- Atomic FOK, post-only, validation, and capacity rejection.
- Full-book aggressive matching that can recycle a released slot.
- Fixed-capacity, allocation-free engine operations after construction.
- Compact 24-byte order slots and 16-byte level descriptors.

## Verification

The suite lives in [`src/verify/`](src/verify/) and is compiled into the library rather
than hidden behind `cfg(test)`, so the shipped binary and `cargo test` execute identical
code. The binary is self-verifying: it will not report a benchmark number if any group
fails.

| | |
|---|---|
| Named check groups | 43 |
| Randomized bitmap ops vs an ordered set | 500,000 |
| Randomized L2 updates vs an ordered model | 600,000 |
| Randomized L3 commands vs a map + FIFO model | 1,600,000 |
| Exhaustive maker/taker/TIF FIFO combinations | 864 |
| Deterministic 100,000-command replay | pinned to a golden state hash |
| Engine line coverage | 97.74% |
| Memory safety | `#![forbid(unsafe_code)]`, no raw pointers |

The randomized groups are differential tests. The engine is compared against reference
models built from completely different primitives: `BTreeMap` plus plain FIFO vectors,
against the engine's bitmap ladder over a fixed slot arena. Agreement is therefore
evidence that both are right; it does not define what right means. Every operation
compares reject reason, fill sequence, queue order, BBO, level aggregates, state hash and
free-list integrity exactly.

Detail in [`docs/TESTING.md`](docs/TESTING.md).

## Repository layout

```text
README.md                  You are here
Cargo.toml                 No dependencies
rust-toolchain.toml        Pins Rust 1.97.1

src/lib.rs                 The engine
src/verify/                43 check groups and the independent reference models
  mod.rs                     Registry, runner, workload scaling
  reference.rs               BTreeMap/FIFO models sharing no code with the engine
  bitmap_l2.rs               Bitmap and L2 groups
  l3.rs                      L3 and matching-engine groups
  differential.rs            Randomized, exhaustive, and replay groups
  api.rs                     Public API surface and error formatting
src/bin/bench.rs           Self-verifying benchmark binary

docs/ARCHITECTURE.md       Layout, complexity, invariants, and design limits
docs/TESTING.md            Verification contract and coverage
docs/BENCHMARKS.md         Measurement method
```

## Behavioral contract

The engine models a continuous, visible, single-instrument price-time FIFO book. Price
priority precedes arrival priority; executions occur at resting-maker prices. An amend
that only reduces quantity retains queue position; any other change is a cancel/replace,
which assigns a new ID and new priority.
Post-only rejects a crossing order instead of repricing it.

These choices align with common exchange behavior documented by CME, Coinbase Exchange,
and Nasdaq specifications. Venue-specific auction, hidden-liquidity, routing, and
allocation rules are intentionally outside this project.

## Scope boundary

This is the auditable matching core, not a claim of a complete exchange. It excludes
gateway decoding, participant/account state, self-trade prevention, risk reservation,
sequencing services, journaling, snapshots/recovery, auctions, hidden/iceberg orders,
pegging, pro-rata allocation, multi-instrument sharding, networking, persistence, and
failover.

## What would have to change before this took real order flow

The scope boundary above is what sits *around* the engine. Below is what would have to
change *inside* it. Each one follows directly from a decision that is right for a bounded,
verifiable core and wrong for a venue:

- Order IDs are dense table indices, not 64-bit exchange or client order IDs.
- `u32` quantity and 16-bit tick prices are too narrow for a crypto venue.
- No participant identity, so no self-trade prevention, fee tiers, or per-account risk.
- No timestamps or sequence numbers; arrival priority is implicit in queue order.
- Only fills are published. With no book-delta stream there is no market data feed.
- Market orders sweep to the domain extreme, with no collar or price band.
- Panics are reachable on the matching path.
- No mass quote, cancel-on-disconnect, or good-till-date.

Reasoning and cost for each in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
