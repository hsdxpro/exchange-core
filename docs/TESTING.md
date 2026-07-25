# Verification strategy

The engine is verified by 43 named check groups: targeted unit scenarios, exhaustive bounded cases, randomized differential comparison against independently written reference models, and structural validation.

The engine is never checked against itself. It is compared to reference models built from different primitives: `BTreeMap` plus per-level FIFO `Vec`s and a `HashMap` location index, against the engine's bitmap ladder over a fixed slot arena with an intrusive free list. Agreement is therefore evidence that both are correct, not the definition of correctness.

The suite lives in `src/verify/` and is compiled into the library rather than behind `cfg(test)`. `cargo test` runs each group as a named test; `cargo run --bin bx-bench` runs the identical code, prints one `PASS`/`FAIL` line per group with a total, and refuses to report benchmark numbers if anything failed.

## Behavioral sources

The selected contract follows common professional exchange semantics:

- Price-time FIFO: best price first, then oldest order at that price.
- Trades execute at the resting maker's price.
- An amend that only reduces size preserves priority.
- A price change or size increase is a cancel/replace and receives new priority.
- IOC cancels any unfilled remainder.
- FOK either executes fully or leaves the book unchanged.
- Post-only never takes liquidity; this project rejects a crossing post-only order.

Reference material:

- CME FIFO overview: <https://www.cmegroup.com/education/articles-and-reports/overview-what-makes-ags-markets-work>
- Coinbase Exchange trading concepts: <https://docs.cdp.coinbase.com/exchange/concepts/trading>
- Coinbase FIX cancel/replace behavior: <https://docs.cdp.coinbase.com/exchange/fix-api/order-entry-messages/order-entry-messages5>
- Nasdaq OUCH 5.0: <https://www.nasdaqtrader.com/content/technicalsupport/specifications/TradingProducts/OUCH5.0.pdf>

## Test layers

### 1. Hierarchical bitmap

- Empty state and idempotent insertion/removal.
- Every individual price from 0 through 65,535 as a singleton.
- Boundaries at 63/64 and 4,095/4,096.
- Full-domain next/previous queries.
- Alternating removals from the full domain.
- 500,000 randomized insert/remove operations against `BTreeSet`.
- Root, summary, and leaf consistency validation.

### 2. L2 book

- Empty state, minimum/maximum prices, and 64-bit totals.
- Add, modify, remove, and cached-BBO transitions.
- Bid descending and ask ascending top-N ordering.
- Zero-limit traversal.
- Sweep/VWAP with zero, partial, exact, and insufficient depth.
- Exactly 1,000 sparse levels on each side.
- 600,000 randomized updates against an independent ordered model.
- Exact BBO, total quantity, top-1,000 output, sweep result, and state-hash comparison.

### 3. L3 queues and matching

- Oversized slot capacity rejected before allocation.
- Zero-capacity and empty-book behavior.
- Order ID zero, duplicate IDs, unknown IDs, and out-of-range IDs.
- Zero quantity and unsupported time-in-force on the market-order entry point.
- Boundary prices 0 and 65,535.
- Prevention of a crossed resting book.
- Strict FIFO at one price.
- Price priority across several levels.
- Buy/sell and market-order symmetry.
- Maker-price execution.
- Partial maker fills and full maker removal.
- GTC passive rest and match-then-rest.
- IOC no-fill, partial-fill, and full-fill behavior.
- FOK acceptance and atomic insufficient-liquidity rejection.
- Price-limit enforcement even when deeper out-of-limit liquidity exists.
- Post-only passive acceptance and atomic crossing rejection.
- Cancel head, middle, tail, and sole order.
- Amend down, no-op amend, amend-to-zero as cancel, and rejected amend up.
- Cancel/replace with a new ID and new queue priority.
- Cancel/replace that crosses and then rests its remainder.
- Invalid cancel/replace leaves the original untouched.
- Full-capacity passive rejection.
- Full-capacity aggressive fill and slot reuse.
- Repeated free-list recycling over 10,000 cycles.
- 32-bit maximum order quantities, 64-bit level aggregates, and safe notional arithmetic.
- Direct order lookup and ordered level/queue iteration.
- Exact sweep over 1,000 sparse prices.
- A fill callback that panics after committed fills leaves all structural invariants valid.

### 4. Independent differential model

The reference model shares no code with the engine under test: `BTreeMap` for levels, per-level `Vec` FIFO queues, and a `HashMap` location index. It reuses none of the bitmap, linked-slot or free-list implementation.

Randomized coverage:

```text
4 seeds × 250,000 operations = 1,000,000 large-capacity operations
5 capacities × 120,000        =   600,000 capacity-pressure operations
Total randomized L3 commands = 1,600,000
```

`--quick` (and a debug-profile `cargo test`) divides the randomized counts by 50 so an interactive run stays under a second; the deterministic and exhaustive groups are never reduced.

The stream includes passive adds, cancels, amends, cancel/replaces, GTC, IOC, FOK, post-only, invalid IDs, duplicate IDs, zero quantities, maximum quantities, crossed and non-crossed limits, full-capacity books, and both sides.

Every command compares return status and exact fill sequence. At regular checkpoints it also compares:

- best bid and ask;
- live-order count;
- every occupied level and aggregate quantity;
- exact FIFO order IDs and quantities;
- deterministic state hash;
- all internal engine invariants.

### 5. Exhaustive bounded FIFO matrix

Three resting FIFO makers each take every quantity from 1 through 3. Taker quantity ranges from 1 through total available quantity plus 2, under all four limit-order TIF modes.

```text
864 complete engine/reference scenarios
```

### 6. Deterministic replay

A fixed 100,000-command pseudo-random stream is run twice. Command-result hash, final state hash, and live-order count must match exactly.

## Structural invariant audit

`L3Book::validate()` checks:

- bitmap hierarchy consistency;
- cached BBO consistency;
- no crossed resting book;
- queue head/tail and bidirectional links;
- every live slot appears in exactly one queue;
- every live ID maps to its exact slot;
- every used slot is either live or on the free list, never both;
- no free-list cycle or duplicate slot;
- level aggregate equals sum of queued quantities;
- live count and slot accounting match.

## Toolchain gates

```text
rustc 1.97.1: #![forbid(unsafe_code)], rustfmt --check,
              clippy --all-targets -D warnings
```

Rust engine coverage is 908 of 928 executable lines in `src/lib.rs` (97.74%; 98.11% of regions, 98.75% of functions). Nineteen of the twenty uncovered lines are `return false` arms inside `L3Book::validate()` and `L2Book::validate()`, the invariant-failure branches that can only be reached by deliberately corrupting private state. The twentieth is an unreachable `else` guard in `HierarchicalBitmap::next` that would only matter if the summary tier exceeded 64 words. Reproduce with:

```bash
cargo llvm-cov --release --locked --summary-only
```

(Requires `cargo install cargo-llvm-cov` and the `llvm-tools-preview` component; neither is a build dependency of the engine.)

