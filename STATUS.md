# Where this project is

Working notes: what we are building, what exists, what was decided and why, and
what comes next. [`DESIGN.md`](DESIGN.md) is the architecture; this is the state
of play.

---

## 1. Objective

Build a production-shaped crypto exchange around the existing matching engine.
Binary protocol, reliable transport open to the public internet, 1000+ symbols,
replicated durability, automatic failover, deterministic crash recovery, and a
subscription feed that clients can resume after a disconnect.

Guiding constraints, in the user's words: **best modern practice, no invented
constants, no overcomplication, least layers and code that still meets the
requirement, everything runnable locally with no CI.**

---

## 2. Repository layout

Three folders on disk, deliberately separate:

| Path | What it is | Touch it? |
|---|---|---|
| `D:\Code\matching_engine` | This project. Git repo, active work. | Yes |
| `D:\Code\bitmap-exchange` | The finished engine, zipped and sent as a job-application code sample. | **No — frozen** |
| `D:\Code\bitmap-exchange-cpp` | The C++23 cross-implementation, kept for reference. | Reference only |

Two branches:

- **`matching_engine`** — one commit, the engine exactly as it was sent.
- **`matching_engine_with_subscription`** — current work. All commits below.

```
bea8c29  Bitmap-ladder matching engine and order book
3f6195e  Design for the subscription exchange
5902386  Resolve the remaining design decisions
4b19a7f  Cut the invented complexity from the design
0092e1f  Workspace, wire protocol, and journal
3658d4b  Exchange pipeline, end-to-end tests, and a local runner
```

### Crates

```
crates/engine/     bx-engine     the matching engine. ZERO dependencies,
                                 forbid(unsafe_code). Keep it that way.
crates/protocol/   bx-protocol   wire types. Depends only on zerocopy.
crates/journal/    bx-journal    append-only log + replay.
crates/pipeline/   bx-pipeline   sequencer, accounts, books, events.
xtask/             xtask         local task runner. Replaces CI.
```

---

## 3. What exists and is tested

**107 tests pass.** `cargo x` runs fmt, clippy (`-D warnings`), and everything.

| Crate | Tests | Covers |
|---|---|---|
| `bx-engine` | 44 | The engine's own suite, unchanged from the shipped version. |
| `bx-protocol` | 6 | Record round-trip, 64-byte layout, unknown discriminants, short buffers. |
| `bx-journal` | 15 | Append/replay, torn writes, crash before sync, corruption, sequence gaps, device failure, and the same against a real file. |
| `bx-pipeline` | 28 | Instrument price mapping, balances and reservation, engine adapter, deltas, hashing. |
| end-to-end | 14 | Full path with simulated traders and a subscriber. |

### The end-to-end tests matter most

`crates/pipeline/tests/end_to_end.rs`. **Nothing is faked.** Simulated traders
send real limit and market orders through the real API; the real engine matches
them; a `Subscriber` that knows *only* the event stream rebuilds the book from
deltas alone. The assertion is that its reconstruction equals the venue's actual
book.

A 2,000-command session runs across four seeds and asserts:

- subscriber's depth == the venue's depth, both sides
- zero gaps in the event sequence
- asset supply conserved (trading moves value, never creates it)
- the session actually traded

Plus real journal replay (take the storage, build a fresh exchange over it, call
`recover()`, compare) and crash-before-sync recovery.

### Measured latency

`cargo x latency`. Full path per command: sequence, journal append and sync,
reserve, match, emit. In-memory journal, so no real disk.

| Path | first cut | now |
|---|---|---|
| passive limit order | 432 ns | **117 ns** |
| crossing order, one fill | 372 ns | **75 ns** |
| cancel by order id | 298 ns | **118 ns** |
| mixed stream | 500 ns | **169 ns** |

Roughly 6M commands/sec on the mixed stream, up from 2M.

These drift by 10-20% between sessions on this desktop even for an unchanged
binary, which is why the benchmark reports the minimum of seven runs and why no
change is kept on a single-digit-percent showing.

Three changes got there. The first was the one asked for, the second was the
one that mattered, and the third was a bug fix that happened to help:

1. **Zero allocation on the command path.** `Outcome` is a caller-owned buffer
   that `Exchange` reuses; `Book` writes into it. Capacity settles at the
   high-water mark of real traffic rather than a guessed reserve.
2. **A fast hasher for integer keys.** The command path does about seven map
   lookups, all keyed by integers. `SipHash` costs 20–30 ns each, which was most
   of the budget. `FastMap` uses the FxHash finalizer instead, and
   `reservations` moved from `BTreeMap` to a hash map since nothing iterates it.
3. **Publishing only levels that actually changed.** The order's price was
   touched speculatively before matching, so every command paid for level
   lookups and events it did not need — and a market order, which addresses the
   ladder extreme, published two deltas for a price no book occupies.

### Against a real disk, which is the number that actually matters

Everything above uses an in-memory journal. On a real file, per command:

| batch size | per command | commands/sec |
|---|---:|---:|
| 1 | **3,098,161 ns** | 322 |
| 16 | 198,256 ns | 5,000 |
| 256 | 15,168 ns | 66,000 |
| 4,096 | **3,935 ns** | 254,000 |

One `fsync` costs about 3 ms here. The in-memory figure of 142 ns is **twenty
thousand times smaller** than the real cost of an unbatched command, so all the
micro-optimisation above is noise next to durability. Batching turns 3.1 ms into
3.9 µs — a 787× improvement — which is the design's claim that batching is
mandatory rather than an optimisation, now measured.

Two consequences worth carrying:

- **Even at batch 4,096 this is 254k commands/sec, not millions.** Local `fsync`
  cannot reach the target on this hardware. Windows `FlushFileBuffers` is
  unusually slow and Linux NVMe would do far better, but the shape holds.
- **Replicating to memory on two other machines is probably faster than an
  fsync to local disk.** A LAN round trip is 10–50 µs against 3 ms here. That is
  precisely why real venues reach quorum over the network rather than waiting on
  a platter, and it makes the openraft path a throughput decision as well as a
  fault-tolerance one.

---

## 4. Decisions already made

These were argued out. Do not relitigate without new information.

### Architecture

- **Not monolith vs services.** Matching one symbol is inherently single-writer
  and cannot be distributed. Deterministic core, services around it.
- **Everything after the sequencer is deterministic.** No clock reads, no
  randomness, no `HashMap` iteration reaching output. This is what makes replay,
  hot standby and reproducible debugging work. It constrains every future
  feature.
- **Ack after the journal reaches quorum**, not after matching. An ack that has
  to be retracted is worse than one that took 30 µs longer. Research confirmed
  this is standard venue practice.
- **Accounts shard by account ID, books shard by symbol**, both behind one
  sequencer. This is what makes cross-symbol balance reservation atomic without
  a distributed lock. Tested.

### Rejected, with reasons

- **Windowed ladder with re-centring — deleted.** It solved a memory problem
  that does not exist: 1000 symbols is ~2 GB of level tables on a machine with
  256 GB+. An invented crisis with an invented 4,096 constant and a re-centring
  mechanism to cover for it. The engine is unchanged.
- **Aeron** — mature and the right answer for reliable UDP in trading, but Rust
  access is a third-party wrapper over the C API plus a separate media driver
  process. Too heavy. Revisit only if the custom transport becomes the
  bottleneck.
- **`monoio`** — lags io_uring feature parity, little recent maintenance.
- **`glommio`** — interesting dedicated latency ring, small ecosystem, and we do
  not want async on the hot path anyway.
- **`raft-rs`** — in maintenance mode as of 2026; new work is pointed at
  `openraft`.
- **`clippy::pedantic`** — dropped from the workspace. The engine was already
  clean under `clippy::all`; retrofitting a stricter level is churn.

### Chosen stack

| Concern | Choice |
|---|---|
| Matching path | pinned threads, busy-poll, **no async runtime** |
| Between stages | SPSC ring buffers |
| Gateway / fanout | `tokio` + `quinn` (connection-bound tier, maturity wins) |
| Consensus | `openraft`, **batching mandatory** — 33k writes/sec unbatched, 5.6M batched |
| Journal I/O | `io_uring`, SQPOLL |
| Encoding | hand-rolled fixed-layout structs via `zerocopy` |
| Transport | QUIC on UDP/443 for everyone; raw UDP/shm for colo later |

### Product rules

- **Fees never touch matching.** Price-time priority on the raw limit price;
  fees apply at settlement. Fee-adjusted matching would couple the engine to
  account state and break determinism.
- **Self-trade prevention: cancel-newest** by default. Protects resting
  liquidity. Matches CME SMP and Binance `EXPIRE_TAKER`. Needs an owner on the
  order — **not yet implemented.**
- **MBP public, MBO colocated only.** MBO leaks trading patterns and is 10–100×
  the volume.
- **An instrument's ladder range IS its price band.** The memory bound and the
  fat-finger control are the same mechanism. Implemented in
  `instrument.rs::to_slot`.

### Things learned the hard way

- **A journal that cannot recover from a torn write is not durable at all.**
  Replay stopped at the tear correctly, but the partial record was left in
  place, so the next append landed after it and every later record was
  unreachable. `Journal::open` now truncates to the last intact record. The
  component whose only job is fault tolerance had no test against a real file.
- **`File::try_clone` shares the file offset on Unix.** Seeking the clone seeks
  the original, so a read during replay would move the append cursor and the
  next write would land mid-log. It happens to be harmless on Windows, so no
  local test could have caught it.
- **Benchmarks on this desktop swing 2–4× between runs of the same binary.**
  Use minimum-of-N, never a single run. A supposed 9–12% improvement turned out
  to be **two byte-identical binaries** — always checksum A/B artifacts.
- **AWS has no usable multicast.** Transit Gateway multicast measures ~0.4 ms;
  native L2/L3 exists only on Outposts racks.
- **AWS Nitro gives hardware packet timestamps** (64-bit ns, at the NIC, before
  the kernel) plus a PTP hardware clock. Clears MiFID II without dedicated
  timing hardware.

---

## 5. Known debt

Ordered by how much it matters.

1. **No owner on the order**, so self-trade prevention cannot be implemented.
   Requires `OrderSlot` to grow from 24 to 32 bytes in the engine.
3. **Deposits are not journalled.** Balances are not recovered by replay; tests
   re-apply them manually. Needs a `Deposit` command kind.
4. **`CancelReplace` is cancel-then-submit** and emits both sets of events. It
   works, but a client sees a `Canceled` it did not ask for.
5. **`market_order` uses `Ticks::MIN` as its sentinel.** Works, but a real
   protocol should carry an explicit order-type field.
6. **Book capacity is a hardcoded 65,535** per symbol in `pipeline/src/lib.rs`.
   Should come from the instrument definition.
7. **openraft's reported 40 ms blocking issue is unverified** against our
   batching pattern. Measure before committing to it on the data path.

---

## 6. Next steps, in order

The disk measurement reordered this list. Per-command compute is no longer the
constraint, so **openraft moved up from last to second**: reaching a quorum in
memory over a LAN is two orders of magnitude faster than an fsync, which makes
replication a throughput decision, not only a fault-tolerance one.

1. **Subscription channels.** `book.{symbol}`, `trades.{symbol}`, `bbo.{symbol}`,
   and the private per-account channels. Per-channel sequence numbers, a
   fixed-size ring buffer, and `RESUME {channel, last_seq}` that replays from the
   ring or forces a fresh snapshot. The `Subscriber` in the e2e tests already
   models the client side of this.
4. **Snapshots.** Serialize book plus balances at a sequence, so recovery does
   not replay from zero. Cadence derived from a recovery-time target, not picked.
5. **QUIC gateway.** `quinn`, binary framing, session handling. **The e2e
   scenarios were written transport-agnostic on purpose** — the same assertions
   should run over real sockets.
6. **Multi-process local tests.** Separate gateway and core processes, real
   sockets, same scenarios.
7. **Deterministic simulation.** Fake clock and lossy network behind the traits
   that already exist, whole cluster in one process, failure injection,
   reproducible from a seed.

---

## 7. How to run

```bash
cargo x            # fmt check, clippy -D warnings, all tests
cargo x test       # tests only
cargo x e2e        # end-to-end, with output
cargo x latency    # pipeline latency
cargo x engine     # the engine's own 43 checks and benchmark
cargo x all        # everything
```

Requires rustup only. `rust-toolchain.toml` pins 1.97.1 and rustup fetches it.
There is no CI and nothing to install.

---

## 8. Working preferences

Established over this project:

- Plain language over jargon. Explain the concept before the term.
- No invented constants. Derive them, measure them, or say they are arbitrary.
- Own mistakes plainly and fix them; do not defend a bad call.
- Report what is verified separately from what is assumed.
- Keep the engine crate dependency-free and `forbid(unsafe_code)`.
