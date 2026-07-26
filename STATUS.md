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
crates/journal/    bx-journal    append-only log, replay, replication.
crates/pipeline/   bx-pipeline   sequencer, accounts, books, events, snapshots.
crates/gateway/    bx-gateway    framing, sessions, group commit, TCP server,
                                 config. Binaries: venue, load.
xtask/             xtask         local task runner. Replaces CI.
```

---

## 3. What exists and is tested

**211 tests pass.** `cargo x` runs fmt, clippy (`-D warnings`), and everything.

| Crate | Tests | Covers |
|---|---|---|
| `bx-engine` | 44 | The engine's own suite. |
| `bx-protocol` | 11 | Record layout, discriminants, order type, subscription channels. |
| `bx-journal` | 22 | Append/replay, torn writes, crash before sync, corruption, device failure, real files, and replication quorum. |
| `bx-pipeline` | 57 | Prices, balances, engine adapter, deltas, hashing, snapshots, and the crossable walk's complexity. |
| `bx-gateway` | 24 | Framing, the group-commit loop, config parsing and validation. |
| end-to-end | 19 | Full path with simulated traders and a subscriber. |
| subscription | 7 | Channels, resume after disconnect, lagging out of the window. |
| snapshot | 6 | Snapshot/restore equality with a full replay, queue priority. |
| simulation | 4 | Seeded crash injection, torn writes, dead device, replay determinism. |
| over sockets | 9 | Real TCP: split records, disconnects, bursts, selective subscription. |
| venue snapshots | 7 | Cadence from a recovery target, atomic replace, corrupt snapshot refused. |

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

`cargo x latency`, minimum of five runs, whole run in 6.5 s. Full path per
command: sequence, journal append, durability, reserve, match, emit.

| Path | ns |
|---|---:|
| passive limit order | 157 |
| crossing order, one fill | 159 |
| crossing, self-match check running | 162 |
| cancel by order id | 75 |
| mixed stream | 161 |
| mixed stream, three subscribers attached | 160 |
| mixed stream, batch 64 | 142 |

Two of those are worth reading twice. **Fan-out to three channels is free** at
this resolution -- 160 against 161 -- because publishing is a bounded ring write
and nothing else. And **self-match prevention costs 3 ns** on the crossing path,
because an account with nothing resting answers the question in one hash lookup
and never touches the book.

These figures drift 2-3x on a loaded machine. The numbers above are from a quiet
one; anything measured while a build is running is worthless.

### Durable throughput, which is the number that answers "can it keep up"

| group | local fsync | quorum on loopback |
|---|---:|---:|
| 1 | 318 cmd/sec | 19,644 cmd/sec |
| 16 | 5,128 | 274,296 |
| 256 | 78,471 | 1,974,452 |
| 4,096 | 1,080,376 | 5,299,407 |
| 16,384 | **2,975,883** | **6,222,727** |

**Millions per second, durably, is reached.** At a group of 16,384 the quorum path
costs 161 ns per command, which *is* the compute cost from the table above:
durability has become free, and the venue is bound by matching again.

Getting there needed a fix, not a bigger batch. Group commit was half-done: one
sync per group, but `FileLog::append` still issued one `write` syscall per
64-byte record. A group of sixteen thousand commands made sixteen thousand
syscalls, and they dominated so completely that throughput plateaued near 345,000
a second no matter how large the group grew. Appends are now buffered and written
once per sync, which is worth **8.6x** at that group size.

The trade a group size buys is latency for throughput: the first command in a
group waits for the last. Nothing picks the size -- the group is whatever arrived
since the previous pass, so it grows under load exactly when throughput matters
and falls to one when the venue is idle and latency does.

### What a client actually experiences

`venue` and `load` as separate processes over loopback TCP.

| | one pipelined client | 32 concurrent clients |
|---|---:|---:|
| durable file journal | 662,951 orders/sec | **928,943 orders/sec** |
| in-memory journal | 2,962,954 orders/sec | 1,745,152 orders/sec |

Concurrency helps exactly where there is a sync to amortise and costs where
there is not. On the durable path more clients mean larger groups per pass, so
32 of them beat one pipelined client by 1.4x. With the journal in memory there
is nothing to amortise and 32 sockets only add syscalls, so the same test runs
at 0.6x. That is the group-commit design working, and it is the reason nothing
in the code picks a batch size.

Round trip, one order in flight: **9.0 us** in memory, **3,066 us** against a
real file. The second is one fsync and matches the durability table above. The
first is **not a durable number** and must not be quoted as one; the durable
answer for a single order in flight is quorum, at 51 us.

### Restart time

| | |
|---|---:|
| replay all 100,000 commands | 12.0 ms |
| snapshot + replay the last 5,000 | **1.3 ms** |

A 9x saving with 2,002 orders in the snapshot. What it saves depends entirely on
the ratio between the journal and the resting book, since restoring costs one
insert per resting order: a journal where nothing is ever cancelled saturates the
book and the snapshot saves almost nothing.

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
- **Self-trade prevention: cancel-newest.** Protects resting liquidity; matches
  CME SMP and Binance `EXPIRE_TAKER`. Implemented without widening the engine's
  order record: the pipeline already knows every resting order owner, so the
  question is answered above the engine. Guarded by a per-(account, symbol)
  resting count, so an account with nothing resting -- almost every taker --
  answers it in one lookup and never touches the book.
- **MBP public, MBO colocated only.** MBO leaks trading patterns and is 10–100×
  the volume.
- **An idle connection is not free, and the active clients pay for it.** Reading
  every socket every pass measured **422 ns per idle session**, perfectly linear,
  and it lands in the same pass as real orders -- 10,000 connections would have
  put 4.25 ms in front of every order, invisibly. Sessions are now found by
  readiness (`mio`: epoll, IOCP or kqueue), which took the marginal cost to
  **23 ns**, an 18x reduction, and the residue is userspace rather than syscalls.
  A ceiling still exists because the cost is still linear, so `max_sessions`
  refuses past it and counts the refusals.
- **Edge-triggered readiness means reading until `WouldBlock`, not once.** The
  loop deliberately reads one buffer per session per pass, for fairness. With
  edge triggering that is not enough on its own: a socket is reported readable
  once and not again until exhausted, so a session that stopped mid-buffer went
  silent forever. It now stays in the ready set until a read actually returns
  `WouldBlock`, which keeps the fairness bound and costs one extra read after a
  session's last data.
- **A per-channel cost multiplies by the instrument list.** The retention window
  is 64 bytes an event per channel and there are two public channels per symbol,
  so the shipped default of 65,536 wanted 7.8 GiB across a thousand
  instruments -- an out-of-memory kill, from following the example config. The
  default is now 8,192 (1 GiB at a thousand symbols) and the configuration
  refuses to start when the window times the instrument list exceeds a stated
  budget.
- **A dense table is only cheap if its index is bounded.** Instruments live in a
  table indexed by symbol, which makes lookup a bounds check and an offset. It
  also means one instrument numbered 4,294,967,295 asks for a four-billion-entry
  table, about 171 GB, from a single mistyped configuration line. Symbol IDs are
  venue-assigned, so numbering them densely from zero costs nothing;
  `MAX_SYMBOL` makes that a refusal instead of a kill.
- **Price levels and order slots are different things and must not share a
  number.** The book has 65,536 *price levels* because that is the bitmap
  ladder, and that is the design. It separately needs a pool of *resting order
  slots*, because the engine addresses orders by dense index to keep insert and
  cancel O(1) with no allocation. That pool used to be a `DEFAULT_BOOK_CAPACITY`
  of 65,535 sitting in `lib.rs` — a magic number that, by coinciding with the
  ladder size, read as though the book could only hold 65,535 prices. It is now
  `Instrument::max_open_orders`, declared per instrument. A benchmark had
  already saturated it silently, so every order past the limit was being
  rejected and the measurement was meaningless. Proving there is no per-level
  limit then turned up a second bug: the slot allocator returned `None` for
  both a duplicate order ID and an exhausted pool, and the caller reported both
  as `DuplicateOrderId`. A full venue therefore told every client "order ID is
  already live", which a client answers by retrying with a fresh ID forever.
  `OrderLimitReached` existed and was never emitted.
- **An instrument's ladder range IS its price band.** The memory bound and the
  fat-finger control are the same mechanism. Implemented in
  `instrument.rs::to_slot`.

### Things learned the hard way

- **The hot path was making an fsync per command.** `submit` appended, synced
  and only then matched, so an operation costing milliseconds sat directly in
  front of one costing nanoseconds -- eighteen thousand times the cost of the
  thing it was protecting. Split into `enqueue` (no sync, events buffered) and
  `commit` (one sync, events released). `submit` and `submit_batch` are now
  three-line wrappers over the pair, so the two durability paths cannot drift.
- **A restart that recovers every order but none of the money is not a
  restart.** Deposits lived only in memory, so replay rebuilt the books and left
  the balances at zero. It looked correct only because every recovery test
  re-applied the deposits by hand before asserting. `CommandKind::Deposit` is
  now journalled like anything else and those workarounds are gone.
- **A swallowed error is how a money bug hides.** `settle` skipped an execution
  with a bare `continue` when a value it needed was missing, which would drop
  one side of a trade the engine had already matched and published. Every
  accounting call that "cannot fail" now increments a counter the end-to-end
  tests assert is zero.
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

4. **`CancelReplace` is cancel-then-submit** and emits both sets of events. It
   works, but a client sees a `Canceled` it did not ask for.
7. **openraft's reported 40 ms blocking issue is unverified** against our
   batching pattern. Measure before committing to it on the data path.

---

## 6. What is left

Two items, both deliberate rather than forgotten:

1. **Leader election.** Quorum durability is implemented and measured;
   automatic failover is not. It needs consensus, hand-written consensus is how
   distributed systems lose data quietly, and the right answer is `openraft`.
   `ReplicatedLog` is the boundary where it belongs.
2. **QUIC in place of TCP.** The transport is TCP. Swapping it is a `tcp.rs`
   replacement -- `venue` and `codec` do not change -- which is why they are
   separate.

Smaller, if wanted: MBO for colocated clients, fee schedules at settlement,
per-account rate limits.

## 7. Earlier plan, now done

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
