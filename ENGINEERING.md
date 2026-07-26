# Engineering notes

Why the code is shaped the way it is: the decisions that were argued out, the
alternatives that were rejected and on what grounds, and the bugs that changed
the design. [`README.md`](README.md) is the entry point and
[`DESIGN.md`](DESIGN.md) is the architecture.

The measurements below were taken on one desktop and drift by a factor of two or
more on a loaded machine, so they are reported as minimum-of-N and any A/B
comparison here was run interleaved against checksummed binaries. A single
timing is not evidence of anything.

## What exists, and what proves it

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

## Decisions

Each of these was settled against a specific alternative, noted below.

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
- **A subscriber cannot build a book out of increments.** It has no idea what
  was resting before it arrived. Every end-to-end test happened to subscribe
  while the book was empty, where empty-plus-increments is accidentally correct,
  so this went unnoticed: a client joining a live venue could never construct the
  book at all. Subscribing now sends the current levels as `BookSnapshot` stamped
  with the sequence the increments resume from, so state and change compose
  exactly.
- **A cursor advances whether or not the client reads.** Which means falling
  outside the retention window is nearly unreachable, and the real overload path
  is the per-session outbox budget -- that sheds the connection. Both are fine
  now that reconnecting restates the book; before, a shed client came back to a
  stream of increments against a book it no longer knew.
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

### Bugs that changed the design

- **Replication had no notion of leadership.** A follower accepted whatever any
  leader sent, so a partitioned leader that had been replaced would keep
  appending and two leaders would acknowledge orders into divergent logs.
  Election was not the first thing missing; safe promotion was. Groups now carry
  the leader's term and a follower refuses anything older, replying with the term
  that won so the stale leader stops.
- **A stream per channel is not a stream per channel if one task writes them
  all.** The QUIC session dispatched every stream from a single loop, so a feed
  stalled on flow control held up the acknowledgements queued behind it -- exactly
  the head-of-line blocking separate streams exist to prevent, rebuilt one layer
  up. The test written for the property caught it on its first run; every other
  test passed. It is now a writer task per stream and the dispatcher never awaits
  a write.

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

## Known debt

Ordered by how much it matters.

4. **`CancelReplace` is cancel-then-submit** and emits both sets of events. It
   works, but a client sees a `Canceled` it did not ask for.
7. **openraft's reported 40 ms blocking issue is unverified** against our
   batching pattern. Measure before committing to it on the data path.

---

## What is left

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

