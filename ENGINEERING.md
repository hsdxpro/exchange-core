# Engineering notes

Why the code is shaped the way it is: the decisions that were argued out, the
alternatives that were rejected and on what grounds, and the bugs that changed
the design. [README.md](README.md) is the entry point,
[ARCHITECTURE.md](ARCHITECTURE.md) the code map, [DESIGN.md](DESIGN.md) the
design reasoning, [PROTOCOL.md](PROTOCOL.md) the wire, and
[BENCH.md](BENCH.md) the numbers with their methodology.

The measurements below were taken on one desktop and drift by a factor of two or
more on a loaded machine, so they are reported as minimum-of-N and any A/B
comparison here was run interleaved against checksummed binaries. A single
timing is not evidence of anything.

## What exists, and what proves it

**476 tests pass.** `cargo x` runs fmt, clippy (`-D warnings`), everything in
debug, and the engine suite again in release — debug runs the Quick workload,
and only Full checks the golden replay hash and drives the randomized
differentials at depth.

| Crate | Tests | Covers |
|---|---|---|
| `bx-engine` | 47 | 46 named verify check groups (differential vs. independent models, exhaustive TIF/quantity combinations, randomized workloads, the golden replay hash) plus the check registry itself. |
| `bx-protocol` | 16 | Record layout, discriminants, field aliasing, subscription channels. |
| `bx-journal` | 36 | Append/replay, torn writes, crash before sync, corruption, device failure, real files, replication quorum and term fencing, partitions. |
| `bx-pipeline` | 169 | Books, balances, hub, snapshots, instruments in `src`; end-to-end, chain, risk, BBO, subscription, watermark, snapshot and crash simulation in `tests/`. |
| `bx-gateway` | 208 | Config, the Ed25519 challenge, token buckets, codec, handoff, multicast, metrics, TLS in `src`; admission, over-TCP, over-feed, over-TLS, venue snapshots, shipped binaries, failover, machine-down, idle cost and many-clients over real sockets and processes in `tests/`. |
| `bx-election` | 4 | Three nodes electing one leader, a higher term after a death, a minority electing nobody, and a vote that survives a restart. |
| `xtask` | 1 | The task-runner's own string hygiene. |

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
| passive limit order | 188 |
| crossing order, one fill | 224 |
| crossing, self-match check running | 187 |
| cancel by order id | 106 |
| mixed stream | 171 |
| mixed stream, three subscribers attached | 182 |
| mixed stream, batch 64 | 171 |

The crossing and cancel rows read **3,036 ns and 447 ns** until the per-account
open-order index stopped being searched linearly; the table above is after.

A row worth adding on its own: **a market order sweeping 2,000 distinct levels
cost 562 ns a level and now costs 176**, measured interleaved across three runs.
Every crossing benchmark before it put its makers at a *single* price, so it
measured the fill loop and nothing about breadth — and breadth is where per-level
bookkeeping that looks free at one level stops being free. The touched-price list
was deduplicated by searching it, once per fill, which is quadratic in the levels
crossed. It now compares against the last entry, which is sufficient because the
engine sweeps best-first and finishes each level before moving on. That
assumption has its own test, because breaking it would produce a duplicate
restatement rather than a wrong book — wasteful, invisible, and exactly the kind
of thing that survives a green suite.

**Fan-out to three channels costs 11 ns**, because publishing is a bounded ring
write and nothing else. An earlier version of this table reported it as free,
which was true only at the resolution of a noisier machine — it is small, not
absent, and saying "free" invites somebody to attach a thousand.

These figures drift 2-3x on a loaded machine. The numbers above are from a quiet
one; anything measured while a build is running is worthless.

### Durable throughput, which is the number that answers "can it keep up"

| group | local fsync | quorum on loopback |
|---|---:|---:|
| 1 | 321 cmd/sec | 15,596 cmd/sec |
| 16 | 4,779 | 237,830 |
| 256 | 77,989 | 1,477,527 |
| 4,096 | 957,952 | 5,390,966 |
| 16,384 | **2,344,665** | **4,646,905** |

**Millions per second, durably, is reached.** At a group of 16,384 the quorum path
costs 215 ns per command, which is roughly the compute cost from the table above:
durability has become nearly free, and the venue is bound by matching again.

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

All three durability shapes, 200,000 orders each, measured in one sitting so the
rows are comparable with each other rather than with an earlier machine.

| durability | round trip, one in flight | pipelined |
|---|---:|---:|
| in-memory journal | 11.4 us | 1,796,009/sec |
| durable file journal | 357 us | 1,560,998/sec |
| quorum of two followers | **66.7 us** | **1,785,257/sec** |

The round-trip column is where the design's argument lives: with one order in
flight there is nothing to amortise a sync over, and reaching two other processes
is 5.4x faster than reaching the platter. The pipelined column is where it stops
mattering — once the group is thousands of orders the sync is shared by all of
them and every row converges on what matching costs.

The in-memory round trip is **not a durable number** and must not be quoted as
one; the durable answer for a single order in flight is the quorum, at 66.7 us.

With eight concurrent clients against the replicated cluster: 1,532,482
orders/sec, with the leader intact — the same load that used to stop it before a
dropped follower could rejoin.

### Restart time

| | |
|---|---:|
| replay all 100,000 commands | 12.8 ms |
| snapshot + replay the last 5,000 | **2.0 ms** |

A 6.4x saving with 2,002 orders in the snapshot. What it saves depends entirely
on the ratio between the journal and the resting book, since restoring costs one
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

- **QUIC — built, measured, removed.** A stream per channel does solve real head-of-line
  blocking: acknowledgements and market data stop sharing a queue, so a slow feed reader no
  longer backs up its own fills. It also reaches clients behind NAT and survives mobile
  handoff. Measured against the same venue, same journal, same traffic:

  | | TCP | QUIC |
  |---|---:|---:|
  | round trip, one order in flight | **8.6 us** | 38.6 us |
  | pipelined throughput | **3.76M/sec** | 1.48M/sec |

  4.5x the latency and 40% of the throughput, for properties a venue serving market makers
  does not need. TLS 1.3 is not optional in QUIC either -- packet protection is part of the
  transport, so there is no unencrypted mode to fall back to, and a market maker on a
  cross-connect wants nothing between it and the book. Removing it also deleted a second copy
  of every session feature, which is where a session leak had already lived in one transport
  and not the other. The consequence kept: one socket per session means the outbox bound and
  shedding a slow client are load-bearing rather than belt-and-braces.

- **Sharding the matching stage across threads — measured, then rejected in
  favour of partitioning across processes.** The goal is real: at a thousand
  instruments a command costs 331 ns against 183 at one, and after the book
  lookup was fixed the remainder is cache — a thousand books is a thousand bitmap
  ladders, and one core keeps none of them warm. Giving each core its own working
  set is the answer.

  In-process sharding is the wrong way to get it, for three reasons.

  **Amdahl, structurally.** Stage one owns balances, and an account trades many
  symbols, so it cannot be sharded *by symbol* at all. Every command passes
  through it in sequence. Sharding only the books therefore buys about 2x before
  the serial stage dominates, and going past that needs an account-shard to
  symbol-shard crossbar with a sequence-ordered merge.

  **It costs the property the rest of the system is built on.** "Everything after
  the sequencer is a deterministic function of the sequenced stream" is what makes
  replay a recovery mechanism rather than an approximation. Concurrent shards
  merging back have to be reordered by sequence, which either reintroduces a
  barrier that eats the gain, or becomes a considerably harder thing to prove
  than the thing it replaced. Replay, snapshots, the golden hash and the seeded
  crash simulation all rest on that property.

  **A venue already owns exactly the instruments its configuration lists**, so
  running four of them over disjoint symbol sets partitions the listing across
  four cores today, with no new code. Each partition keeps single-writer
  determinism *exactly* — a partition is a single venue — and each gets its own
  journal, its own replication, its own leadership and its own failure domain.
  One partition crashing does not stop the others, which threads in one process
  can never offer. That is also how venues are actually run.

  What partitioning does not solve is an account trading symbols in two
  partitions, because its balance lives in one place. That is a real problem and
  a real design — a position service, or partitioning accounts as well as
  symbols, or pre-allocated buying power per partition — and it is a much larger
  question than "shard the matching thread". Pretending a thread pool answers it
  would be the wrong kind of progress.

- **Windowed ladder with re-centring — deleted, then returned for a different
  reason.** The first draft windowed the ladder to cut memory, a problem that
  does not exist (1000 symbols is ~2 GB of level tables), and was deleted.
  The window that ships solves *range*: the price domain is 31 bits per
  instrument instead of a fixed 65,536 ticks, and the level tables cover the
  slice where prices rest — boot at the old 2 MiB, re-anchor free on an empty
  book, extend upward without a rebuild, shift downward one copy per
  doubling. Slots store absolute prices so growth moves no order, and a book
  that stays inside the boot window hashes identically to the fixed ladder —
  the golden replay's state hash did not move when this landed.
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

Two columns, because a table that does not separate what runs from what is
planned cannot be checked against the code.

| Concern | Built | Intended |
|---|---|---|
| Matching path | one thread, **no async runtime** | pinned, busy-poll, symbol shards |
| Between stages | one process, direct calls | SPSC ring buffers |
| Encoding | hand-rolled fixed-layout structs via `zerocopy` | — |
| Transport | TCP, unencrypted | raw UDP or shm for colo |
| Consensus | quorum + term fencing on the data path, `openraft` electing the leader | — |
| Journal I/O | buffered `std`, one write and one sync per group | `io_uring`, SQPOLL |

QUIC was in this table's Built column while the code had none, which is the
failure mode a single-column table invites. It was built, measured and removed
for the reasons above; nothing here should read as shipped until it is.

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
- **An idle connection costs nothing, and it took two goes to get there.** The
  cost lands in the same pass as real orders, so whatever an idle connection
  costs, every active client pays it.

  Reading every socket every pass measured **422 ns per idle session**, perfectly
  linear: 10,000 connections would have put 4.25 ms in front of every order,
  invisibly. Finding sessions by readiness (`mio`: epoll, IOCP or kqueue) took
  that to **16 ns**.

  Still linear, though, and that was still a ceiling — it was simply a higher
  one, which is why it survived. The remaining cost was the *write* half: every
  pass asked every session about every channel it followed, whether or not
  anything had happened on it. The hub now reports which channels a group
  touched and the gateway holds an index from channel to session, so a
  connection with nothing to say and nothing to be told is not visited at all.

  Measured over 256 idle connections: **5,400 ns a pass before, 1,300 ns after,
  and a marginal cost per connection of zero**. The pass no longer grows with the
  connection count at all. `max_sessions` still refuses past its limit and counts
  the refusals, but it now bounds descriptors and memory rather than time.

  The index has to be given back when a session goes, and getting that wrong
  produces no wrong events -- the write pass checks the session really follows
  the channel -- so it needed a test that watches the index itself across
  connection churn rather than one that watches for bad output.
- **Edge-triggered readiness means reading until `WouldBlock`, not once.** The
  loop deliberately reads one buffer per session per pass, for fairness. With
  edge triggering that is not enough on its own: a socket is reported readable
  once and not again until exhausted, so a session that stopped mid-buffer went
  silent forever. It now stays in the ready set until a read actually returns
  `WouldBlock`, which keeps the fairness bound and costs one extra read after a
  session's last data.
- **A per-channel cost multiplies by the instrument list.** The retention window
  is 64 bytes an event per channel and there are three public channels per
  symbol (book, trades, bbo), so the old default of 65,536 wanted ~11.7 GiB
  across a thousand instruments -- an out-of-memory kill, from following the
  example config. The default is now 8,192 (~1.5 GiB at a thousand symbols) and
  the configuration refuses to start when the window times the instrument list
  exceeds a stated budget.
- **A dense table is only cheap if its index is bounded.** Instruments live in a
  table indexed by symbol, which makes lookup a bounds check and an offset. It
  also means one instrument numbered 4,294,967,295 asks for a four-billion-entry
  table, about 171 GB, from a single mistyped configuration line. Symbol IDs are
  venue-assigned, so numbering them densely from zero costs nothing;
  `MAX_SYMBOL` makes that a refusal instead of a kill.
- **Price levels and order slots are different things and must not share a
  number.** The book boots with 65,536 *price levels* (the window) and
  separately needs a pool of *resting order slots*, because the engine
  addresses orders by dense index to keep insert and cancel O(1). That pool
  used to be a `DEFAULT_BOOK_CAPACITY` of 65,535 sitting in `lib.rs` — a
  magic number that, by coinciding with the ladder size, read as though the
  book could only hold 65,535 prices. It became `Instrument::max_open_orders`
  and is now a boot size: the pool grows on demand, so the number an operator
  writes decides the allocation the venue starts with, never what it can
  hold. A benchmark had already saturated the old cap silently, so every
  order past the limit was being rejected and the measurement was
  meaningless. Proving there is no per-level limit then turned up a second
  bug: the slot allocator returned `None` for both a duplicate order ID and
  an exhausted pool, and the caller reported both as `DuplicateOrderId` —
  `OrderLimitReached` existed and was never emitted. Growth retires that
  refusal below the `u32` index space entirely.
- **An instrument's band IS its price bound, and now its allocation bound.**
  `instrument.rs::to_slot` is the fat-finger check, and the band is handed to
  the engine as its price domain, so the window cannot be grown past it. One
  mechanism, both controls.

### Bugs that changed the design

- **A band that reached zero could print a trade nothing could settle.**
  Prices are signed on the wire and a negative floor parsed happily, but a
  notional is price times quantity in *unsigned* money. An ask resting below
  zero would trade, publish both fills, and then fail settlement: no assets
  moved, the maker's reservation was never consumed, and the order it was
  held against had already been deleted -- an asset frozen for the life of
  the venue, with only a violations counter to show for it. The band now has
  to start at one tick or higher, refused at the line that declares it. The
  same rule is what keeps `notional`'s `None` unreachable from a fill.
- **The market/TIF refusal changes how a pre-fix journal replays.** A market
  order carrying a resting time-in-force used to be accepted and rest at the
  band extreme; it is now refused, so a journal recorded before the fix
  replays that command -- and anything that traded against its remainder --
  differently. Deliberate: reproducing the old behaviour would mean
  reproducing a way to grow a book's window to its worst case for one unit of
  margin. Snapshots are unaffected, since `Book::restore` re-adds resting
  orders directly and pre-fix resting prices were all inside the band.
- **QUIC sessions were never reaped.** The venue frees a session when told the
  peer has gone; the stream writer was parked waiting for the venue to free it.
  Each waited on the other, so a disconnected client stayed in the map for good
  and cancel-on-disconnect could never fire. Nothing noticed because nothing
  counted sessions -- the tests all checked what arrived, never what was left
  behind. Whoever observes the connection ending now says so, and the writer also
  wakes on the connection closing.
- **A test helper hid it three times.** The collector stopped at the first event
  satisfying its predicate, so a reply of several events was silently truncated to
  one. Twice that looked like a product bug and once it masked one. Answers whose
  *length* is the measurement now use a drain with no early exit.

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

1. **Authentication establishes identity at connect and nothing after it.** The
   session that follows is neither encrypted nor authenticated, so an attacker
   who can *write* to the wire can still inject orders on an admitted
   connection. That is the accepted cost of a transport with no TLS, and the
   answer is a private link rather than a protocol change -- which is the
   deployment this transport was chosen for. It is listed here because
   `Authenticated` is the kind of word a reader assumes means more than it does.
2. **`CancelReplace` is cancel-then-submit** and emits both sets of events. It
   works, but a client sees a `Canceled` it did not ask for.
3. **Timestamps are stamped in the gateway, not at the NIC.** So they measure
   our own scheduling as well as the network, and the resolution is the group
   rather than the packet. `SO_TIMESTAMPING` is where the real value comes from
   and the field is already the right shape for it. `match_ns` is also not
   journalled -- a 64-byte command has no room -- so a replay re-emits it as
   zero.
4. **openraft's reported 40 ms blocking issue is unverified** against our
   batching pattern. Measure before committing to it on the data path.

---

## What is left

One, and it is a distributed-systems question rather than a threading one.

1. **An account trading in two partitions.** Symbols partition across processes
   today and that is how the venue uses more than one core — see the rejected
   note above for why threads were not the answer. What partitioning does not
   solve is a balance: an account's money lives in one partition, so it cannot
   reserve against a symbol served by another.

   Three shapes are worth weighing, and none of them is small: a position service
   the partitions reserve against; partitioning accounts as well as symbols, with
   a crossbar between the two; or pre-allocated buying power per partition,
   rebalanced out of band. The last is what several venues actually do, because
   it keeps the matching path free of any remote call — and its primitive now
   exists: an allotment moves as a `Withdraw` sequenced in one journal and a
   `Deposit` in the other, withdraw first so a crash strands value rather than
   minting it. What is still unbuilt is the settlement process that drives the
   pair and rebalances.

   Until that is answered, a deployment partitions by asset class or by
   settlement currency, so accounts rarely straddle a boundary — which is also
   what venues do.

Smaller, if wanted: MBO for colocated clients, fee schedules at settlement,
`io_uring` behind `LogStorage`, NIC hardware timestamping behind the
`ingress_ns` field.

