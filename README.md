# A crypto exchange core, in Rust

A matching engine and the venue around it: a binary protocol over QUIC, order
books on a bitmap price ladder, balance reservation, an append-only journal that
is the single source of truth, a resumable market-data feed, snapshots, and
replication with quorum acknowledgement.

Everything below is measured on the machine it was developed on, not estimated.
`cargo x latency` reproduces it in about seven seconds.

## What it costs

| | |
|---|---:|
| passive limit order, full path | **157 ns** |
| crossing order, one fill | **159 ns** |
| cancel by order ID | **75 ns** |
| three market-data subscribers attached | +0 ns (below noise) |
| self-match prevention on the crossing path | +3 ns |

"Full path" means sequence, journal, balance reservation, match, and event
emission — not the book in isolation.

Durability is a different order of magnitude, and choosing between these two rows
is the largest decision in the design:

| commands/sec, durable | local fsync | quorum of replicas |
|---|---:|---:|
| group of 1 | 318 | 19,644 |
| group of 256 | 78,471 | 1,974,452 |
| group of 16,384 | 2,975,883 | **6,222,727** |

At a group of 16,384 the quorum path costs 161 ns per command — which *is* the
compute cost above. Durability has become free and the venue is bound by matching
again. Reaching another machine beats reaching the platter by **59×** at a group
of one, which is why the design acknowledges after a quorum rather than after a
flush.

Nothing in the code picks a group size. A group is whatever arrived since the
last pass, so it grows under load — exactly when a sync needs amortising — and
falls to one when the venue is idle and latency matters more.

Two processes over loopback, which is what a client actually experiences:

| | one pipelined client | 32 concurrent clients |
|---|---:|---:|
| durable file journal | 662,951/sec | **928,943/sec** |
| in-memory journal | 2,962,954/sec | 1,745,152/sec |

Concurrency helps where there is a sync to amortise and costs where there is not,
which is the group-commit design behaving as intended.

### Does it hold at scale

Same traffic spread over more accounts, every order resting:

| accounts | per command | holdings | balance memory |
|---|---:|---:|---:|
| 16 | 311 ns | 32 | ~0 |
| 100,000 | 792 ns | 200,000 | 9 MiB |
| **1,000,000** | **986 ns** | 2,000,000 | **91 MiB** |

About 1.01M commands a second at a million accounts. The 3.2× degradation is
cache misses on the balance map, not anything algorithmic — lookups are still
O(1), the working set simply stopped fitting. Memory is per *holding* rather than
per registered user, so an account that has never traded costs nothing.

Connections are the other axis, and the honest answer is different: an idle
connection costs 23 ns a pass, so a gateway holds thousands rather than millions.
`max_sessions` makes that a stated ceiling and counts refusals, because a venue
that accepts ten thousand connections and serves them all slowly is worse than
one that accepts what it can serve.

### Restart

| | |
|---|---:|
| replay all 100,000 commands | 12.0 ms |
| snapshot + replay the last 5,000 | **1.3 ms** |

## Running it

Requires `rustup` and nothing else. There is no CI; `xtask` is the task runner.

```bash
cargo x
```

That is format, `clippy -D warnings`, and all 233 tests. Also:

```bash
cargo x latency
```

To run the venue as a process and point a load client at it:

```bash
cargo run --release -p bx-gateway --bin venue -- --config venue.conf
```

```bash
cargo run --release -p bx-gateway --bin load -- 127.0.0.1:7070 --clients 32
```

## Layout

```
crates/engine/     the matching engine. No dependencies, forbid(unsafe_code).
crates/protocol/   wire records: fixed 64-byte layouts, asserted at compile time.
crates/journal/    append-only log, replay, replication with term fencing.
crates/pipeline/   sequencer, accounts, books, events, snapshots.
crates/gateway/    framing, sessions, group commit, QUIC and TCP, config.
xtask/             the task runner.
```

Dependencies are concentrated at the edge on purpose. The engine has none and
forbids `unsafe`; `protocol`, `journal` and `pipeline` use only `zerocopy` for
fixed-layout casts. Everything else — `quinn`, `rustls`, `tokio`, `mio` — is in
`gateway`, so the 80 crates in the lockfile buy transport and none of them can
reach the matching path.

[`DESIGN.md`](DESIGN.md) is the architecture. [`ENGINEERING.md`](ENGINEERING.md)
is the decisions, what was rejected and why, and the bugs worth remembering.

## Things worth knowing about the design

**QUIC, with a stream per channel.** Order acknowledgements and each market-data
channel get their own stream and their own flow control, so a client reading its
depth feed slowly no longer backs up its own fills — which one connection
carrying both cannot avoid. QUIC costs 5–20 µs more per exchange than raw TCP for
crypto and userspace work, which is invisible beneath a 51 µs quorum
acknowledgement. Optimising the transport below the sync would be optimising the
wrong thing.

**Async at the edge, one writer at the core.** Matching a book is a sequential
dependency — each order changes what the next one sees — so there is no
parallelism in it to take, and it stays one thread with no runtime. Connections
run on tokio and hand commands across a queue, which is also where the group
comes from.

**The price ladder is the price band.** An instrument's book covers 65,536 ticks
from its floor. A price the ladder cannot address is a price the venue refuses, so
the memory bound and the fat-finger control are one mechanism rather than two.

**The journal is the only source of truth.** Every other piece of state is
derived, so recovery is: load a snapshot, replay from there. Records are a fixed
64 bytes, which is what makes replay zero-copy and lets recovery seek straight to
a sequence instead of scanning to it.

**Everything after the sequencer is deterministic.** No clock reads, no
randomness, no `HashMap` iteration reaching output. That is what makes replay a
recovery mechanism rather than an approximation, and it constrains every feature.

**Nothing is acknowledged before it is durable.** A group's events are released
only after the commit succeeds. An acknowledgement that has to be retracted is
worse than one that took longer.

**Cancel-on-disconnect is opt-in.** A market maker cannot manage risk it can no
longer see, so leaving its quotes in the book after its connection dies is
dangerous; a client holding a limit order for a week wants exactly the opposite.
A venue that picks one for everybody is wrong for half its clients. The cancels
it causes are ordinary journalled commands, so a departing session cannot change
state by a private route.

**A client can ask what it still has working.** A book can be rebuilt from a
snapshot; a client's own orders cannot, and a trader that has just reconnected
must know what is in the market before it acts. The answer costs that account's
own order count rather than a scan of the venue, because the index that lets
self-match prevention skip in one lookup is the same index that lists the orders.

**A subscriber is told the book before the changes to it.** Increments alone
cannot build a book — a client has no idea what was resting before it arrived — so
subscribing sends the current levels stamped with the sequence the increments
resume from.

**Replication is fenced by term.** A follower refuses a group from a term older
than the highest it has seen, so a leader that has been replaced cannot keep
writing and two leaders cannot acknowledge into logs that diverge.

## What is deliberately not here

Scope boundaries, with reasons rather than apologies:

- **Automatic leader election.** Quorum durability is built and measured, and
  fencing makes a promotion safe however it is performed — but detecting a dead
  leader and promoting a replacement needs consensus, and that means `openraft`.
  It belongs on a *separate* leadership log: an openraft entry is variable-length
  and heterogeneous, so putting the command log through it would cost the
  zero-copy replay and the O(1) seek that make a 1.3 ms restart possible.
- **Sharding across cores.** One book is single-writer by nature. Different
  symbols could run as independent engines, but an account trading two of them
  shares one balance, so that needs a two-stage account/symbol split rather than
  a lock.
- **`io_uring`.** `LogStorage` is the seam it belongs behind. It is Linux-only,
  and untested platform-specific I/O is worse than none — the measurement also
  said the cost was one syscall *per record*, and batching the writes recovered
  8.6× without leaving `std`.
- **Withdrawals and trading halts.** Venue features rather than exchange-core
  ones.
- **Fees.** The design puts them at settlement so they never touch matching;
  nothing applies them yet.

## Correctness

233 tests. The ones worth looking at:

- `crates/pipeline/tests/simulation.rs` — the venue crashed repeatedly from a
  seed, asserting after every crash that recovery reproduces the last committed
  state order for order, and that nothing uncommitted survived.
- `crates/pipeline/tests/snapshot.rs` — a restart from a snapshot lands in
  exactly the state a full replay of the same journal does, including queue
  position, not merely the same depth.
- `crates/gateway/tests/over_quic.rs` — real QUIC, including a client that drops
  with no close handshake and reconnects to a book it can rebuild, and a stalled
  market-data stream that must not block order acknowledgements.
- `crates/journal/src/replication.rs` — a replaced leader is refused and its
  write never reaches the follower's log.
- `crates/gateway/tests/idle_cost.rs` — a benchmark that fails if an idle
  connection ever costs more than 120 ns a pass again.
