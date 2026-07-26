# A crypto exchange core, in Rust

A matching engine and the venue around it: binary protocol over TCP, order books
on a bitmap price ladder, balance reservation, an append-only journal that is the
single source of truth, a resumable market-data feed, snapshots, and replicated
durability.

Everything below is measured on the machine it was developed on, not estimated.
`cargo x latency` reproduces it in about six seconds.

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

Two processes over loopback TCP, which is what a client actually experiences:

| | one pipelined client | 32 concurrent clients |
|---|---:|---:|
| durable file journal | 662,951/sec | **928,943/sec** |
| in-memory journal | 2,962,954/sec | 1,745,152/sec |

Round trip for a single order in flight: 9 µs in memory, 51 µs to a quorum,
3.1 ms to a local disk. The first is **not** a durable number.

## Running it

Requires `rustup` and nothing else. There is no CI; `xtask` is the task runner.

```bash
cargo x
```

That is format, `clippy -D warnings`, and all 219 tests. Also:

```bash
cargo x latency
```

```bash
cargo x engine
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
crates/engine/     the matching engine. Zero dependencies, forbid(unsafe_code).
crates/protocol/   wire records: fixed 64-byte layouts, asserted at compile time.
crates/journal/    append-only log, replay, and replication to followers.
crates/pipeline/   sequencer, accounts, books, events, snapshots.
crates/gateway/    framing, sessions, the group-commit loop, TCP server, config.
xtask/             the task runner.
```

Three dependencies in total: `zerocopy` for the fixed-layout casts, `mio` for
readiness notification, and `libc`/`log` beneath `mio`. The engine itself has
none, and forbids `unsafe`.

[`DESIGN.md`](DESIGN.md) is the architecture. [`ENGINEERING.md`](ENGINEERING.md)
is the decisions, what was rejected and why, and the bugs worth remembering.

## Things worth knowing about the design

**The price ladder is the price band.** An instrument's book covers 65,536 ticks
from its floor. A price the ladder cannot address is a price the venue refuses,
so the memory bound and the fat-finger control are one mechanism rather than two.

**The journal is the only source of truth.** Every other piece of state is
derived, so recovery is: load a snapshot, replay from there. Deleting every
snapshot costs recovery time and nothing else — 12 ms to replay 100,000 commands
against 1.3 ms from a snapshot.

**Everything after the sequencer is deterministic.** No clock reads, no
randomness, no `HashMap` iteration reaching output. That is what makes replay a
recovery mechanism rather than an approximation, and it constrains every feature.

**Nothing is acknowledged before it is durable.** A group's events are released
only after the commit succeeds. An acknowledgement that has to be retracted is
worse than one that took longer.

**A subscriber is told the book before the changes to it.** Increments alone
cannot build a book — a client has no idea what was resting before it arrived — so
subscribing sends the current levels stamped with the sequence the increments
resume from.

## What is deliberately not here

Scope boundaries rather than omissions:

- **Leader election.** Quorum durability is implemented and measured; automatic
  failover is not. Failover needs consensus, hand-written consensus is how
  distributed systems lose data quietly, and the right answer is `openraft`.
  `ReplicatedLog` is the seam it belongs behind.
- **QUIC.** The transport is TCP. Swapping it replaces `gateway/src/tcp.rs` and
  touches neither the venue loop nor the codec, which is why they are separate.
- **Sharding across cores.** One book is inherently single-writer — matching is a
  sequential dependency, so there is no parallelism in it to take. Different
  symbols can run as independent engines, but an account trading two of them
  shares one balance, so that needs a two-stage account/symbol split rather than
  a lock.
- **Withdrawals, order-status queries, cancel-on-disconnect, trading halts.**
  Each is a venue feature rather than an exchange-core one. The one that matters
  most is the order-status query: a client shed for being slow can rebuild the
  book from a snapshot but not its own open orders.
- **Fees.** The design puts them at settlement so they never touch matching;
  nothing applies them yet.

## Correctness

219 tests. The ones worth looking at:

- `crates/pipeline/tests/simulation.rs` — the venue crashed repeatedly from a
  seed, asserting after every crash that recovery reproduces the last committed
  state order for order, and that nothing uncommitted survived.
- `crates/pipeline/tests/snapshot.rs` — a restart from a snapshot lands in
  exactly the state a full replay of the same journal does, including queue
  position, not merely the same depth.
- `crates/gateway/tests/over_tcp.rs` — real sockets, including a record torn
  across two writes and a client shed for being slow that reconnects and rebuilds.
- `crates/gateway/tests/idle_cost.rs` — a benchmark that fails if an idle
  connection ever costs more than 120 ns a pass again.
