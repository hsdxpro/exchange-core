# Exchange with subscriptions — design

A venue built around the existing matching engine: binary protocol over the public
internet, 1000+ symbols, replicated durability, automatic failover, deterministic recovery.

The guiding constraint is least code that meets the requirement. Where a number appears it
is derived or measured, not chosen.

---

## 1. Scope

**In:** order entry, matching, balance reservation, market data and private event
subscriptions with gap recovery, replicated journal, automatic failover, crash recovery by
replay.

**Out:** settlement and withdrawal, margin and liquidation, auctions, hidden orders,
cross-venue routing, KYC, fee tiers.

---

## 2. Pipeline

```
  clients ──QUIC──► Gateway ──► Sequencer ──► Journal ──► Risk ──► Matching ──► Publisher
           (a stream per channel: acks and market data never block each other)
                    decode      assigns seq   replicated  balance   the engine   deltas,
                    auth        single        3 nodes     reserve   unchanged    trades,
                    rate limit  writer        ↓                                  private
                                          ACK RELEASED
```

One rule holds the whole thing together: **everything after the sequencer is a
deterministic function of the sequenced stream.** No clock reads, no randomness, no
external calls, no map-iteration-order dependence. Timestamps are captured before
sequencing and travel inside the command.

That rule is what makes replay, hot standby, and reproducible debugging possible. It is a
constraint on every future feature.

**The ack means "received and durable", not "accepted".** The matching outcome — resting,
filled, rejected — follows as a separate event. This is how FIX already works, and it is
what lets us confirm in microseconds without ever confirming something we might have to
retract.

Risk and matching are pipeline stages in one process, not network services. Accounts shard
by account ID, books shard by symbol. An account's orders always pass through that
account's shard first, so reserving balance across two symbols at once is atomic without a
distributed lock.

---

## 3. Durability and failover

**Ack once the order is on a majority of three journal nodes.** A crash then loses nothing
a client was told we had.

**Consensus: `openraft` — intended, not built.** What exists is quorum durability with term fencing: a group is acknowledged once a majority holds it, measured 59x faster than a local fsync, and a follower refuses a group from a replaced leader. So no acknowledged order is lost when a leader dies and no two leaders can diverge; what is missing is noticing and promoting automatically.

**Why openraft when it is built.** `raft-rs` entered maintenance mode; openraft is where new Rust
work is pointed, and it is the consensus engine behind Databend in production.

The number that decides the design: openraft handles **33k writes/sec for a single writer
but 5.6M/sec batched**. Unbatched it is far too slow for us; batched it is beyond what we
need. The sequencer batches naturally, so this works — but **batching is mandatory, not an
optimization**, and that has to be true from the first commit.

**Known risk, to be measured before committing:** openraft is async and there is a reported
40 ms blocking issue in its tracker. If our workload reproduces it, the fallback is the
CORFU/Aeron-Cluster pattern — consensus for leader election and membership only, plain
leader-to-follower replication on the data path with a fencing token. Election happens once
per failure and can be slow; replication happens constantly and must not be. That is the
escape hatch, not the plan; taking it before measuring would be premature.

**Recovery** is: load the newest snapshot, replay the journal from the next sequence,
resume. Determinism means the recovered state is bit-identical to what was lost. The engine
already pins this property with a golden state hash over a 100,000-command replay; the same
check extends across the pipeline.

**Snapshot cadence is derived, not picked.** Choose a target recovery time, measure replay
throughput, snapshot often enough that replay never exceeds it. If replay runs at 5M
commands/sec and the target is 60 seconds, snapshot every 300M commands.

---

## 4. Memory

**The engine is unchanged.** An earlier draft proposed a windowed ladder to cut memory. The
arithmetic does not support it: 1000 symbols is about 2 GB of level tables, on a machine
with 256 GB or more. That is under 1% of the box. Ten thousand symbols is 20 GB, still
unremarkable. There was no memory problem to solve.

What is worth doing:

- **Hugepages** for the level tables, so sparse access does not thrash the TLB. A mount
  option, not a redesign.
- **One order-slot arena per shard**, shared across the symbols on that shard, rather than a
  fixed per-book allocation. Total concurrent orders is the real bound, not orders × symbols.
- **Hard caps** on orders per account and per symbol, sized at startup.
- **Fixed-size ring buffers** for subscriber replay. They overwrite; they never grow.

No unbounded queue anywhere. Every backpressure path either blocks the producer or drops
with an explicit, counted, observable policy.

---

## 5. Risk checks

Applied at the risk stage, before matching, in this order:

1. Account exists, session authorised.
2. Order is inside the price band — a percentage either side of a reference price. This is
   fat-finger protection. It is a range comparison, not a data structure.
3. Sufficient free balance; reserve it.
4. Per-account and per-symbol order count under cap.

Self-trade prevention runs inside the matching engine, because only the engine sees both
sides of a potential match. Default cancel-newest: the resting order survives and the
incoming aggressor is cancelled, which protects the participant who was there first. This
matches CME's Self-Match Prevention default and Binance's `EXPIRE_TAKER`.

Self-trade prevention needs an owner on the order, so `OrderSlot` grows from 24 to 32
bytes. That is the whole justification; earlier drafts also claimed a cache-line benefit
from an experiment that was inconclusive, and that claim is withdrawn.

**Fees never touch matching.** Matching is price-time priority on the raw limit price. The
engine tags each fill maker or taker; fees apply downstream. Fee-adjusted matching would
make the engine depend on account state and break the determinism rule.

---

## 6. Transport

**QUIC only.** One way in, for retail and professionals alike. Order entry and each market-data channel take their own stream, so a client reading its feed slowly stalls that feed and not its own fills. The 5-20 us QUIC adds over raw TCP is invisible beneath a 51 us quorum acknowledgement.

QUIC runs over UDP, so it is fast, but re-sends anything lost, so nothing goes missing. It
uses independent streams, so one lost packet does not stall everything behind it the way
TCP does. It reconnects in a single round trip, and a client changing network keeps its
session.

It reaches 95–99% of networks; the failures are some corporate proxies and mobile carriers
that block UDP on port 443. A TCP fallback for those is a known, deferred piece of work,
added when a customer actually hits it.

A raw UDP or shared-memory path for colocated clients sits behind the same trait and gets
built when someone needs it. Building it now would mean two protocol implementations to
keep in step for a benefit nobody has asked for yet.

**One binary encoding** on whatever carries it: fixed-layout structs, little-endian,
version byte in the header, zero-copy decode. No serialization framework on the hot path.

---

## 7. Subscriptions and recovery

```
book.{symbol}      depth deltas
trades.{symbol}    executions
bbo.{symbol}       top of book only — cheapest, and what most clients want
orders.{account}   private
fills.{account}    private
balance.{account}  private
```

Every message carries `(channel, sequence)`, so a client detects a gap by arithmetic rather
than by waiting for a timeout.

```
SUBSCRIBE {channels}  ──►  SNAPSHOT {seq = S}  ──►  DELTA {S+1}  ──►  DELTA {S+2} ...
```

On reconnect the client sends `RESUME {channel, last_seq}`. If that sequence is still in
the publisher's ring buffer it gets the missing deltas; if not, a fresh snapshot followed by
deltas. The ring is fixed size, holding a few seconds of peak rate. This is the
MoldUDP64/ITCH pattern and it makes disconnection a non-event.

The publisher and the connection fanout start as one process. They separate when connection
count actually forces it, not before.

---

## 8. Time

Two timestamps travel with every order and are published:

| | Source | Meaning |
|---|---|---|
| `ingress_ns` | NIC hardware timestamp | when the packet reached the venue |
| `match_ns` | matching shard | when it executed or rested |

AWS Nitro stamps every inbound packet with 64-bit nanoseconds **at the NIC, before the
kernel or our process sees it**, disciplined by the Amazon Time Sync PTP hardware clock.
That is the true arrival time and it is immune to jitter in our own gateway. This clears
MiFID II's 100 µs traceability requirement without dedicated timing hardware, which used to
be the expensive part.

`rdtsc` is used for internal latency measurement only. It is fast but not traceable to a
time standard, so it never leaves the process.

---

## 9. Stack

**No async runtime on the matching path.** Sequencer, risk and matching run on pinned,
busy-polling threads with single-producer single-consumer ring buffers between them. An
async runtime there buys nothing and costs scheduling jitter. Async lives in the gateway,
where the problem is connection count rather than per-message latency.

Two columns, because a design document that does not separate what exists from what is
intended is worse than none: a reader cannot tell which parts have been tested against
reality.

| Concern | Built | Intended |
|---|---|---|
| Language | Rust 1.97+, edition 2024 | — |
| Matching path | one thread, no async runtime | thread pinning |
| Encoding | fixed 64-byte records via `zerocopy` | — |
| Gateway | QUIC via `quinn` + `tokio`, a stream per channel | — |
| Durability | group commit; quorum to followers, fenced by term | — |
| Consensus | none: safe promotion, but no election | `openraft`, on a leadership log |
| Journal I/O | buffered `std`, one write and one sync per group | `io_uring`, SQPOLL |
| Metrics | none | histograms sampled off the hot path |

Dependencies sit at the edge deliberately. The engine has none, `protocol`/`journal`/
`pipeline` use only `zerocopy`, and everything the transport needs lives in `gateway` — so
the eighty crates in the lockfile cannot reach the matching path.

Two of the intended choices were deliberately deferred, with reasons in
[`ENGINEERING.md`](ENGINEERING.md). **`openraft`** because an openraft entry is
variable-length and heterogeneous, so routing the command log through it would cost the
zero-copy replay and O(1) sequence seek that make a 1.3 ms restart possible; election belongs
on a separate leadership log whose term feeds the fencing that already exists.
**`io_uring`** because the measurement said the cost was one syscall *per record* rather than
the syscall mechanism, and batching the writes recovered 8.6x without leaving `std`;
`LogStorage` is the seam it goes behind when a Linux deployment wants it.

**Considered and rejected.** `monoio` lags io_uring feature parity with little recent
maintenance. `glommio` has an interesting dedicated latency ring but a small ecosystem, and
we do not want async on the hot path regardless. **Aeron** is the mature answer for reliable
UDP in trading, but its first-class clients are Java, C/C++ and .NET; Rust access is a
third-party wrapper over the C API needing a separate media driver process. Revisit if the
custom transport becomes the bottleneck.

---

## 10. Failure modes

| Failure | Behaviour |
|---|---|
| Gateway dies | clients reconnect elsewhere, resume from `last_seq` |
| Leader dies | The journal is already on a majority, so no acknowledged order is lost, and term fencing stops the old leader writing if it returns. Promotion is manual until election is built |
| One journal node dies | majority continues; degraded and alarmed |
| Matching shard panics | deliberate abort with a state dump, restart from snapshot + replay |
| Subscriber falls behind | ring overwrites, subscriber sees the gap and re-snapshots |
| Client floods | per-account rate limit at the gateway, before sequencing |
| Order outside price band | rejected at the risk stage |
| Self-match | cancel-newest; the resting order survives |

A panic on a matching thread is a deliberate abort with a state dump, then replay — never a
caught exception. An engine that continues after violating an invariant is worse than one
that stops.

---

## 11. Phasing

1. **Event emission.** Book deltas and trades out of the engine, sequenced. It currently
   publishes only fills.
2. **Sequencer, journal, replay, single node.** Prove replay reproduces state
   bit-identically, extending the existing golden-hash check.
3. **Accounts, reservation, risk checks.** Including the price band and the 32-byte slot.
4. **QUIC gateway and binary protocol.**
5. **Subscriptions.** Snapshot, delta, resume, ring buffer.
6. **openraft replication and quorum ack.**
7. **Scale-out.** Split fanout from publisher when connection count demands it.

Each step leaves the system runnable and verifiable. The existing approach — differential
tests against an independently written model, plus a golden replay hash — extends to every
stage. That is the payoff for keeping the pipeline deterministic.

---

## 12. To be measured before committing

Design decisions that rest on assumptions rather than observation. Each is cheap to test
and expensive to be wrong about.

- **openraft under our batching pattern**, and whether the reported 40 ms blocking
  reproduces. Decides whether consensus stays on the data path.
- **Replay throughput**, which sets the snapshot cadence.
- **QUIC overhead in-datacenter**, which decides whether a raw path for colocated clients is
  ever worth building.
- **Hugepage benefit** on the level tables at realistic symbol counts.
- **32-byte versus 24-byte slot**, on the target hardware with a pinned core — the earlier
  attempt on a loaded desktop could not resolve it and reversed sign between runs.
