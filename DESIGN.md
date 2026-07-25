# Exchange with subscriptions — design

Target: a production-shaped venue. Binary protocol, reliable UDP open to the public
internet, 1000+ symbols, replicated durability, deterministic crash recovery, hard memory
bounds.

Every decision below is made. This document is the plan; no code has been written against
it yet.

---

## 1. What we are building

| | |
|---|---|
| Order entry | Binary, over QUIC (public) and raw UDP or shared memory (colocated) |
| Matching | Price-time FIFO, one writer per symbol shard, deterministic |
| Accounts | Balance reservation before matching, sharded by account |
| Market data | L2 depth deltas, trades, BBO — sequenced, snapshot + delta, gap recovery |
| Private data | Order updates, fills, balance changes, per account |
| Durability | Replicated journal to quorum before ack |
| Recovery | Event-sourced replay onto snapshot, deterministic |
| Scale | 1000+ symbols, millions of messages/sec, bounded memory |

**Not in this phase:** settlement and withdrawal, margin and liquidation, auctions,
iceberg/hidden orders, cross-venue routing, KYC.

---

## 2. Pipeline

Everything after the sequencer must be a deterministic function of the sequenced stream.
That single rule is what makes replay, hot standby, and debugging possible. No wall-clock
reads, no random numbers, no external calls, no map-iteration-order dependence downstream
of the sequencer. Timestamps are captured *before* sequencing and travel inside the
command.

```
  clients
    │  QUIC/443 binary  ·  raw UDP or shm (colo)  ·  TCP fallback
    ▼
┌─────────────────┐   stateless, N instances
│    Gateway      │   decode, authenticate, rate limit, session state
└────────┬────────┘   stamps hardware ingress time
         │ commands
         ▼
┌─────────────────┐   single writer
│   Sequencer     │   assigns global seq, the moment an order "exists"
└────────┬────────┘
         │
         ▼
┌─────────────────┐   append-only, replicated to quorum
│    Journal      │   ══► ACK RELEASED HERE ══►  client hears "received"
└────────┬────────┘
         │
         ▼
┌─────────────────┐   sharded by account id, single writer per shard
│  Risk / Account │   reserve balance, or reject
└────────┬────────┘
         │
         ▼
┌─────────────────┐   sharded by symbol, single writer per shard
│    Matching     │   the engine on `matching_engine`, with a windowed ladder
└────────┬────────┘
         │ events: fills, book deltas, rejects
         ▼
┌─────────────────┐   release/settle reservations from fills
│  Risk (post)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐   sequenced channels, ring-buffered for replay
│   Publisher     │
└────────┬────────┘
         ▼
   fanout tier ──► subscribers
```

**Two-stage client response.** The ack at the journal means *received and durable*, not
*accepted*. The risk and matching outcome arrives after as a separate event: resting,
filled, or rejected. This mirrors FIX, where an order is acknowledged and then reports its
status, and it is what lets us ack in microseconds without ever acking a lie.

---

## 3. Durability and recovery

**Ack after quorum.** An order is durable on a majority of journal replicas before the
client is told anything. Budget 10–50 µs. The alternative — ack first, journal behind —
means a crash leaves clients holding acks for orders we have no record of, and the book we
recover disagrees with what thousands of clients believe they have resting. That is a
reconciliation incident, not a performance tradeoff.

**Journal format.** Append-only, fixed-size records, memory-mapped, one file per segment.
Each record: `seq | ingress_ns | source | command`. Nothing else. The journal is the source
of truth; every other piece of state is derived and therefore rebuildable.

**Snapshots.** Every 100M sequences or 15 minutes, whichever comes first. A shard
serializes its full state and records the sequence it corresponds to. Snapshots are taken
on a secondary replaying the same stream, so the primary never pauses.

**Recovery:** load the newest snapshot, replay the journal from `snapshot.seq + 1`, resume.
Because every stage downstream of the sequencer is deterministic, recovered state is
bit-identical to what was lost. The existing engine already pins this with a golden state
hash over a 100,000-command replay; that check extends to cover the whole pipeline.

**Retention.** Hot journal on local NVMe for 7 days, which covers replay for any realistic
incident. Segments then compress and ship to S3, with a lifecycle rule to Glacier Deep
Archive, total retention 7 years to satisfy trade-record obligations. Snapshots keep the
last 24 hours hot, then follow the same path. Retention is a background job; it never
touches the matching path.

**Failover for the MVP is operator-assisted**, not automatic. Synchronous replication to
two followers, manual promotion. Automatic leader election is phase 2. Writing consensus is
the single easiest way to turn a working MVP into an unshippable one, and a venue that
fails over in 30 seconds under human control beats one that split-brains on its own.

---

## 4. Sharding

**Accounts shard by account ID. Books shard by symbol.** Both sit behind the same
sequencer, as consecutive stages.

This is what solves cross-symbol balance reservation. An account holding 100k USDT that
sends a 50k BTC buy and a 60k ETH buy at the same moment must not have both accepted. If
each symbol shard checked balances independently, both would pass. Because every order for
an account passes through that account's shard first, and that shard is a single writer,
the reservation is atomic with no distributed lock and no cross-shard coordination.

**Placement.** One primary plus two journal replicas in a single availability zone; a
cross-AZ round trip is ~0.5 ms and would consume the entire ack budget on its own. An
asynchronous replica in a second AZ covers zone loss with a documented RPO. Shards are
pinned one per core, memory allocated on the local NUMA node.

---

## 5. Memory: what has to change

The engine on the `matching_engine` branch allocates a **fixed 2 MiB level table per
book** — 65,536 ticks × 16 bytes × 2 sides — regardless of how many levels are live. That
is the design decision its README highlights, and it does not survive this requirement:

| Symbols | Level tables alone |
|---|---|
| 100 | 200 MB |
| 1,000 | 2 GB |
| 10,000 | 20 GB |

Almost entirely empty. This is the out-of-memory failure mode, and it is structural.

**The fix is a windowed ladder, and it doubles as a risk control.** Keep a dense ladder of
4,096 ticks centred on the touch. Reject any order priced outside the window. Real venues
already do this and call it price banding — an order 50% away from the touch is rejected on
principle, not accepted and parked. The memory bound and the fat-finger control become the
same mechanism.

| | Full domain | Windowed (4,096) |
|---|---|---|
| Level table per book | 2 MiB | 128 KiB |
| Occupancy bitmap per book | 16 KiB | 1 KiB |
| 1,000 symbols | ~2 GB | **~130 MB** |
| 10,000 symbols | ~20 GB | **~1.3 GB** |

Re-centring when the touch moves is O(occupied levels), not O(window), because the bitmap
already enumerates exactly the occupied ticks. The traversal properties the current engine
is built on are unchanged.

**`OrderSlot` grows to 32 bytes.** It must carry an owner ID for self-trade prevention and
fill attribution, which the current 24-byte layout has no room for. At 32 bytes a slot no
longer straddles cache lines, which the earlier A/B experiment suggested is worth
something on random-access cancel — though that experiment was inconclusive on a noisy
desktop and should be redone on the target hardware before anyone claims a number.

**Everything else is capped and pre-allocated.** Order slots come from a per-shard arena
sized at startup. Per-account and per-symbol order limits are hard. Subscriber replay
buffers are fixed-size rings that overwrite rather than grow. There is no unbounded queue
anywhere; every backpressure path either blocks the producer or drops with an explicit,
counted, observable policy. The system should be incapable of OOM by construction, not by
tuning.

---

## 6. Matching rules

**Fees do not affect matching.** Matching is pure price-time priority on the raw limit
price. The engine tags each fill with its maker or taker role; the fee schedule is applied
downstream at settlement. Fee-adjusted matching would make the engine depend on account
state — the taker's fee tier — which couples matching to accounts and breaks the rule that
matching is a deterministic function of the sequenced stream alone. Not worth it.

**Self-trade prevention: cancel-newest by default**, configurable per account to
cancel-oldest or cancel-both. Cancel-newest protects resting liquidity: the maker who was
there first keeps their queue position, and the incoming aggressor is the one cancelled.
This matches CME's Self-Match Prevention default and Binance's `EXPIRE_TAKER`. STP runs
inside the matching engine, because only the engine sees both sides of a potential match,
and it is the reason `OrderSlot` carries an owner ID.

**Market data is MBP publicly, MBO on the colo feed.** The public channel carries
aggregated depth deltas. Order-by-order data, with anonymized per-session order IDs, is a
separate channel for colocated subscribers. Two reasons: MBO leaks individual trading
patterns and is a genuine competitive concern for market makers, and it is 10–100× the
message volume, which matters when the fanout tier is serving millions of connections.

**Price banding** rejects orders outside the ladder window, as above. This is a matching
rule, not just a memory bound, and it is applied at the risk stage so a rejected order
never reaches the engine.

---

## 7. Transport and wire protocol

**One binary encoding everywhere.** Fixed-layout POD structs, little-endian, 8-byte
aligned, version byte in the header. Zero-copy decode: cast the buffer, validate, use. No
serde, no allocation, no reflection on the hot path.

Three carriers for the same bytes:

| Carrier | Who | Notes |
|---|---|---|
| QUIC on UDP/443 | public internet | 0-RTT reconnect, per-stream flow control so one lost packet does not stall others, connection IDs survive NAT rebinding and wifi→5G handoff |
| Raw UDP or shared memory | colocated | skips encryption and congestion control; single-digit µs |
| TCP/WebSocket | fallback | for the 1–5% of networks that block UDP/443 |

**On UDP over the internet.** UDP/443 reaches 95–99% of networks. It fails on some
corporate proxies and mobile carriers, which is why the TCP fallback exists and why every
CDN keeps one. UDP also needs 10–20× more keepalive traffic than TCP to hold NAT bindings
open, a real cost on mobile that is budgeted for rather than wished away.

**What we do not do: UDP multicast.** Classic exchange market data is multicast, but a
normal AWS VPC has no usable multicast — only Transit Gateway multicast, which AWS
themselves flag as unsuitable for latency-sensitive work and which measures ~0.4 ms within
an AZ. Native L2/L3 multicast on AWS exists only on Outposts racks. If we move to Outposts
or a real colo, multicast becomes available and the publisher grows a second backend behind
the same trait.

---

## 8. Subscription and gap recovery

Channels are independently sequenced:

```
book.{symbol}      L2 depth deltas          (public)
trades.{symbol}    executions               (public)
bbo.{symbol}       top of book only         (public, cheapest, what most retail wants)
mbo.{symbol}       order-by-order           (colocated subscribers)
orders.{account}   private: acks, rejects, status
fills.{account}    private: executions
balance.{account}  private: reservations and settlements
```

**Every message carries `(channel, seq)`.** The client detects a gap by arithmetic, not by
timeout.

```
  SUBSCRIBE  {channels, symbols, depth}
      ──►  SNAPSHOT {channel, seq = S, state}
      ──►  DELTA    {channel, seq = S+1}
      ──►  DELTA    {channel, seq = S+2}
           ...
```

On reconnect or gap the client sends `RESUME {channel, last_seq}`:

- within the publisher's ring buffer → replayed deltas, no snapshot needed
- beyond it → fresh snapshot at the current sequence, then deltas

The ring is fixed size, holding a few seconds of peak rate per channel. This is the
MoldUDP64/ITCH pattern, and it is what makes disconnection a non-event.

**Fanout.** 100M registered users is not 100M concurrent connections — expect low
single-digit millions. Fanout nodes are stateless, consume the internal sequenced feed,
hold per-connection subscription sets, and scale horizontally at roughly 100k connections
each. They are the only tier that scales with user count, and the only tier where losing a
node costs nothing but reconnects.

---

## 9. Time

Solved by hardware, and better than expected on AWS.

**Hardware packet timestamping** puts a 64-bit nanosecond timestamp on every inbound packet
at the NIC, before the kernel, socket or application sees it. That is the true "exchange
received" time, and it is immune to scheduling jitter in our own gateway. It is disciplined
by the Amazon Time Sync PTP hardware clock, which runs off satellite-connected atomic
references in each region.

**Three timestamps travel with every order** and are published to clients:

| | Source | Meaning |
|---|---|---|
| `ingress_ns` | NIC hardware timestamp | when the packet arrived at the venue |
| `seq_ns` | sequencer, PTP-disciplined | when the order entered the official order |
| `match_ns` | matching shard | when it executed or rested |

**TSC is for us, not for clients.** `rdtsc` measures intra-process latency at ~20 cycles
against ~25 ns for `clock_gettime`, but it is not traceable to a time standard. It never
leaves the process and is never published.

This configuration clears MiFID II's 100 µs traceability requirement for high-frequency
activity without dedicated timing hardware, which used to be the expensive part.

---

## 10. Stack

Minimal by default. Every dependency is one we would otherwise have to write, and each sits
behind a trait so it can be replaced or compiled out.

**The hot path has no async runtime at all.** Sequencer, journal, risk and matching run on
pinned, busy-polling threads with SPSC ring buffers between them — the Disruptor pattern.
An async runtime on the matching path buys nothing and costs scheduling jitter. Async lives
only in the gateway and fanout tiers, where the problem is connection count rather than
per-message latency.

| Concern | Choice | Why |
|---|---|---|
| Language | Rust 1.97+, edition 2024 | already the engine's home |
| Matching path | pinned threads, busy-poll, no runtime | determinism and no scheduler involvement |
| Inter-stage | SPSC ring buffers in shared memory | no locks, no allocation |
| Gateway/fanout runtime | `tokio` | connection-bound tier; maturity beats micro-optimization, and `quinn` integrates natively |
| QUIC | `quinn` | fastest Rust QUIC in published benchmarks under stable conditions, largest ecosystem. `s2n-quic` behind the same trait as fallback if we hit stability problems under loss |
| Raw UDP | `std::net::UdpSocket` with `recvmmsg` | plus an optional F-Stack backend behind the same trait, compile-time feature, off by default |
| Journal I/O | `io_uring` in SQPOLL mode | submission without syscalls on the write path |
| Encoding | hand-rolled fixed-layout structs | zero-copy, versioned, no codegen step |
| Metrics | HdrHistogram, sampled off the hot path | latency distributions are the only metric that matters here |
| Consensus | none in MVP; `openraft` in phase 2 | synchronous replication with operator failover first |

**Runtimes considered and rejected.** `monoio` is thread-per-core over io_uring but lags
io_uring feature parity and shows little recent maintenance — not something to build a
venue on. `glommio` has a genuinely interesting three-ring design with a dedicated latency
ring, but its ecosystem is small and we do not want an async runtime on the hot path
regardless. Both are worth revisiting for the fanout tier if tokio becomes the constraint.

**Aeron is the road not taken, for now.** It is the mature answer for reliable UDP in
trading: single-digit µs, 20M+ msg/sec, NAK-based retransmission, plus Archive for
journalling and Cluster for consensus. But its first-class clients are Java, C/C++ and
.NET; Rust access is a third-party wrapper over the C API and needs a separate media driver
process. Too heavy against the minimal-dependency requirement. Revisit if the custom
transport becomes the bottleneck, which it may.

---

## 11. Failure modes

| Failure | Behaviour |
|---|---|
| Gateway dies | clients reconnect to another; sessions re-established from `last_seq` |
| Sequencer/primary dies | operator promotes a replica; replicas already hold the journal to the last acked sequence; zero acked orders lost |
| Journal replica dies | quorum continues on the remaining replicas; degraded, alarmed |
| Matching shard panics | deliberate abort with a state dump; restart from snapshot + replay |
| Subscriber falls behind | ring buffer overwrites; subscriber detects the gap and re-snapshots |
| Client floods | per-account rate limit at the gateway, before sequencing |
| Order outside price band | rejected at the risk stage — this is also the memory bound |
| Self-match | cancel-newest by default; the resting order survives |

**Panics.** The current engine has bounds-checked indexing and one invariant `expect` on
the matching path. Here a panic on a matching thread is a deliberate abort with a state
dump, followed by replay — never a caught-and-continued exception. A matching engine that
continues after violating an invariant is worse than one that stops.

---

## 12. Phasing

1. **Windowed ladder, 32-byte slot with owner ID.** Change the memory model, keep every
   existing check passing, extend the differential model to cover re-centring. Nothing else
   can be built on a book that cannot hold 1000 symbols.
2. **Event emission.** Book deltas and trades out of the engine, sequenced. The engine
   currently publishes only fills.
3. **Sequencer, journal, replay.** Single node, no replication yet. Prove replay reproduces
   state bit-identically, extending the existing golden-hash check.
4. **Accounts and reservation.** Account shards, balance reserve and release, price banding.
5. **Binary protocol and QUIC gateway.** One transport first, fallbacks after.
6. **Subscriptions.** Snapshot, delta, resume, ring buffer.
7. **Replication and quorum ack.** Synchronous followers, operator failover.
8. **Fanout tier.** Horizontal scale-out for the public feed.

Each step ends with the system still runnable and still verifiable. The existing
verification approach — differential tests against an independently written model, plus a
golden replay hash — extends to every stage, and is the reason to keep the pipeline
deterministic in the first place.
