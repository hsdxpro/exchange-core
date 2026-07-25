# Exchange with subscriptions — design

Target: a production-shaped venue. Binary protocol, reliable UDP open to the public
internet, 1000+ symbols, replicated durability, deterministic crash recovery, hard memory
bounds.

This document is the plan. No code has been written against it yet.

---

## 1. What we are building

| | |
|---|---|
| Order entry | Binary, over QUIC (public) and raw UDP or shared memory (colocated) |
| Matching | Price-time FIFO, one writer per symbol shard, deterministic |
| Accounts | Balance reservation before matching, sharded by account |
| Market data | L2 book deltas, trades, BBO — sequenced, snapshot + delta, gap recovery |
| Private data | Order updates, fills, balance changes, per account |
| Durability | Replicated journal to quorum before ack |
| Recovery | Event-sourced replay onto snapshot, deterministic |
| Scale | 1000+ symbols, millions of messages/sec, bounded memory |

**Not in this phase:** settlement and withdrawal, fee tiers, margin and liquidation,
auctions, iceberg/hidden orders, cross-venue routing, KYC.

---

## 2. Pipeline

Everything after the sequencer must be a deterministic function of the sequenced stream.
That single rule is what makes replay, hot standby, and debugging possible. No wall-clock
reads, no random numbers, no external calls, no map iteration order dependence downstream
of the sequencer.

```
  clients
    │  QUIC/443 binary  ·  raw UDP or shm (colo)  ·  TCP fallback
    ▼
┌─────────────────┐   stateless, N instances
│    Gateway      │   decode, authenticate, rate limit, session state
└────────┬────────┘
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
│    Matching     │   the engine on `matching_engine` branch, extended
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
Each record: `seq | timestamp | source | command`. Nothing else. The journal is the source
of truth; every other piece of state is derived and therefore rebuildable.

**Snapshots.** Periodically (every N million sequences, or T seconds) a shard serializes
its full state and records the sequence it corresponds to. Snapshots are taken on a
secondary that is replaying the same stream, so the primary never pauses.

**Recovery** is then: load the most recent snapshot, replay the journal from
`snapshot.seq + 1` to the end, resume. Because every stage downstream of the sequencer is
deterministic, the recovered state is bit-identical to the state that was lost. The
existing engine already pins this property with a golden state hash over a 100,000-command
replay; that check extends to cover the whole pipeline.

**Failover for the MVP is operator-assisted**, not automatic. Synchronous replication to
two followers, manual promotion. Automatic leader election via Raft is phase 2. Writing
consensus is the single easiest way to turn a working MVP into an unshippable one, and a
venue that fails over in 30 seconds under human control beats one that split-brains
automatically.

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
already do this and call it price banding — an order 50% away from the touch is rejected
on principle, not accepted and parked. So the memory bound and the fat-finger control are
the same mechanism, which is the kind of coincidence worth taking.

| | Full domain | Windowed (4,096) |
|---|---|---|
| Level table per book | 2 MiB | 128 KiB |
| Occupancy bitmap per book | 16 KiB | 1 KiB |
| 1,000 symbols | ~2 GB | **~130 MB** |
| 10,000 symbols | ~20 GB | **~1.3 GB** |

Re-centring when the touch moves is O(occupied levels), not O(window), because the bitmap
already enumerates exactly the occupied ticks. The traversal properties the current engine
is built on are unchanged.

**Everything else is capped and pre-allocated.** Order slots come from a per-shard arena
sized at startup. Per-account and per-symbol order limits are hard. Subscriber replay
buffers are fixed-size ring buffers that overwrite rather than grow. There is no unbounded
queue anywhere; every backpressure path either blocks the producer or drops with an
explicit, counted, observable policy. The system should be incapable of OOM by
construction, not by tuning.

---

## 6. Transport and wire protocol

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
open, which is a real cost on mobile and is budgeted for, not wished away.

**What we do not do:** UDP multicast. Classic exchange market data is multicast, but a
normal AWS VPC has no usable multicast — only Transit Gateway multicast, which AWS
themselves flag as unsuitable for latency-sensitive work and which measures ~0.4 ms within
an AZ. Native L2/L3 multicast on AWS exists only on Outposts racks. If we ever move to
Outposts or a real colo, multicast becomes available and the publisher grows a second
backend behind the same trait.

---

## 7. Subscription and gap recovery

Channels are independently sequenced:

```
book.{symbol}      L2 depth deltas
trades.{symbol}    executions
bbo.{symbol}       top of book only — cheapest, and what most retail wants
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

The ring buffer is fixed size, sized for a few seconds of peak rate per channel. This is
the MoldUDP64/ITCH pattern and it is what makes disconnection a non-event.

**Fanout.** 100M registered users is not 100M concurrent connections — expect low
single-digit millions. Fanout nodes are stateless, consume the internal sequenced feed,
hold per-connection subscription sets, and scale horizontally at roughly 100k connections
each. They are the only tier that needs to scale with user count, and they are the only
tier where losing a node costs nothing but reconnects.

---

## 8. Stack

Minimal by default. Every dependency below is one we would have to write otherwise, and
each is isolated behind a trait so it can be replaced or compiled out.

| Concern | Choice | Why |
|---|---|---|
| Language | Rust 1.97+, edition 2024 | already the engine's home |
| Hot path threading | thread-per-core, pinned, busy-poll | no async runtime downstream of the gateway; the matching path must not yield |
| QUIC | `s2n-quic` or `quinn` | both mature; `s2n-quic` is AWS's and fits the deployment |
| Raw UDP | `std::net::UdpSocket`, `recvmmsg` | with an optional F-Stack backend behind the same trait, compile-time feature, off by default |
| Inter-stage | SPSC ring buffers in shared memory | the Disruptor pattern; no locks, no allocation |
| Journal | custom mmap append log | a few hundred lines, and we control the format and the fsync policy |
| Encoding | hand-rolled fixed-layout structs | zero-copy, versioned, no codegen step |
| Metrics | HdrHistogram, sampled off the hot path | latency distributions are the only metric that matters here |
| Consensus | none in MVP; `openraft` in phase 2 | synchronous replication with operator failover first |

**Aeron is the road not taken, for now.** It is the mature answer for reliable UDP in
trading — single-digit µs, 20M+ msg/sec, NAK-based retransmission, plus Archive for
journalling and Cluster for consensus. But its first-class clients are Java, C/C++ and
.NET; Rust access is a third-party wrapper over the C API and needs a separate media
driver process. That is heavy against the minimal-dependency requirement. Revisit if the
custom transport becomes the bottleneck, which it may.

---

## 9. Failure modes

| Failure | Behaviour |
|---|---|
| Gateway dies | clients reconnect to another; sessions re-established from `last_seq` |
| Sequencer/primary dies | operator promotes a replica; replicas already hold the journal to the last acked sequence; zero acked orders lost |
| Journal replica dies | quorum continues on the remaining replicas; degraded, alarmed |
| Matching shard panics | the process aborts deliberately with a state dump; restarts from snapshot + replay |
| Subscriber falls behind | ring buffer overwrites; subscriber detects the gap and re-snapshots |
| Client floods | per-account rate limit at the gateway, before sequencing |
| Order outside price band | rejected at the risk stage — this is also the memory bound |

**Panics.** The current engine has bounds-checked indexing and one invariant `expect` on
the matching path. In this system a panic on a matching thread is a deliberate abort with
a state dump, followed by replay — never a caught-and-continued exception. A matching
engine that continues after violating an invariant is worse than one that stops.

---

## 10. Phasing

1. **Windowed ladder.** Change the engine's memory model, keep every existing check
   passing, extend the differential model to cover re-centring. Nothing else can be built
   on a book that cannot hold 1000 symbols.
2. **Event emission.** Book deltas and trades out of the engine, sequenced. The engine
   currently publishes only fills.
3. **Sequencer, journal, replay.** Single node, no replication yet. Prove that replay
   reproduces state bit-identically, extending the existing golden-hash check.
4. **Accounts and reservation.** Account shards, balance reserve and release.
5. **Binary protocol and QUIC gateway.** One transport first, fallbacks after.
6. **Subscriptions.** Snapshot, delta, resume, ring buffer.
7. **Replication and quorum ack.** Synchronous followers, operator failover.
8. **Fanout tier.** Horizontal scale-out for the public feed.

Each step ends with the system still runnable and still verifiable. The existing
verification approach — differential tests against an independently written model, plus a
golden replay hash — extends to every stage, and is the reason to keep the pipeline
deterministic in the first place.

---

## 11. Open questions

- Fee model and whether fees affect matching or only settlement.
- Self-trade prevention policy: cancel-newest, cancel-oldest, or cancel-both.
- Whether market data carries order IDs (MBO) publicly or aggregated depth only (MBP).
- Time source: PTP versus TSC, and what timestamp we publish to clients.
- Retention: how far back the journal is kept, and where it goes after that.
