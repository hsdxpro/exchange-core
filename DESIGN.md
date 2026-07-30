# Exchange with subscriptions — design

A venue built around the existing matching engine: a binary protocol, 1000+ symbols,
replicated durability, safe failover, deterministic recovery.

The guiding constraint is least code that meets the requirement. Where a number appears it
is derived or measured, not chosen. Where a section describes something that is not built
it says so in its first sentence — a design document whose reader cannot tell the two apart
is worse than none.

---

## 1. Scope

**In:** order entry, matching, balance reservation, market data and private event
subscriptions with gap recovery, replicated journal, crash recovery by replay, and failover
that is safe and complete in execution — a promoted node catches up to a majority before it
serves, and term fencing stops a replaced leader writing.

**Deciding** *when* to promote and *whom* is `openraft`, on a leadership log the orders
never enter — §3.

**Out:** settlement and withdrawal, margin and liquidation, auctions, hidden orders,
cross-venue routing, KYC, fee tiers.

---

## 2. Pipeline

```
  clients ──TCP──► Gateway ──► Sequencer ──► Journal ──► Risk ──► Matching ──► Publisher
                   decode      assigns seq   replicated  balance   the engine   deltas,
                   frame       single        quorum      reserve   unchanged    trades,
                   sessions    writer        ↓                                  private
                                         ACK RELEASED
```

The gateway also guards admission: a session proves which account it acts for before it may
send anything, and each account has a send allowance. Both sit *before* the sequencer, so
neither a key lookup nor a clock reading can reach the deterministic path, and a command
that is refused or throttled is never sequenced — replay never has to ask what time it was.

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

Risk and matching are pipeline stages in one process, not network services. Reserving
balance across two symbols is therefore atomic without a distributed lock, because both
symbols are in the same process and the same thread.

**More than one core means more than one process, not more than one thread.** A venue owns
exactly the instruments its configuration lists, so a listing is partitioned by running
several of them over disjoint symbol sets. Each partition keeps single-writer determinism
exactly — a partition *is* a venue — and gets its own journal, replication, leadership and
failure domain, which threads in one process cannot offer. Threading the matching stage was
measured and rejected; [`ENGINEERING.md`](ENGINEERING.md) has the reasoning, and §12 has
the question partitioning leaves open.

---

## 3. Durability and failover

**Ack once the order is on a majority of three journal nodes.** A crash then loses nothing
a client was told we had.

**Consensus: `openraft`, built.** A node serves only while the cluster has elected it and stops the moment it has not, so a promotion needs no person. It runs a *separate* leadership log whose state machine holds one fact — who leads — and the command log never enters it: an openraft entry is variable-length and heterogeneous, so routing orders through it would cost the fixed 64-byte record and with it the zero-copy replay and O(1) sequence seek. What crosses the boundary is the term, which Raft already guarantees is unique per leader and is therefore a fencing token by construction. This is the CORFU and Aeron Cluster shape: consensus for leadership, plain leader-to-follower replication on the data path.

**Why openraft.** `raft-rs` entered maintenance mode; openraft is where new Rust
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

### A chain a client can check

**Built, off by default.** The journal keeps a running SHA-256 over its records:
`h[n] = H(h[n-1] ‖ records)`, sealed every 1,024 records and carried in the snapshot.

Why it is worth having: a client that follows the stream can recompute the head and see
for itself that its order was included where it was told and that nothing was inserted in
front of it. *Did the sequencer front-run me* is otherwise a question a venue can only
answer by asserting an answer. This is the Certificate Transparency shape — an append-only
log whose head commits to everything before it — pointed at a matching engine's sequencer,
and it is cheap here because the sequencer is a single writer over fixed 64-byte records.

A hash chain rather than a Merkle tree, deliberately: a tree buys compact inclusion proofs
for a client that does *not* hold the stream, and every client here can follow the feed. The
tree is the upgrade if that stops being true.

**What it costs, measured back to back:** +20 ns a command at the shipped interval (199 →
220 ns passive, 129 → 146 ns cancel), and nothing under batching. Per-*record* sealing was
tried first and cost 45–60 ns, almost all of it the setup and padding around a digest rather
than the hashing — a command is exactly one SHA-256 block, so finalising per command pays
that overhead every time.

**Two constraints the design forced, both learned by getting them wrong first:**

- **The boundary is a count of records, not a group.** Sealing per group was the obvious
  choice, since a group is one sync and one acknowledgement. It is unreproducible: group
  boundaries are never written to the journal, so a replay cannot find them, and a chain a
  replay cannot reproduce is a chain nobody can check.
- **Chaining cannot be retrofitted.** Turning it on over a journal that already holds records
  gives a head covering the suffix, while a replay of that journal covers all of it — so the
  venue and every client would disagree for a reason neither could see. It is refused after
  the first append.

**Not yet on the wire.** The head is state and API; publishing it to clients as a signed
checkpoint event is the remaining step, and until that exists no client can actually check
anything.

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
- **One order-slot arena per partition**, shared across the symbols it lists, rather than a
  fixed per-book allocation. Total concurrent orders is the real bound, not orders × symbols.
- **Hard caps** on orders per account and per symbol, sized at startup.
- **Fixed-size ring buffers** for subscriber replay. They overwrite; they never grow.

No unbounded queue anywhere. Every backpressure path either blocks the producer or drops
with an explicit, counted, observable policy.

---

## 5. Risk checks

Applied at the risk stage, before matching, in this order:

1. The session proved which account it acts for, by signing a nonce the venue issued on
   connect. Nothing else is accepted until it has — not an order, not a subscription.
2. The account is inside its send allowance. A token bucket per account, refilled once per
   pass rather than once per command, so a flood is discarded before it is sequenced.
3. Order is inside the price band. The instrument's ladder range *is* the band, so the
   memory bound and the fat-finger control are one mechanism rather than two.
4. The session is acting for its *own* account. A session may name only the account it
   proved, or in an open venue the one its first command claimed. Without this,
   authentication established identity at connect and then bound nothing to it, so one
   valid credential was enough to trade every account on the venue.
5. The symbol is trading. `Trading`, `CancelOnly` or `Halted`, set by the administrator.
6. The account has not been stopped. The kill switch, per account.
7. Order ID above the highest that account has used, so a client may retry an order it
   never got an answer for without risking a second execution.
8. Order is inside the price band.
9. Sufficient free balance; reserve it.
10. Order count under the instrument's `max_open_orders` cap.

**Every restriction stops new risk and none of them stops reducing it.** A halted symbol
and a stopped account both still accept cancels and amends down. A venue that will not let
a client out of what it already holds is more dangerous than one that lets it keep trading,
and `CancelOnly` exists precisely so a book can be drained in an orderly way rather than
freezing everyone into their positions.

The privileged commands — halting a symbol, stopping an account — are permitted only from
the configured `admin_account`, checked in the gateway before sequencing so an unauthorised
one never reaches the journal. There is no default administrator: a kill switch reachable
because a configuration line was forgotten reads exactly like one that was meant to be
there. `CancelAll` is deliberately *not* privileged, because a client that has lost track of
its own state must be able to flatten itself without an operator.

Both restrictions are journalled and both are in the snapshot. An operator's halt that did
not survive a recovery would be the worst kind of bug: the venue comes back trading a symbol
somebody deliberately stopped, and it looks like a successful restart.

The first two happen in the gateway, ahead of the sequencer. That is not incidental: a key
lookup, a nonce and a clock reading must never reach the deterministic path, or replay
stops reproducing state.

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

**TCP only, unencrypted.** One way in, for retail and market makers alike: fixed 64-byte
records with nothing between the client and the book.

This reverses an earlier decision in this document, and the measurement is the reason.
QUIC was built, benchmarked against the same venue and the same traffic, and removed:

| | TCP | QUIC |
|---|---:|---:|
| round trip, one order in flight | **8.6 us** | 38.6 us |
| pipelined throughput | **3.76M/sec** | 1.48M/sec |

A stream per channel does solve real head-of-line blocking, and QUIC does survive NAT and
mobile handoff. None of that is worth 4.5x the round trip here. The earlier claim that
QUIC's overhead is "invisible beneath a 51 us quorum acknowledgement" was wrong twice over:
30 us against a 54 us path is not invisible, and it is charged on every order rather than
amortised over a group the way the quorum cost is.

TLS 1.3 also is not optional in QUIC — packet protection is part of the transport, so there
is no unencrypted mode — and a market maker on a cross-connect wants nothing in the path.
Encryption for clients that need it belongs in front of the venue, not inside it.

A raw UDP or shared-memory path for colocated clients sits behind the same seam and gets
built when someone needs it. `tcp.rs` is that seam: `venue` and `codec` do not change.

**One binary encoding** on whatever carries it: fixed-layout structs, little-endian,
version byte in the header, zero-copy decode. No serialization framework on the hot path.

---

## 7. Subscriptions and recovery

```
book.{symbol}      depth deltas, and a snapshot on subscribe
trades.{symbol}    executions
account            private: this session's own order lifecycle and fills
```

Three channels rather than the six an earlier draft listed. The private ones collapsed
because a client wanting its fills wants its order states too, and splitting them costs a
second subscription to learn the same story. `bbo.{symbol}` is the one worth adding back:
top-of-book is the cheapest feed and what most clients actually want, and the engine
already caches it — but nothing publishes it yet.

Every message carries `(channel, sequence)`, so a client detects a gap by arithmetic rather
than by waiting for a timeout.

```
SUBSCRIBE {channels}  ──►  SNAPSHOT {seq = S}  ──►  DELTA {S+1}  ──►  DELTA {S+2} ...
```

**What is built:** a reconnecting client subscribes again and is placed at the channel's
current sequence, with a snapshot first on the two channels that carry state. The ring is
fixed size, holding a few seconds of peak rate, and a subscriber that falls out of it is
restated the same way.

**What is not:** a client cannot name where to resume from. `RESUME {channel, last_seq}` —
the MoldUDP64/ITCH pattern, where a client that missed a few seconds is sent just the gap —
is designed and not implemented. Until it is, every reconnect costs a snapshot rather than
a delta, and this section previously claimed otherwise.

Sequence numbering is per channel and starts at zero when the channel is first followed. It
is therefore **not** continuous across a restart or a promotion: a new leader replays the
journal to rebuild state but does not re-publish, so its channels begin again. That is
survivable only because clients do not carry cursors across — and it is the reason a cursor
past a channel's end is refused rather than treated as current. Adding `RESUME` requires
making the numbering continuous first, by carrying the cursors in the snapshot or by
deriving them from the journal sequence.

The publisher and the connection fanout start as one process. They separate when connection
count actually forces it, not before.

---

## 8. Time

Two timestamps travel with every order and are published on its acknowledgement:

| | Source | Meaning | Journalled |
|---|---|---|---|
| `ingress_ns` | gateway, before sequencing | when the venue read the command off the wire | yes |
| `match_ns` | gateway, at group commit | when the group began matching | no |

Both are read in the gateway and handed *in* to the pipeline. Neither is a clock reading
taken on the deterministic path — that is the same rule that puts authentication and rate
limiting ahead of the sequencer, and it is what lets a replay reproduce `ingress_ns` rather
than invent a new one.

`match_ns` is not journalled, because a 64-byte command has no room for it and widening the
record would cost a cache line on every order the venue ever writes. A recovered venue
therefore re-emits it as zero, which is the honest answer: it is a measurement of the run,
not a fact about the order.

They ride on the `Received` event, whose `quantity` and `price` are otherwise zero. Giving
them fields of their own would mean a 72-byte event, and an event that no longer fits a
cache line costs every subscriber on every message to carry a number most never read.

**Resolution is the pass, not the packet.** The clock is read once per group rather than
once per command, so two orders from the same write share an arrival time. True per-packet
arrival has to come from the NIC — `SO_TIMESTAMPING`, or the AWS Nitro hardware stamp
described below — because a reading taken in the gateway measures our own scheduling as
well as the network. The field is where that value goes when the deployment can supply it.

Off by default in the measurement configuration and on by default in a deployed one: two
wall-clock readings a pass vanish under load and are worth about a quarter of a pass when
the group is one, and the numbers in the README exist to measure matching rather than
`SystemTime`.

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
| Matching path | one thread per partition, no async runtime | thread pinning |
| Encoding | fixed 64-byte records via `zerocopy` | — |
| Transport | TCP, unencrypted, `mio` readiness | raw UDP or shm for colo |
| Gateway | sessions, framing, group commit, Ed25519 challenge, per-account rate limits | `ed25519-dalek` |
| Durability | group commit; quorum to followers, fenced by term | — |
| Consensus | `openraft`, on a separate leadership log | — |
| Journal I/O | buffered `std`, one write and one sync per group | `io_uring`, SQPOLL |
| Timestamps | `ingress_ns` journalled, `match_ns` on the ack | NIC hardware stamping |
| Metrics | log-linear histograms, sampled every 64th pass | export to a scrape endpoint |

Dependencies sit at the edge deliberately. The engine has none, `protocol`/`journal`/
`pipeline` use only `zerocopy`, and everything the transport needs lives in `gateway` — so
none of the thirty-three crates in the lockfile can reach the matching path.

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
| Leader dies | The cluster elects a replacement in about a second. The journal is already on a majority so no acknowledged order is lost, the new leader catches up to the longest log a majority holds before serving, and term fencing stops the old one writing if it returns |
| One journal node dies | majority continues; degraded and alarmed |
| A partition panics | deliberate abort with a state dump, restart from snapshot + replay. The other partitions are untouched, which is the point of them being processes |
| Subscriber falls behind | ring overwrites, subscriber sees the gap and re-snapshots |
| Client floods | A token bucket per account at the gateway, before sequencing, so a discarded command is never journalled. Separately, the per-session outbox budget sheds a client the venue cannot write *to*, which is the opposite failure |
| Subscriber stops reading | Its socket fills, the outbox grows past its budget, and the session is shed. How long that takes depends on what the kernel will hold for it first. Pinning the send buffer to make that a fixed number was tried and reverted: the same bound caps how far a healthy reader may fall behind, and it disconnected subscribers over milliseconds of jitter |
| Order outside price band | rejected at the risk stage |
| Self-match | cancel-newest; the resting order survives |
| Client retries an order it got no answer for | Refused. Order IDs increase per account, so an ID already accepted cannot be sent again whatever became of the order it named — a retry is safe, because at most one attempt can ever be live. `DuplicateOrderId` means the ID is live now; `OrderIdNotIncreasing` means it was used and is finished, which is what tells a retrying client its first attempt landed |
| Leader dies between the ack and the outcome | **Known limit.** The ack means durable, and the matching outcome follows as a separate event. A leader that dies in between leaves the client holding a durable ack it never gets an outcome for: the new leader replays state correctly but does not re-publish, so the event is gone. State is right and recoverable — the client resubscribes, is sent the book, and asks `QueryOpenOrders` — but it has to reconcile rather than be told. The fix is a published watermark in the journal and re-publishing from it on promotion |
| Two accounts choose the same order ID | **Known limit.** Order IDs are venue-global, not per account, so an ID one account has resting is refused to another as a duplicate. One client can therefore deny another an ID and probe which IDs are in use. Real venues namespace the client order ID per client; doing that here means keying the reservation table by `(account, order_id)`, which is on the hot path and wants measuring first |

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
4. **Gateway and binary protocol.** Built on QUIC, measured, moved to TCP.
5. **Subscriptions.** Snapshot, delta, resume, ring buffer.
6. **Quorum replication and term fencing.** Done. Election is not — see §3.
7. **Scale-out.** Split fanout from publisher when connection count demands it.

Steps 1–6 exist, except election. Step 7 is untouched: one process still holds the
publisher and the fanout, which the idle-connection measurement says is right until the
connection count rather than the order rate is what hurts.

Each step leaves the system runnable and verifiable. The existing approach — differential
tests against an independently written model, plus a golden replay hash — extends to every
stage. That is the payoff for keeping the pipeline deterministic.

---

## 12. To be measured before committing

Design decisions that rest on assumptions rather than observation. Each is cheap to test
and expensive to be wrong about.

- **openraft under our batching pattern**, and whether the reported 40 ms blocking
  reproduces. Decides whether consensus stays on the data path.
- **Hugepage benefit** on the level tables at realistic symbol counts.
- **32-byte versus 24-byte slot**, on the target hardware with a pinned core — the earlier
  attempt on a loaded desktop could not resolve it and reversed sign between runs.

Two came off this list by being measured, and both changed a decision. **QUIC's overhead**
was assumed negligible and is 4.5x the round trip, so the transport is TCP. **Replay
throughput** was the free variable in the snapshot cadence and is 7.6M commands/sec, so the
cadence is now derived from a stated recovery target rather than picked.
