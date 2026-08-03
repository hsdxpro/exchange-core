# Benchmarks

Methodology first, then numbers. [README.md](README.md) carries the headlines;
this file says how they were taken and what they are not.

[ARCHITECTURE.md](ARCHITECTURE.md) &middot; [PROTOCOL.md](PROTOCOL.md) &middot;
[DESIGN.md](DESIGN.md) &middot; [ENGINEERING.md](ENGINEERING.md)

## Method

- One desktop, no pinning claimed unless stated. Figures drift 2–3× on a loaded
  machine; anything measured while a build runs is worthless.
- **Minimum of N**, never a single run. This box swings 2–4× between runs of the
  same binary; a supposed 9–12% improvement once turned out to be two
  byte-identical binaries. A/B comparisons run interleaved, against
  **checksummed** artifacts.
- Deltas beat absolutes: a before/after taken hot in one sitting is comparable;
  two absolutes from different days are not. Where a claim is inside the bench's
  own session-to-session spread, it says so.
- "Full path" = sequence, journal append, durability, balance reservation,
  match, event emission. Not the book in isolation.

Reproduce:

```bash
cargo x latency                                              # command-path + durability tables, ~7 s
cargo run --release -p bx-gateway --bin venue -- --config bench.conf
cargo run --release -p bx-gateway --bin load -- 127.0.0.1:7070 --clients 32
cargo run --release -p bx-gateway --bin replica -- 127.0.0.1:7201   # then list in venue.conf
cargo run --release -p bx-gateway --bin load -- 127.0.0.1:7070 --subscribers 1024 --feed 127.0.0.1:7071
```

`bench.conf` and `venue.conf` differ on purpose — a benchmark and a deployment
want opposite things (auth, rate limit, journal on disk). Differences are stated
in the files; `load` says so out loud rather than inventing a number if the
venue refuses. `feed_listen` ships commented out (a fixed port in a file the
test suite runs is a collision); uncomment it for the feed-port columns, and
restart the venue between runs or the second one measures the reject path — and
says so.

## Command path

Best of three runs, each already a minimum of five.

**2026-08-02, windowed ladder + growing pool:** measured interleaved against
the prior revision in one sitting, minimum across two pairs: the composite
paths are inside noise (mixed stream +3 ns, batches +2–7 ns, self-match and
chain flat), the 2,000-level sweep pays +4 ns/level, and cancel pays +33 ns —
the offset translation landing on the most lookup-heavy path, the same path
that took +128 ns for account-keyed IDs. The table below is the quiet-machine
baseline and wants re-taking there.

| Operation | Cost |
|---|---:|
| Passive limit order, full path | **190 ns** |
| Crossing order, one fill | **255 ns** |
| Cancel by order ID | **118 ns** |
| Mixed stream | **186 ns** |
| Market order sweeping 2,000 levels | 60 ns/level |
| Three market-data subscribers attached | +19 ns |
| Verifiable chain, when enabled | +25 ns |
| Chain signed by the venue | +17 ns more |

Some rows are a few nanoseconds slower than earlier revisions: order IDs are now
checked for monotonicity and every command against the account its session
proved. The trade is stated rather than smoothed — a duplicate fill or an order
placed for somebody else's account is worth more than 20 ns.

**Cancel is the exception, stated too.** Orders are keyed by account *and* ID —
venue-global IDs collided the first time two ordinary clients both numbered from
one. The most lookup-heavy path took the sixteen-byte key: **207 → 335 ns**,
measured back to back on one machine. Everything else was flat or better on the
same run (the self-match check improved: the book now carries the owner, no hash
lookup per crossable order). A venue two honest clients cannot both use is not
worth 128 ns.

The 2,000-level sweep row exists because every earlier crossing benchmark put
its makers at one price — measuring the fill loop, nothing about breadth. It
cost 562 ns/level until the touched-price dedup stopped being quadratic; 176
then, 60 now on the current revision.

## Durability

The largest decision in the design is which of these two rows to acknowledge on.

| Commands/sec, durable | Local `fsync` | Quorum of replicas | Gain |
|---|---:|---:|---:|
| Group of 1 | 317 | 32,756 | **103×** |
| Group of 256 | 78,581 | 2,832,642 | 36× |
| Group of 16,384 | 2,996,385 | **6,617,455** | 2.2× |

- The gap is widest with nothing to amortise a sync over — a client waiting on
  one order.
- At 16,384 the quorum path costs 151 ns/command, below the compute cost above:
  durability is nearly free and the venue is matching-bound again.
- **Nothing picks a group size.** A group is whatever arrived since the last
  pass: it grows under load, and falls to one when idle and latency matters.
- Getting here needed a fix, not a bigger batch: group commit once issued one
  `write` syscall per 64-byte record, plateauing near 345k/sec however large the
  group. Buffered, written once per sync: **8.6×** at 16,384.

## End to end, separate processes over loopback

One `venue`, two `replica` processes, 200,000 orders each, one sitting.

| Durability | Round trip, 1 in flight | Pipelined |
|---|---:|---:|
| None (journal in memory) | 11.4 µs | 1,796,009/sec |
| Local disk, one `fsync` per group | 357 µs | 1,560,998/sec |
| **Quorum of two followers** | **66.7 µs** | 1,785,257/sec |

The round-trip column is the argument: with one order in flight there is nothing
to amortise, and reaching two machines is **5.4× faster than reaching the
platter**. Pipelined, the three converge — the sync is shared across thousands.
The in-memory round trip is **not a durable number**; the durable answer for one
order in flight is the quorum, 66.7 µs.

The multi-process figures were measured on an earlier revision and are
unchanged: the checks added since cost ~20 ns a command, 0.03% of the round
trip, below what three processes over loopback can resolve.

## Scaling

**Accounts** — same traffic, spread wider:

| Accounts | Per command | Memory |
|---|---:|---:|
| 16 | 204 ns | ~0 |
| 100,000 | 454 ns | 9 MiB |
| **1,000,000** | **506 ns** | **91 MiB** |

1.98M commands/sec at a million accounts. The 2.5× is cache misses on the
balance map — still O(1), the working set stopped fitting. Memory is charged per
*holding*; an account that never traded costs nothing.

**Connections** — the ceiling is gone:

| Strategy | Per pass (256 idle) | Marginal |
|---|---:|---:|
| Read every socket | — | 422 ns |
| Found by readiness | 5,400 ns | 16 ns |
| **Written only when touched** | **1,300 ns** | **0 ns** |

A connection with nothing to say and nothing to be told is never visited.
Memory is **263 KB per session** (a 256 KiB read buffer preallocated at
admission, so the trading path never allocates), measured across 4,000 idle
sessions; 8,000 opened and closed return RSS to baseline. Past `max_sessions` a
connect is accepted and immediately dropped — the client learns now, not by
timeout.

## Under a crowd

Same order load twice — alone, then with 1,024 subscribers following the book
channel on the **trading port**. One venue process; generators and audience
share the machine.

| 128 senders, 200,000 orders | alone | + 1,024 subscribers |
|---|---:|---:|
| Round trip, min / p50 | 9.3 µs / 17.5 µs | 369.7 µs / **14.3 ms** |
| Pipelined throughput | 2.40M orders/sec | 35,910/sec |
| Concurrent throughput | 1.33M orders/sec | 968,701/sec |
| Orders acknowledged | 200,000 of 200,000 | 200,000 of 200,000 |
| Delivered to the audience | — | 214.8M events (13.7 GB) |
| Venue RSS peak | 237 MiB | 852 MiB |

The audience taxes latency, never correctness or memory. The fan-out is
deterministic: two runs a day apart delivered the identical 214,790,272 events.

**The feed port removes the tax.** `feed_listen` gives public market data its
own thread and port; the venue's share becomes one copy per group into a bounded
handoff, whatever the audience size. Same run, same 1,024 subscribers, only the
port differs:

| 128 senders, 200,000 orders, 1,024 subscribers | Trading port | Feed port |
|---|---:|---:|
| Round trip, min | 511.8 µs | **19.5 µs** |
| Round trip, p50 | 28.1 ms | **23.3 µs** |
| Pipelined throughput | 31,054/sec | **2,531,928/sec** |
| Orders acknowledged | 200,000 of 200,000 | 200,000 of 200,000 |

**1,207× on p50, 81× on throughput** — clear of any noise this bench has; the
smaller claims below are not, and say so. This is the OUCH/ITCH split every
venue converges on, for this reason rather than tradition.

Within the feed thread, indexing subscribers by channel and visiting only
sessions holding bytes moved p50 30.5 → 23.3 µs and throughput 1.97M → 2.53M in
one session — about 1.3×, **inside the session-to-session spread**. Read it as
direction, not size.

**Woken, not polled.** The feed thread waits on sockets; the handoff is not a
socket, so a group handed over in a quiet moment sat until the wait timed out —
**61.7 ms** of market-data latency on a venue answering orders in tens of µs.
The venue rings a `mio::Waker` when it fills the seam, suppressed twice over
(skipped while draining, capped at one per 50 µs). What the suppression is
worth, this benchmark cannot say: five variants ranked cleanly in one session
and the ranking *inverted* in another, on unchanged binaries. The numbers kept
are the deterministic ones: 61.7 ms without the wake, and a handoff stress test
going from a 20 s timeout to 0.01 s.

## Restart

| | |
|---|---:|
| Replay all 100,000 commands | 9.6 ms |
| Snapshot + replay the last 5,000 | **0.9 ms** |

What a snapshot saves depends on the ratio between journal and resting book —
restoring costs one insert per resting order, so a journal where nothing is
cancelled saturates the book and the snapshot saves almost nothing. Snapshot
cadence is derived, not picked: target recovery time ÷ measured replay rate
(7.6M commands/sec).

## What these numbers are not

- Not a claim about your hardware; reproduce them on the machine that matters.
- Not asserted in CI: perf gates on shared runners are flake factories. This
  file is the contract; `cargo x latency` re-takes the core tables in seconds.
- The overload figures measure a venue, its generators and its audience sharing
  one machine — worst case for the venue, stated rather than staged.
