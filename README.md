<h1 align="center">exchange-core</h1>

<p align="center">
  A crypto exchange core in Rust — matching, journaling, market data, replication and failover.<br>
  Single-writer, deterministic, and durable by quorum rather than by disk.
</p>

<p align="center">
  <a href="https://github.com/hsdxpro/exchange-core/actions/workflows/ci.yml"><img src="https://github.com/hsdxpro/exchange-core/actions/workflows/ci.yml/badge.svg" alt="ci"></a>
  <img src="https://img.shields.io/badge/rust-1.97.1-000000?logo=rust" alt="Rust 1.97.1">
  <img src="https://img.shields.io/badge/tests-467%20passing-success" alt="467 tests">
  <img src="https://img.shields.io/badge/engine%20deps-0-success" alt="Zero engine dependencies">
  <img src="https://img.shields.io/badge/unsafe-forbidden-success" alt="Forbid unsafe">
  <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT">
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="#performance">Performance</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="#design-decisions">Design decisions</a> ·
  <a href="DESIGN.md">Design</a> ·
  <a href="ENGINEERING.md">Engineering log</a>
</p>

---

- **190 ns** for a passive limit order across the full path: sequence, journal, balance reservation, match, emit
- **6.6M commands/sec** durable, acknowledged by a quorum of replicas
- **66.7 µs** round trip replicated, against 357 µs for a local `fsync`. Reaching two machines is **5.4× faster than reaching the platter**
- **0.9 ms** to restart from a snapshot instead of 9.6 ms replaying from zero
- **467 tests**, including multi-process failover, kill-and-restart recovery, and seeded crash simulation
- Ed25519 logon, TLS 1.3 for internet sessions, a per-symbol kill switch, and an optional venue-signed hash chain a client can check against the stream

Every number measured on the machine this was built on. `cargo x latency` reproduces the
command-path and durability tables in about seven seconds.

The multi-process figures below were measured on an earlier revision and are unchanged: the
checks added since cost about 20 ns a command, which is 0.03% of a 66.7 µs round trip and
below what three processes over loopback can resolve.

## Quick start

Needs `rustup` and nothing else. `xtask` is the task runner, and CI runs the
same `cargo x` on Linux and Windows rather than a list of its own -- so
green locally and green on a runner cannot become two different things.

```bash
cargo x
```

Format, `clippy -D warnings`, and all 467 tests.

```bash
cargo x latency
```

Reproduces the performance tables below.

#### Run it as a real venue and measure it

```bash
cargo run --release -p bx-gateway --bin venue -- --config bench.conf
```

```bash
cargo run --release -p bx-gateway --bin load -- 127.0.0.1:7070 --clients 32
```

Two configs, because a benchmark and a deployment want opposite things. `venue.conf` is
the deployment shape — auth required, a real rate limit, journal on disk — so pointing
`load` at it measures those three, not the venue. Differences are stated in the file, and
`load` says so out loud rather than inventing a number if the venue refuses.

#### Run it replicated

Start followers first, then list them in `venue.conf`:

```bash
cargo run --release -p bx-gateway --bin replica -- 127.0.0.1:7201
```

## Performance

### Command path

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

Best of three runs, each already a minimum of five. Some of these are a few nanoseconds
slower than earlier revisions: order IDs are now checked for monotonicity and every command
is checked against the account its session proved, and both cost something on the path that
accepts. The trade is stated rather than smoothed — a duplicate fill or an order placed for
somebody else's account is worth more than 20 ns.

**Cancel is the exception, and it is stated too.** Orders are now keyed by account *and* ID,
because venue-global IDs meant two ordinary clients that both number their orders from one
collided on their first — no attacker needed, and what every client library does by default.
Cancel is the most lookup-heavy path and the key went from eight bytes to sixteen: measured
back to back on one machine, **207 → 335 ns**. Everything else was flat or better on the same
run, the self-match check improving because the book now carries the owner and no longer needs
a hash lookup per crossable order. The absolute figures in this table were taken on a quiet
machine and want re-measuring there; the delta was taken hot, on both sides, which is what
makes it a delta. A venue two honest clients cannot both use is not worth 128 ns.

"Full path" = sequence, journal, balance reservation, match, event emission. Not the book
in isolation.

### Durability

The largest decision in the design is which of these two rows to acknowledge on.

| Commands/sec, durable | Local `fsync` | Quorum of replicas | Gain |
|---|---:|---:|---:|
| Group of 1 | 317 | 32,756 | **103×** |
| Group of 256 | 78,581 | 2,832,642 | 36× |
| Group of 16,384 | 2,996,385 | **6,617,455** | 2.2× |

- The gap is widest with nothing to amortise a sync over — a client waiting on one order.
- At 16,384 the quorum path costs 151 ns/command, below the compute cost above.
  Durability is nearly free and the venue is matching-bound again.
- **Nothing picks a group size.** A group is whatever arrived since the last pass: it
  grows under load, and falls to one when idle and latency matters more.

### End to end, separate processes over loopback

| Durability | Round trip, 1 in flight | Pipelined |
|---|---:|---:|
| None (journal in memory) | 11.4 µs | 1,796,009/sec |
| Local disk, one `fsync` per group | 357 µs | 1,560,998/sec |
| **Quorum of two followers** | **66.7 µs** | 1,785,257/sec |

One `venue`, two `replica` processes, 200,000 orders each, one sitting. Pipelined the
three converge — once the group is large the sync is shared across thousands of orders.

### Scaling

<table>
<tr valign="top"><td>

**Accounts** — same traffic, spread wider

| Accounts | Per command | Memory |
|---|---:|---:|
| 16 | 204 ns | ~0 |
| 100,000 | 454 ns | 9 MiB |
| **1,000,000** | **506 ns** | **91 MiB** |

1.98M commands/sec at a million accounts. The 2.5× degradation is cache misses on the
balance map — lookups are still O(1), the working set stopped fitting. Memory is charged
per *holding*, so an account that never traded costs nothing.

</td><td>

**Connections** — the ceiling is gone

| Strategy | Per pass | Marginal |
|---|---:|---:|
| Read every socket | — | 422 ns |
| Found by readiness | 5,400 ns | 16 ns |
| **Written only when touched** | **1,300 ns** | **0 ns** |

A pass over 256 idle connections costs the same as over one. A connection with nothing to
say and nothing to be told is never visited, so cost stops growing with connection count.

Memory is **263 KB per session**, measured across 4,000 idle sessions: a 256 KiB read
buffer preallocated at admission so the trading path never allocates, plus bookkeeping.
Past `max_sessions` a connect is accepted and immediately dropped, so the client learns
now rather than timing out against a venue that will never read it. 8,000 sessions opened
and closed return RSS to baseline.

</td></tr>
</table>

### Under a crowd

The same order load twice -- alone, then with 1,024 market-data subscribers
watching. Every subscriber follows the book channel, so every top-of-book
change fans out to all of them. One venue process; generators and audience
share the machine.

| 128 senders, 200,000 orders | alone | + 1,024 subscribers |
|---|---:|---:|
| Round trip, min / p50 | 9.3 µs / 17.5 µs | 369.7 µs / **14.3 ms** |
| Pipelined throughput | 2.40M orders/sec | 35,910/sec |
| Concurrent throughput | 1.33M orders/sec | 968,701/sec |
| Orders acknowledged | **200,000 of 200,000** | **200,000 of 200,000** |
| Delivered to the audience | — | 214.8M events (13.7 GB) |
| Venue RSS peak | 237 MiB | 852 MiB |

Heavier crowds hold the same shape: 512 senders with 512 subscribers moved
518M events (33 GB) at a 1.10 GiB peak; 1,024 senders alone peaked at
1.03 GiB with a 17.1 µs p50.

### The audience, moved off the order path

Those figures are what an audience costs when it is served *by the thread that
sequences orders*. `feed_listen` gives public market data its own thread and its
own port, and the venue's share of distribution becomes one copy of each group
into a bounded handoff, whatever the audience size. Same run, same 1,024
subscribers, the only difference being which port they watch:

| 128 senders, 200,000 orders, 1,024 subscribers | On the trading port | On the feed port |
|---|---:|---:|
| Round trip, min | 511.8 µs | **19.5 µs** |
| Round trip, p50 | 28.1 ms | **23.3 µs** |
| Pipelined throughput | 31,054/sec | **2,531,928/sec** |
| Orders acknowledged | 200,000 of 200,000 | **200,000 of 200,000** |

**1,207× on p50, 81× on throughput**, and the order path is back to roughly what it
costs with nobody watching at all. This is the OUCH/ITCH split every venue
converges on, and the reason is this table rather than tradition. The private
account feed stays on the trading session — that is the trader's own reply path
— and the feed port serves the public channels only, which is why it can ask for
no credentials.

The feed thread earns a fifth of that on its own. Its first version walked every
subscriber for every channel that moved and fanned out once per group; indexing
subscribers by channel, accumulating the channels across every group waiting and
walking the audience once, and visiting only the sessions holding bytes took p50
from 30.5 to 23.3 µs and throughput from 1.97M to 2.53M. The same three
mechanisms the venue's own loop uses, which is where they should have been
copied from in the first place.

### One packet, however many are listening

TCP fan-out still costs a copy and a write per subscriber — cheap now that it is
off the order path, but it grows with the audience. `multicast` sends the same
events as UDP packets instead: the switch replicates, so a group of ten and a
group of ten thousand are the same send. Two lines are A and B, identical
packets on independent paths, so a receiver takes whichever copy of a sequence
arrives first and a packet lost on one path costs nothing.

The shape is MoldUDP64's, which is what Nasdaq's ITCH rides on: a header naming
the run and the position, several messages per packet, a sequence checked by
arithmetic. Ours drops the per-message length prefix because an event is a fixed
64 bytes — a packet is a header and an array. 21 events per packet, sized so a
full one plus IP and UDP headers stays inside a 1,500-byte MTU, because a
fragmented market-data packet turns one lost fragment into a lost packet.

A client joining a market already in motion is stated the book first — the feed
rebuilds each symbol's levels from the deltas it is already forwarding, so the
snapshot costs the venue nothing and needs no round trip to it. The stated book
carries the sequence the increments then resume from, so the two join without a
gap and without an overlap.

Nothing on the packet path waits for anyone: no acknowledgement, no flow control,
no retransmission. A receiver that misses a packet sees the gap and asks on the feed
port — `RESUME {channel, last_seq}`, served from the retention ring by the thread
that owns it, so a slow receiver never reaches back into the fast path. The
feed retains every public channel the venue produces rather than only the ones
somebody is watching: a repair is asked for *after* the loss, and a ring created
at that moment would be empty. The private account channel has no wire name at all, so
it cannot be broadcast to a group anyone may join.

Uncomment `feed_listen` in `bench.conf`, then
`load --subscribers 1024 --feed 127.0.0.1:7071` reproduces both columns. It ships
commented out on purpose: the test suite parses and runs that file, and a fixed
port in it is a collision waiting for two venues to start at once. Restart the
venue between runs, or the second one measures the reject path — and says so.

What the pair says:

- **The audience taxes latency, never correctness or memory.** Every order is
  still acknowledged, RSS stays bounded -- outboxes are capped, a session is
  263 KB, the journal is 64 bytes a command -- and overfilling the 1M
  resting-order pool produces clean rejections at 2.1M/sec instead of growth.
- **The 14 ms is a seam, not slow code.** One gateway thread serves the order
  path and the audience, so a top-of-book change becomes 1,024 unicast writes
  and the next acknowledgement queues behind them. The venue-shaped fix is a
  market-data publisher of its own -- a separate thread or process, or a
  multicast feed that costs one packet regardless of audience size. That is
  the next seam worth opening, and this table is its baseline.
- **The fan-out is deterministic.** Two runs a day apart delivered the
  identical 214,790,272 events to the audience: the workload is seeded and
  nothing after the sequencer reads a clock.

`load --subscribers N` reproduces the table.

### Two doors

Two listeners, because two kinds of client want opposite things. The colocated cross-connect
gets raw TCP — fixed 64-byte records, nothing between the client and the book, the same trade
CME iLink and Nasdaq OUCH make. Internet sessions get **TLS 1.3** (`tls_listen`, rustls,
1.3 only): the Ed25519 logon proves who a session is, and on a public wire TLS is what extends
that from the handshake to every byte after it. Past the record layer the two are identical —
same framing, same budgets, same books — and the e2e suite proves a plaintext probe at the TLS
door is dropped without disturbing the client behind it. Certificate and key are operator-held
PEM files; nothing inline, nothing in the repository.

### Operating it

Counters are kept off the hot path already -- sampled every 64th pass, no clock read in front
of an order -- and `metrics_listen` now serves them in Prometheus exposition format so a
monitoring system can page on a degraded majority or a rising shed count while the venue keeps
serving. The text is published on the cadence the venue already logs at, never built per
request: an endpoint that did work when asked would let whoever scrapes decide how much work
the venue does. HTTP is thirty lines by hand rather than a framework and an async runtime.

### Verifiable ordering

**Optional, off by default, +25 ns a command, +17 ns more when signed.** The journal keeps a running SHA-256 over its
records, sealed every 1,024 of them and published on a `checkpoint` channel. Each head names
the first sequence it does **not** cover, because it lags the log between boundaries — naming
the newest sequence instead would claim coverage of records the head has not committed to,
and a client folding those would disagree with a venue that had done nothing wrong.

A client that follows the stream recomputes the head and sees for itself that its order was
included where it was told and that nothing was inserted in front of it. *Did the sequencer
front-run me* is otherwise a question a venue can only answer by asserting an answer. It is
the Certificate Transparency shape pointed at a matching engine's sequencer, and it is cheap
here because the sequencer is a single writer over fixed-width records.

Each head is followed by the venue's Ed25519 signature over it, as two records because a
signature is 64 bytes and an event is 64 bytes in total. The sealed sequence is inside the
signed message rather than beside it, so a past commitment cannot be replayed as a description
of the present, and the domain string differs from the logon's so one key cannot produce a
signature valid in both places. Unsigned, a chain shows only that the venue agrees with itself:
a venue that rewrote its history could publish a head over the rewritten stream and nothing
would contradict it. Signing is what makes the head evidence rather than an assertion.

The key is loaded from a file the operator holds — `chain_key_file`, never the key itself in
the configuration — and only the public half is ever printed. Ed25519 is deterministic, so a
promoted node replaying the journal reproduces the same signatures, which is what allows them
to live in the event stream at all.

One thing it is not: it **cannot be retrofitted**. Switching it on over a journal that already
holds records commits to a suffix while a replay commits to everything, so it is refused at the
first append rather than publishing a head that overstates what it covers.

### Reconnecting

`RESUME {channel, last_seq}` sends a client the events it missed rather than a whole book.
Three answers and never silence: the gap, a restatement if it fell outside the retention
window, or a restatement if it named a sequence the channel never reached — which is what a
cursor from a previous leader looks like, since channel numbering restarts when a venue does.

Gap-filling across a promotion would mean retaining a ring for every channel and republishing
into all of them during replay, which costs the whole feed budget on a venue with no
subscribers. The price of not paying it is one snapshot per client per promotion.

The one thing a restatement cannot carry is an outcome the dead leader never published — an
order acked but never reported resting or filled. The venue journals a **watermark** every 64
groups meaning "everything before me was handed to the feed"; recovery regenerates the private
outcomes past the last marker, and the first session to act for each account is handed them on
reconnect, sequence zero. Told, not queried. Never accepted from the wire, and never the ack
itself — durable is what the ack means.

### Restart

| | |
|---|---:|
| Replay all 100,000 commands | 9.6 ms |
| Snapshot + replay the last 5,000 | **0.9 ms** |

## Architecture

```text
crates/engine/     Matching engine. No dependencies, forbid(unsafe_code).
crates/protocol/   Wire records: fixed 64-byte layouts, asserted at compile time.
crates/journal/    Append-only log, replay, replication with term fencing.
crates/pipeline/   Sequencer, accounts, books, events, snapshots.
crates/gateway/    Framing, sessions, admission, group commit, TCP server, config.
crates/election/   Leader election, on a log the orders never touch.
xtask/             Task runner.
```

No crate on the matching path has a dependency it could reach; the engine stands alone.

- `protocol`, `journal`, `pipeline`: `zerocopy` for fixed-layout casts.
- `journal`: `socket2` at connection setup only, to bound kernel buffers on replication
  sockets — the one buffer the venue does not otherwise own. `sha2` for the optional chain,
  already in the tree beneath `ed25519-dalek`, which uses SHA-512 internally.
- `gateway`: `mio`, `ed25519-dalek`, `getrandom`, confined there. Authentication runs once
  per connection, so none of it is on the order path.
- 164 crates in the lockfile, 123 from one decision — `openraft` + `tokio` in
  `crates/election`, used by the `venue` binary, not the gateway library. Large for one
  feature, accepted because writing consensus correctly is harder than depending on it.
  An order never enters that path.

## Design decisions

The reasoning for each is in [`DESIGN.md`](DESIGN.md); what was rejected and why, plus the
bugs worth remembering, is in [`ENGINEERING.md`](ENGINEERING.md).

| Decision | Why |
|---|---|
| **One unencrypted TCP transport** | Nothing between a market maker and the book. QUIC was built and removed — 38.6 µs round trip against TCP's 8.6 µs. |
| **One thread, no async runtime** | Matching is a sequential dependency: each order changes what the next one sees. |
| **The price ladder is the price band** | A book covers 65,536 ticks from its floor, so the memory bound and the fat-finger control are one mechanism. |
| **The journal is the only source of truth** | Everything else is derived state, so recovery is a snapshot load followed by a replay. |
| **Determinism after the sequencer** | No clock reads, no randomness, no `HashMap` order reaching output, so a replay reproduces the original exactly. |
| **Nothing acknowledged before it is durable** | A group's events are released only after its commit succeeds. |
| **No secret exists to steal** | The venue issues a nonce and the client returns an Ed25519 signature over a domain string and that nonce. The venue holds only public keys, so reading its configuration yields nothing signable — and a signature made elsewhere with the same key is not a logon. |
| **A session acts for one account** | The account it proved, or in an open venue the one its first command claimed. Authentication used to establish identity at connect and bind nothing to it, so one valid credential could trade every account on the venue. |
| **Restrictions stop new risk, never reducing it** | A halted symbol and a stopped account both still take cancels. `CancelOnly` exists so a book drains in an orderly way instead of freezing everyone into their positions, and `CancelAll` is unprivileged because a client that has lost track of its own state must be able to flatten itself without an operator. |
| **Order IDs increase per account** | A client that loses its connection before an acknowledgement cannot tell whether the order landed. Resending risked a second execution, because the duplicate check only found orders still resting. Now at most one attempt can ever be live, so a retry is safe. |
| **Batching is the default shape, not a mode** | A command is a fixed 64 bytes and the send path takes a slice, so a group is whatever arrived since the last pass. That is where 7,400× between a group of one and a group of 16,384 comes from. |
| **Cancel-on-disconnect is opt-in** | A market maker needs its quotes pulled on disconnect; a week-long limit order does not. |
| **Replication is fenced by term** | A replaced leader cannot keep writing, and two leaders cannot acknowledge into diverging logs. |
| **Failover needs no person** | `openraft` runs a *separate* leadership log. What crosses into the venue is the term — which Raft already guarantees is unique per leader, and therefore *is* a fencing token. |

## Testing

467 tests. The ones worth reading:

| Test | What it proves |
|---|---|
| [`pipeline/tests/simulation.rs`](crates/pipeline/tests/simulation.rs) | The venue crashed repeatedly from a seed; recovery reproduces the last committed state order for order, and nothing uncommitted survives. |
| [`pipeline/tests/snapshot.rs`](crates/pipeline/tests/snapshot.rs) | A snapshot restart lands in exactly the state a full replay does — including queue position, not merely depth. |
| [`gateway/tests/over_tcp.rs`](crates/gateway/tests/over_tcp.rs) | Real sockets: a record torn across two writes, a slow client shed and rebuilding, cancel-on-disconnect withdrawing every quote. Repairing any one channel is checked against every other channel's queued events — a session has one outbox and a cursor per feed, so a repair that touches the buffer as a whole loses fills nothing will resend. |
| [`gateway/tests/failover.rs`](crates/gateway/tests/failover.rs) | The binaries a deployment actually runs. The leader is killed mid-session and a node with an empty log is promoted at a higher term, then checked against everything the dead leader acknowledged. |
| [`gateway/tests/machine_down.rs`](crates/gateway/tests/machine_down.rs) | A standalone venue killed mid-trading recovers every acknowledged order on restart, refuses duplicates of them, and serves. A follower killed mid-trading stops nothing, and is backfilled to the leader's exact log when it returns. |
| [`gateway/tests/shipped_binaries.rs`](crates/gateway/tests/shipped_binaries.rs) | `venue` and `load` run as a pair against the shipped configuration files — the only tests that can catch the binaries and their config drifting apart. |
| [`pipeline/tests/chain.rs`](crates/pipeline/tests/chain.rs) | Written from the client's side: heads are recomputed with the same public function a verifier would use, rather than comparing the venue against itself. Reordering two commands changes the head, inserting one changes it, and both recovery paths reach the head of the venue they recovered. |
| [`pipeline/tests/risk.rs`](crates/pipeline/tests/risk.rs) | Every restriction checked both ways — that it bites, and that a cancel still gets through it. A halt and a stopped account survive snapshot and replay, because a venue that came back trading a symbol somebody stopped would look like a successful restart. |
| [`gateway/tests/admission.rs`](crates/gateway/tests/admission.rs) | Authentication end to end: a signature over the bare nonce refused, half a signature refused, halves from two accounts refused, a captured logon replayed onto a fresh connection refused, and a revoked key closing the sessions already using it. |
| [`gateway/tests/idle_cost.rs`](crates/gateway/tests/idle_cost.rs) | Fails if a pass over 256 idle connections ever costs meaningfully more than a pass over one. |
| [`journal/src/replication.rs`](crates/journal/src/replication.rs) | A replaced leader is refused and its write never reaches the follower's log. |

`failover.rs` covers the one property untestable inside a single process. It found a
leader whose journal held nothing but its magic bytes after ten thousand acknowledged
orders.

## Not included

Scope boundaries, with reasons rather than apologies:

- **Cross-partition balances.** Symbols already partition across processes, but an account
  banking in one partition and trading a symbol in another needs a position service, not a
  thread pool. The largest open question here.
- **`io_uring`.** Linux-only, and untested platform-specific I/O is worse than none.
  Batching writes recovered 8.6× without leaving `std`.
- **Withdrawals and fees.** Venue features, not exchange-core ones. Halts *are* here — a
  symbol has a trading state and an account has a kill switch.

## Related

The matching core grew out of
[**matching-engine**](https://github.com/hsdxpro/matching-engine) — the same bitmap ladder
in Rust and C++, both verified against independent reference models. That project is the
auditable engine on its own; this is the venue around it.

[**tick-to-trade**](https://github.com/hsdxpro/tick-to-trade) is the other side of the
wire: feed parsing, book maintenance and order entry, measured end to end.

## License

[MIT](LICENSE)
