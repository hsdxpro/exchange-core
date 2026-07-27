<h1 align="center">exchange-core</h1>

<p align="center">
  A crypto exchange core in Rust — matching, journaling, market data, replication and failover.<br>
  Single-writer, deterministic, and durable by quorum rather than by disk.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/rust-stable-000000?logo=rust" alt="Rust">
  <img src="https://img.shields.io/badge/tests-325%20passing-success" alt="325 tests">
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

- **188 ns** for a passive limit order across the full path: sequence, journal, balance reservation, match, emit
- **4.6M commands/sec** durable, acknowledged by a quorum of replicas
- **66.7 µs** round trip replicated, against 357 µs for a local `fsync`. Reaching two machines is **5.4× faster than reaching the platter**
- **2.0 ms** to restart from a snapshot instead of 12.8 ms replaying from zero
- **325 tests**, including multi-process failover, kill-and-restart recovery, and seeded crash simulation

Every number measured on the machine this was built on. `cargo x latency` reproduces the
table in about seven seconds.

## Quick start

Needs `rustup` and nothing else. `xtask` is the task runner; there is no CI.

```bash
cargo x
```

Format, `clippy -D warnings`, and all 325 tests.

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
| Passive limit order, full path | **188 ns** |
| Crossing order, one fill | **224 ns** |
| Cancel by order ID | **106 ns** |
| Mixed stream | **171 ns** |
| Three market-data subscribers attached | +11 ns |
| Top-of-book feed, on every command | +8 ns |

"Full path" = sequence, journal, balance reservation, match, event emission. Not the book
in isolation.

### Durability

The largest decision in the design is which of these two rows to acknowledge on.

| Commands/sec, durable | Local `fsync` | Quorum of replicas | Gain |
|---|---:|---:|---:|
| Group of 1 | 321 | 15,596 | **49×** |
| Group of 256 | 77,989 | 1,477,527 | 19× |
| Group of 16,384 | 2,344,665 | **4,646,905** | 2× |

- The gap is widest with nothing to amortise a sync over — a client waiting on one order.
- At 16,384 the quorum path costs 215 ns/command, roughly the compute cost above.
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
| 16 | 177 ns | ~0 |
| 100,000 | 390 ns | 9 MiB |
| **1,000,000** | **463 ns** | **91 MiB** |

2.16M commands/sec at a million accounts. The 2.6× degradation is cache misses on the
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

</td></tr>
</table>

### Restart

| | |
|---|---:|
| Replay all 100,000 commands | 12.8 ms |
| Snapshot + replay the last 5,000 | **2.0 ms** |

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
  sockets — the one buffer the venue does not otherwise own.
- `gateway`: `mio`, `hmac`, `sha2`, `getrandom`, confined there.
- 156 crates in the lockfile, 123 from one decision — `openraft` + `tokio` in
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
| **The secret never crosses the wire** | With no TLS, anything a client *sends* is replayable — so the venue issues a nonce and the client returns `HMAC-SHA256` of it. |
| **Cancel-on-disconnect is opt-in** | A market maker needs its quotes pulled on disconnect; a week-long limit order does not. |
| **Replication is fenced by term** | A replaced leader cannot keep writing, and two leaders cannot acknowledge into diverging logs. |
| **Failover needs no person** | `openraft` runs a *separate* leadership log. What crosses into the venue is the term — which Raft already guarantees is unique per leader, and therefore *is* a fencing token. |

## Testing

325 tests. The ones worth reading:

| Test | What it proves |
|---|---|
| [`pipeline/tests/simulation.rs`](crates/pipeline/tests/simulation.rs) | The venue crashed repeatedly from a seed; recovery reproduces the last committed state order for order, and nothing uncommitted survives. |
| [`pipeline/tests/snapshot.rs`](crates/pipeline/tests/snapshot.rs) | A snapshot restart lands in exactly the state a full replay does — including queue position, not merely depth. |
| [`gateway/tests/over_tcp.rs`](crates/gateway/tests/over_tcp.rs) | Real sockets: a record torn across two writes, a slow client shed and rebuilding, cancel-on-disconnect withdrawing every quote. |
| [`gateway/tests/failover.rs`](crates/gateway/tests/failover.rs) | The binaries a deployment actually runs. The leader is killed mid-session and a node with an empty log is promoted at a higher term, then checked against everything the dead leader acknowledged. |
| [`gateway/tests/machine_down.rs`](crates/gateway/tests/machine_down.rs) | A standalone venue killed mid-trading recovers every acknowledged order on restart, refuses duplicates of them, and serves. A follower killed mid-trading stops nothing, and is backfilled to the leader's exact log when it returns. |
| [`gateway/tests/shipped_binaries.rs`](crates/gateway/tests/shipped_binaries.rs) | `venue` and `load` run as a pair against the shipped configuration files — the only tests that can catch the binaries and their config drifting apart. |
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
- **Encryption.** Auth proves who a session is at connect; it does not protect the orders
  after. The accepted cost of no TLS; the answer is a private link.
- **Withdrawals, halts and fees.** Venue features, not exchange-core ones.

## Related

The matching core grew out of
[**matching-engine**](https://github.com/hsdxpro/matching-engine) — the same bitmap ladder
in Rust and C++, both verified against independent reference models. That project is the
auditable engine on its own; this is the venue around it.

[**tick-to-trade**](https://github.com/hsdxpro/tick-to-trade) is the other side of the
wire: feed parsing, book maintenance and order entry, measured end to end.

## License

[MIT](LICENSE)
