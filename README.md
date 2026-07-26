# A crypto exchange core, in Rust

A matching engine and the venue around it: a binary protocol over TCP, order
books on a bitmap price ladder, balance reservation, an append-only journal that
is the single source of truth, a resumable market-data feed, snapshots, and
replication with quorum acknowledgement.

Everything below is measured on the machine it was developed on, not estimated.
`cargo x latency` reproduces it in about seven seconds.

## What it costs

| | |
|---|---:|
| passive limit order, full path | **188 ns** |
| crossing order, one fill | **224 ns** |
| cancel by order ID | **106 ns** |
| mixed stream | **171 ns** |
| three market-data subscribers attached | +11 ns |
| top-of-book feed, on every command | +8 ns |

"Full path" means sequence, journal, balance reservation, match, and event
emission — not the book in isolation.

The crossing and cancel figures were **3,036 ns and 447 ns** until the index that
answers "what does this account have working" stopped being searched linearly.
It is the same index that makes self-match prevention one lookup, and taking an
order out of it now costs a swap rather than a scan of the account's open
orders — which is worst for exactly the client a venue exists to serve, since a
market maker is defined by having thousands resting at once.

Durability is a different order of magnitude, and choosing between these two rows
is the largest decision in the design:

| commands/sec, durable | local fsync | quorum of replicas |
|---|---:|---:|
| group of 1 | 321 | 15,596 |
| group of 256 | 77,989 | 1,477,527 |
| group of 16,384 | 2,344,665 | **4,646,905** |

At a group of 16,384 the quorum path costs 215 ns per command — which is roughly
the compute cost above. Durability has become nearly free and the venue is bound
by matching again. Reaching another machine beats reaching the platter by **49×**
at a group of one, which is why the design acknowledges after a quorum rather
than after a flush.

Nothing in the code picks a group size. A group is whatever arrived since the
last pass, so it grows under load — exactly when a sync needs amortising — and
falls to one when the venue is idle and latency matters more.

Separate processes over loopback, which is what a client actually experiences.
The third row is a real cluster: one `venue` and two `replica` processes, a group
acknowledged once a majority holds it.

| durability | round trip, one order in flight | pipelined |
|---|---:|---:|
| none (journal in memory) | 11.4 µs | 1,796,009/sec |
| local disk, one `fsync` per group | 357 µs | 1,560,998/sec |
| **quorum of two followers** | **66.7 µs** | **1,785,257/sec** |

All three measured in one sitting, 200,000 orders each, so the rows are
comparable with each other rather than with some earlier machine.

Reaching two other processes is **5.4× faster than reaching the platter** on a
single order in flight, which is the whole argument for acknowledging after a
quorum — and the gap is far wider when there is nothing to amortise the sync
over, which is exactly when a client is waiting. Pipelined, the three converge:
once the group is large the sync is shared by thousands of orders and the venue
is bound by matching instead.

### Does it hold at scale

Same traffic spread over more accounts, every order resting:

| accounts | per command | holdings | balance memory |
|---|---:|---:|---:|
| 16 | 177 ns | 32 | ~0 |
| 100,000 | 390 ns | 200,000 | 9 MiB |
| **1,000,000** | **463 ns** | 2,000,000 | **91 MiB** |

About 2.16M commands a second at a million accounts. The 2.6× degradation is
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
| replay all 100,000 commands | 12.8 ms |
| snapshot + replay the last 5,000 | **2.0 ms** |

## Running it

Requires `rustup` and nothing else. There is no CI; `xtask` is the task runner.

```bash
cargo x
```

That is format, `clippy -D warnings`, and the whole test suite. Also:

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

To run it replicated, start followers first and list them in the config:

```bash
cargo run --release -p bx-gateway --bin replica -- 127.0.0.1:7201
```

## Layout

```
crates/engine/     the matching engine. No dependencies, forbid(unsafe_code).
crates/protocol/   wire records: fixed 64-byte layouts, asserted at compile time.
crates/journal/    append-only log, replay, replication with term fencing.
crates/pipeline/   sequencer, accounts, books, events, snapshots.
crates/gateway/    framing, sessions, admission, group commit, the TCP server, config.
xtask/             the task runner.
```

Thirty-three crates in the lockfile, and the engine has none of them: it forbids
`unsafe` and depends on nothing. `protocol`, `journal` and `pipeline` use only
`zerocopy` for fixed-layout casts. Everything else — `mio` for readiness, and
`hmac`/`sha2`/`getrandom` for the challenge — is in `gateway` alone, so nothing
on the matching path can reach a dependency.

[`DESIGN.md`](DESIGN.md) is the architecture. [`ENGINEERING.md`](ENGINEERING.md)
is the decisions, what was rejected and why, and the bugs worth remembering.

## Things worth knowing about the design

**One transport, unencrypted, as fast as the machine allows.** Fixed 64-byte
records over TCP with no TLS, so a market maker can be given a direct connection
with nothing between it and the book. QUIC was built and then removed: measured
against the same venue it cost 38.6 µs of round trip against TCP's 8.6 µs, and
1.48M orders a second against 3.76M. It buys NAT traversal, mobile resilience and
per-stream flow control, none of which is worth 4.5x the latency here.

**One thread, no async runtime.** Matching a book is a sequential dependency —
each order changes what the next one sees — so there is no parallelism in it to
take. Sessions are found by readiness notification rather than scanned, which is
what keeps an idle connection at 23 ns a pass.

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

**The secret never crosses the wire.** Taking no TLS is what decides the shape of
authentication: anything a client *sends* can be captured and replayed, so a
bearer token or an API key would be worth exactly as much as reading the wire.
The venue puts a fresh 16-byte nonce on the connection the moment it is accepted
and the client returns `HMAC-SHA256` of it. What an eavesdropper gets is a nonce
that will never be issued again and a tag that answers it. A client may pipeline
its opening orders directly behind the proof, so admission costs one round trip
rather than two.

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
- **Encryption.** Authentication proves who a session is at connect; it does not
  protect the orders after it. On a link an attacker can *write* to, orders can
  still be injected into an admitted session. That is the accepted cost of
  taking no TLS, and the answer is a private link — which is the deployment this
  transport exists for.
- **Withdrawals and trading halts.** Venue features rather than exchange-core
  ones.
- **Fees.** The design puts them at settlement so they never touch matching;
  nothing applies them yet.

## Correctness

285 tests. The ones worth looking at:

- `crates/pipeline/tests/simulation.rs` — the venue crashed repeatedly from a
  seed, asserting after every crash that recovery reproduces the last committed
  state order for order, and that nothing uncommitted survived.
- `crates/pipeline/tests/snapshot.rs` — a restart from a snapshot lands in
  exactly the state a full replay of the same journal does, including queue
  position, not merely the same depth.
- `crates/gateway/tests/over_tcp.rs` — real sockets, including a record torn
  across two writes, a client shed for being slow that reconnects and rebuilds,
  and cancel-on-disconnect withdrawing every quote.
- `crates/journal/src/replication.rs` — a replaced leader is refused and its
  write never reaches the follower's log.
- `crates/gateway/tests/idle_cost.rs` — a benchmark that fails if an idle
  connection ever costs more than 120 ns a pass again.
- `crates/gateway/tests/failover.rs` — the same binaries a deployment runs: the
  leader is killed mid-session and a node with an empty log is promoted at a
  higher term, then checked against everything the dead leader acknowledged.
  This is the one property that cannot be tested in a single process, and it
  found a leader whose journal held nothing but its magic bytes after ten
  thousand acknowledged orders.
