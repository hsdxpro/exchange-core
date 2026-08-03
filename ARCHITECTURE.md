# Architecture

Where to start reading, and where to change things.

[README.md](README.md) &middot; [PROTOCOL.md](PROTOCOL.md) &middot;
[BENCH.md](BENCH.md) &middot; [DESIGN.md](DESIGN.md) &middot;
[ENGINEERING.md](ENGINEERING.md)

`cargo doc --workspace --open` for API docs. Modules carry `//!` headers
stating why they exist.

## Crates

Dependencies run one way, top to bottom. No crate on the matching path has a
dependency it could reach; the engine stands alone.

| Crate | Owns | Depends on |
|---|---|---|
| `bx-engine` | The book: `L3Book`, `HierarchicalBitmap`, order slots, matching. No dependencies, `forbid(unsafe_code)`, knows nothing about accounts. 43 named verify checks compiled into the lib. | — |
| `bx-protocol` | Wire vocabulary: 64-byte `Command`/`Event`, snapshot records, enums, domain strings. Layouts asserted at compile time. | zerocopy |
| `bx-journal` | Append-only log, arithmetic seek, torn-write truncation, SHA-256 chain, quorum replication with term fencing. | protocol |
| `bx-pipeline` | `Exchange`: sequencer, accounts and balances, instrument table, book adapter, event hub, snapshots. The deterministic core. | protocol, journal, engine |
| `bx-gateway` | Sessions, framing, admission (Ed25519), rate limits, group commit, TCP/TLS servers, feed thread, multicast, metrics, config. | all above, election |
| `bx-election` | Leader election via `openraft`, on a leadership log the orders never touch. Used by the `venue` binary only. | openraft, tokio |
| `xtask` | Task runner: `cargo x` = fmt, clippy `-D warnings`, all tests. | — |

Binaries: `venue`, `load`, `replica`, `keygen` (gateway), `latency`, `profile`
(pipeline), `bx-bench` (engine — verifies its 43 check groups before printing a
single number).

Everything after the sequencer is a deterministic function of the sequenced
stream — no clock reads, no randomness, no map iteration order reaching output.
Timestamps are captured in the gateway and travel *inside* the command; that is
what makes replay a recovery mechanism rather than an approximation.

## Life of a command

**1. Bytes arrive** — `gateway/src/tcp.rs`, `Server`

`mio` readiness, edge-triggered; one socket read per session per pass (bounds
the group one client can force), sessions kept in the ready set until a read
returns `WouldBlock`. Records are a fixed 64 bytes; framing is arithmetic, no
length prefix (`codec.rs`, fixed decoder buffer, undecodable records counted).

**2. Admission** — `gateway/src/auth.rs`, `limit.rs`

Before sequencing, so no key lookup, nonce or clock reading reaches the
deterministic path: Ed25519 challenge (the venue holds public keys only), a
session acts for the account it proved, per-account token bucket, admin-only
commands checked here. A refused command is never sequenced.

**3. Group commit** — `gateway/src/venue.rs`

A group is whatever arrived since the last pass — nothing picks a size. The
group is appended to the journal (`enqueue`), replicated to a quorum and synced
(`commit`), and only then are its events released: **the ack means received and
durable, not accepted**.

**4. Risk** — `pipeline/src/lib.rs`, `Exchange::apply_new_order`

Checks in order, first refusal wins: symbol listed → symbol `Trading` →
account not stopped → quantity non-zero and ≤ `max_quantity` → duplicate
`(account, order_id)` → order ID strictly increasing per account → self-match
(cancel-newest, answered in one lookup for accounts with nothing resting) →
balance reserved → price inside the ladder (**the band check**:
`instrument.to_slot`, inside the book adapter) → slot allocated. An accepted
order spends its ID whatever the outcome; only a refusal *before* the
reservation leaves it reusable — a band-rejected resend needs a fresh ID.

**5. Matching** — `engine/src/lib.rs`, `L3Book`

Price-time FIFO; executions print at the maker's price. A market order is a
limit at the most aggressive tick — one matching loop, and because IOC/FOK
never rest, the extreme never grows anything. Fills are delivered by callback
after they are committed, so a panicking callback leaves the book valid. The
steady state allocates nothing; the slot pool doubles when it fills and the
price window grows toward a resting price it missed, both amortised and off
the common path.

**6. Settlement** — `pipeline/src/accounts.rs`, `Exchange::settle`

Per execution: buyer pays quote and receives base, seller the reverse, both
against their reservations; a limit buy that traded below its limit gets the
difference released. Any "cannot fail" accounting step that fails increments a
process-wide violations counter the tests assert is zero.

**7. Events** — `pipeline/src/hub.rs`

Per-channel monotonic sequences: `book.{symbol}` (depth deltas that *state*
levels, plus symbol state), `trades.{symbol}` (anonymous), `bbo.{symbol}`,
`account` (private lifecycle: both sides of a fill are told), `checkpoint`
(chain heads). Deltas derive from the touched-price list; top-of-book events
only when the top moved.

**8. Distribution** — `gateway/src/feed.rs`, `handoff.rs`, `multicast.rs`

Private events go out on the trading session. Public channels cross one
bounded handoff (64 recycled buffers, drops rather than blocks, wakes a
`mio::Waker` capped at one per 50 µs) to the feed thread: own port, no
credentials, snapshot-on-subscribe rebuilt from the deltas it already
forwards, `RESUME` served from retention rings. Multicast sends the same
events as UDP packets on up to two identical A/B groups.

## Threads

| Thread | Started by | Owns |
|---|---|---|
| trading loop | `venue` | everything on the order path: sockets, sequencer, journal, books, balances |
| feed distributor | `feed_listen` | public-feed sockets, retention rings, multicast |
| metrics exporter | `metrics_listen` | the scrape listener; `publish` uses `try_lock` and drops rather than blocking |
| tokio runtime | `peer` lines | `openraft`; two atomics cross back — `is_leader`, `term` |

One writer applies every command in arrival order; the book takes no locks.
More cores = more `venue` processes over disjoint symbol sets, each its own
journal, replication, leadership and failure domain — not more threads
(measured and rejected; [ENGINEERING.md](ENGINEERING.md)).

## State

| State | Lives in | Snapshotted |
|---|---|---|
| resting orders, levels, best prices | `L3Book` per instrument | yes, price-then-time order |
| balances (free, reserved) | `Accounts` | yes |
| per-account highest order ID | `Exchange` | yes (`SnapshotOrderIdMark`) |
| symbol trading states, stopped accounts | `Exchange` | yes |
| chain head | journal | yes, carried in the snapshot |
| sessions, subscriptions, cursors | gateway | no — reconnect restates |

Snapshot `BXSNAPv5`: every section sorted so identical state writes identical
bytes; written beside the journal, synced, renamed. Recovery = newest snapshot
+ replay from the next sequence, bit-identical to what was lost; replay seeks
by arithmetic (`HEADER_LEN + seq × 64`) and runs in 4,096-command chunks to
bound recovery memory.

## Changing things

| Change | Open |
|---|---|
| add a command | `protocol` (`CommandKind`), `pipeline::apply`, admission rules in `gateway` if privileged |
| add a reject reason | `protocol` (`RejectReason`), the pipeline's `REASONS` table (compile-time size-checked) |
| add a risk check | `Exchange::apply_new_order`, in refusal order |
| add a market-data channel | `protocol` (`ChannelKind`), `hub.rs`, `feed.rs` retention |
| change the wire layout | `protocol`, then [PROTOCOL.md](PROTOCOL.md), then the layout tests |
| touch the book | `engine/src/lib.rs` — run `bx-bench` (verification precedes numbers) |
| add an operator control | `protocol`, `pipeline::apply`, admin check in the gateway, journal + snapshot if it must survive restart |
| add config | `gateway/src/config.rs` — unknown keys are errors, every value validated, cross-checks named |

## Invariants

Each is tested. Breaking one is a correctness bug, not a style regression.

**Determinism after the sequencer.** No clock, no randomness, no map order
reaching output. A 100,000-command golden replay hash pins it in the engine;
`simulation.rs` pins it across crash and recovery; `chain.rs` pins two venues
agreeing on the head from the same stream.

**Ack = durable.** A group's events are released only after its commit reaches
a quorum. A replaced leader is fenced by term and cannot write.

**Single writer.** One thread owns the venue's state. Nothing on the order
path takes a lock or reads a clock (rate limiter and timestamps: once per
pass, only when configured).

**Value is conserved.** Trading moves value, never creates it; the violations
counter stays zero across every end-to-end and crash suite; withdrawals cannot
touch reserved collateral; moving an allotment between partitions strands on a
crash rather than minting (withdraw sequenced before deposit).

**Fixed 64-byte records.** Commands, events and journal entries; compile-time
asserted. Framing and replay-seek stay arithmetic because of it.

**Restrictions stop new risk, never reducing it.** Halted symbols and stopped
accounts still accept cancels; both survive snapshot and replay.

## Capacity model

Nothing structural refuses an order below the `u32` slot-index space — 4.29
billion resting per book, arithmetic rather than configuration.

- **Slot pool**: `max_open_orders` sizes the boot allocation (40 bytes a
  slot, ~55 live, pinned by test); the pool, its allocator and the engine's
  ID table double together when it fills. Slots are indices, so growth moves
  nothing a live order holds. The pool is shared across prices and sides:
  one price can hold every order in the book.
- **Price window**: the engine's price domain is 31 bits per instrument; the
  level tables boot 65,536 ticks (~2.1 MiB) and follow the prices that rest —
  re-anchored free while the book is empty, extended upward without a
  rebuild, shifted downward with one copy per doubling. Slots store absolute
  prices, so no growth touches an order. The instrument's band
  (`floor_ticks`..`ceiling_ticks`, config) is the policy bound — the
  fat-finger control and the worst-case window statement in one, since the
  window is dense between the lowest and highest resting price at 16 bytes a
  tick per side.
- **Feed retention**: `retained_per_channel` × instruments × 3 public
  channels is checked against `max_feed_memory_mb` at startup — a config
  that would OOM is refused before it serves.
- **Accounts and holdings** grow with use; an account that never traded
  costs nothing.
