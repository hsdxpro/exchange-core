# Protocol

The wire, the journal and the snapshot formats. Layouts live in
`crates/protocol/src/lib.rs` and are asserted at compile time; this file is the
map, the source is the authority.

[README.md](README.md) &middot; [ARCHITECTURE.md](ARCHITECTURE.md) &middot;
[BENCH.md](BENCH.md) &middot; [DESIGN.md](DESIGN.md) &middot;
[ENGINEERING.md](ENGINEERING.md)

## Records

`Command` and `Event` are both **exactly 64 bytes**, `#[repr(C)]`,
little-endian, decoded zero-copy (`zerocopy`), size asserted at compile time.
One cache line each; a journal entry is the command verbatim.

Types: `AccountId u64`, `OrderId u64`, `SymbolId u32`, `Sequence u64`,
`Ticks i64`, `Quantity u64`. Integers only; no floats anywhere.

Several kinds reuse fields — a union by discriminant, documented per kind:

| Kind | Field reuse |
|---|---|
| `Deposit` / `Withdraw` | `symbol` = asset, `quantity` = amount |
| `Subscribe` / `Unsubscribe` / `Resume` | `quantity` = channel; `Resume` puts the next wanted sequence in `order_id` |
| `Authenticate` (+`Continued`) | 32 bytes of Ed25519 signature packed across `order_id`/`replacement_id`/`quantity`/`price` |
| `Challenge` | 16-byte nonce in `order_id`/`counterparty_order_id` |
| `Checkpoint` (+ signature halves) | 32-byte chain head packed across four fields |
| `Received` | `quantity` = `ingress_ns`, `price` = `match_ns` |

### Command kinds

`NewOrder 0`, `Cancel 1`, `AmendDown 2`, `CancelReplace 3`, `Deposit 4`,
`Subscribe 5`, `Unsubscribe 6`, `QueryOpenOrders 7`, `CancelOnDisconnect 8`,
`Authenticate 9`, `SetSymbolState 10`, `SetAccountTrading 11`, `CancelAll 12`,
`AuthenticateContinued 13`, `RevokeKey 14`, `Resume 15`, `Watermark 16`,
`Withdraw 17`.

Administrative (require the configured `admin_account`, checked in the gateway
before sequencing): `Deposit`, `Withdraw`, `SetSymbolState`,
`SetAccountTrading`, `RevokeKey`. `CancelAll` is deliberately unprivileged.
`Watermark` is journal-internal and refused from the wire.

### Event kinds

`Received 0`, `Rejected 1`, `Resting 2`, `Filled 3`, `Canceled 4`,
`BookDelta 5`, `Trade 6`, `BookSnapshot 7`, `OrderState 8`, `Challenge 9`,
`Authenticated 10`, `Bbo 11`, `SymbolState 12`, `AccountTrading 13`,
`Checkpoint 14`, `CheckpointSignature 15`, `CheckpointSignatureContinued 16`.

`RejectReason` has 22 values; the pipeline's reason table is compile-time
checked against the count, so a new reason cannot be added without naming it.

## Framing

Fixed 64-byte frames, both directions, **no length prefix** — a whole number
of records is a whole number of frames, and framing is arithmetic. The decoder
holds a fixed buffer (`max_records_per_session × 64`), keeps a partial tail
across reads, and drops undecodable whole records while counting them
(`bx_records_undecodable_total`).

## Doors

| Door | Config | Auth | Serves |
|---|---|---|---|
| Raw TCP | `listen` | Ed25519 challenge when `authentication = required` | orders, private account feed, optional public channels |
| TLS 1.3 | `tls_listen` (rustls, 1.3 only) | same | identical past the record layer |
| Feed | `feed_listen`, own thread and port | none | public channels only; `Subscribe`/`Unsubscribe`/`Resume`, everything else ignored; any private-channel request dropped |
| Multicast | `multicast`, up to two groups | none | same events as UDP packets, identical on A and B |
| Metrics | `metrics_listen` | none | Prometheus text, hand-rolled HTTP |
| Replication | `replica` lines | term fencing | leader → followers |

**Logon**: the venue issues a 16-byte nonce (`Challenge`); the client returns
an Ed25519 signature over a domain string and that nonce, split across
`Authenticate` + `AuthenticateContinued`. The venue holds public keys only —
reading its configuration yields nothing signable, and a signature made
elsewhere is not a logon. A session then acts for the account it proved (or,
on an open venue, the one its first command claimed). `keygen` mints pairs and
prints the private half exactly once.

## Channels

Every message carries `(channel, sequence)`; a gap is detected by arithmetic.

| Channel | Kind | Carries |
|---|---|---|
| `book.{symbol}` | 0 | `BookDelta` — **states** a level's absolute quantity (0 removes it), never adjusts; plus `SymbolState` |
| `trades.{symbol}` | 1 | `Trade` — anonymous: no accounts, no order IDs |
| `account` | 2 | private: `Received`/`Rejected`/`Resting`/`Filled`/`Canceled`/`OrderState`/`AccountTrading`; both sides of a fill are told, each seeing its own side. A session can only request its own — the channel is forced to the proven account |
| `bbo.{symbol}` | 3 | `Bbo`, one event per side; quantity 0 = side empty |
| `checkpoint` | 4 | venue-wide chain heads + two-record Ed25519 signature |

**Subscribe** answers state first: `BookSnapshot` events stamped with the
sequence the increments resume from (BBO restates with its own kind), so state
and change compose without gap or overlap. The feed thread rebuilds each
symbol's levels from the deltas it already forwards — a snapshot costs the
venue nothing.

**`RESUME {channel, last_seq}`** answers one of four ways, never silence: the
missed events; a restatement if the cursor fell outside the retention window;
a restatement if it named a sequence the channel never reached (what a cursor
from a previous leader looks like — numbering restarts with a venue); or, on
the private channel, `QueryOpenOrders` results for the named symbol — private
history is the client's own fills and cannot be restated to a ring.

## Journal, snapshot, chain

- **Journal**: 8-byte magic `BXJRNL␁␀`, then fixed 64-byte records. Seek to a
  sequence is arithmetic. A torn trailing record is truncated on open; a
  malformed complete record or a sequence jump refuses the file.
- **Snapshot**: magic `BXSNAPv5` (older versions refused). Header, orders in
  price-then-time order, balances, order-ID marks, symbol states, stopped
  accounts — every section sorted, so identical state writes identical bytes.
  Written to a side file, synced, renamed.
- **Chain** (optional): running SHA-256 sealed every 1,024 records, published
  as `Checkpoint` naming the first sequence it does *not* cover, followed by
  the venue's Ed25519 signature over `(sealed_at, head)` under a chain-specific
  domain string — a past commitment cannot describe the present, and the logon
  key cannot sign for the chain. Cannot be retrofitted over an existing
  journal; refused at the first append rather than overstating coverage.
- **Watermark**: journalled every 64 committed groups, at the *front* of its
  pass, meaning "everything before me was handed to the feed". Recovery
  regenerates private outcomes past the last mark and hands them to the
  account's next session at sequence 0 — told, not queried, and never the ack
  itself: durable is what the ack means.

## Multicast packets

MoldUDP64's shape, minus the per-message length prefix — an event is a fixed
64 bytes, so a packet is a header naming the run and position plus an array:
**21 events per packet**, sized so a full packet with IP and UDP headers stays
inside a 1,500-byte MTU (a fragmented packet turns one lost fragment into a
lost packet). Two groups, A and B, identical packets on independent paths — a
receiver takes whichever copy arrives first. Nothing on the packet path waits
for anyone; a receiver that misses asks `RESUME` on the feed port, served from
retention by the thread that owns it.
