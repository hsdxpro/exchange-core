# Architecture

## Fixed price domain

Prices are unsigned 16-bit integer ticks, so each side has a fixed 0–65,535 ladder. This intentionally trades a bounded 2 MiB level table for direct indexing, predictable memory access, and no tree/hash lookup on the hot path.

## Three-tier occupancy bitmap

Each side stores occupied prices as:

```text
root:      1 × u64
summary:  16 × u64
leaf:   1,024 × u64
```

A next/previous query checks the current leaf word, then at most one summary word and the root. It never walks empty price slots. Search work is bounded by the hierarchy, not by the numerical distance between populated prices.

## L2

Per side:

- `u32[65_536]` quantity ladder;
- hierarchical occupancy bitmap;
- cached best price;
- cached 64-bit total quantity.

| Operation | Complexity |
|---|---|
| Best bid/ask | O(1) |
| Set/modify/remove level | O(1), bounded bitmap maintenance |
| Next/previous occupied price | O(1), bounded hierarchy |
| Top K | O(K) |
| Sweep/VWAP | O(occupied levels visited) |

## L3 storage

Each price has a queue descriptor:

```text
PriceLevel — 16 bytes
u32 head
u32 tail
u64 total_quantity
```

Each resting order is an intrusive index-linked slot:

```text
OrderSlot — 24 bytes
u32 next
u32 previous
u32 order_id
u32 quantity
u32 free_next
u32 packed_side_and_price
```

The engine preallocates order slots and a dense `order_id -> slot` lookup table. Removed slots are returned to an intrusive free list. Engine operations allocate no heap memory after construction; fill delivery is caller-provided.

Approximate fixed memory:

```text
price-level tables: 2 × 65,536 × 16 B = 2 MiB
bitmap hierarchy:   about 16 KiB for both sides
order slots:         24 B × configured capacity
ID lookup:            4 B × configured ID space
```

## Matching contract

- Best price is consumed first.
- Within a price, queue head is consumed first.
- Execution price is the maker's resting price.
- A partial maker fill updates its quantity in place.
- A full maker fill unlinks and recycles its slot.
- Fill callbacks run only after the corresponding state transition is committed, so an exception cannot expose a half-applied fill.
- An amend that only reduces quantity retains queue position.
- Cancel/replace removes the original, assigns a new ID, and receives new priority.

### Time in force

- **GTC:** execute marketable quantity, then rest any remainder.
- **IOC:** execute marketable quantity, cancel any remainder.
- **FOK:** preflight visible executable quantity; reject atomically unless fully fillable.
- **Post-only:** reject atomically if marketable; otherwise rest.
- **Market:** represented by the extreme limit price and supports IOC/FOK only.

## Full-capacity preflight

A full book must not reject an aggressive GTC order merely because no slot is free before matching. The engine previews executable quantity:

- if the order fully executes, no slot is required;
- if matching fully removes at least one maker, that released slot can hold the remainder;
- if a passive remainder would require a slot and no match can release one, the order is rejected before mutating the book.

This preserves atomic rejection while correctly accepting full-book orders that create their own capacity.

## Arithmetic bounds

Order quantity is `u32`, price is `u16`, and execution-report totals are `u64`. Therefore a single incoming order bounds traded quantity to `u32::MAX` and notional to `u32::MAX × u16::MAX`, which fits in `u64`. The slot count is restricted to the 32-bit intrusive-index domain, so a level aggregate also remains below `u64::MAX`.

## Matching complexity

```text
O(number of fills + number of occupied price transitions)
```

The 1,000-level sparse sweep therefore visits 1,000 active bitmap positions and never scans the empty ticks between them.

## Core invariants

- A bitmap bit is set exactly when its level is non-empty.
- Cached BBO equals highest occupied bid / lowest occupied ask.
- The resting book is never crossed.
- Every live ID maps to exactly one live slot.
- Every live slot belongs to exactly one price queue.
- Queue head has no predecessor; queue tail has no successor.
- Level aggregate equals the sum of queued order quantities.
- Every used slot is exactly one of live or free.
- Engine operations perform no post-construction allocation.

## Implementation notes

The library uses:

- Edition 2024 and Rust 1.97.1;
- `Result<T, OrderError>` for explicit rejection paths;
- `let-else`, `is_some_and`, `matches!`, and standard-library integer bit operations;
- no third-party dependencies;
- `#![forbid(unsafe_code)]` in the engine library.

Linux CPU affinity is isolated to the benchmark binary's small documented FFI boundary.

The release profile keeps `panic = "unwind"`. The matching loop publishes each fill to the caller's callback only after that fill has been committed to the book, which guarantees the book is structurally valid if the callback unwinds mid-match. That guarantee only means anything under unwinding panics, and `catch_unwind` is the only way to test it.

The verification suite in `src/verify/` is part of the library, not a `cfg(test)` module. That makes the shipped binary self-verifying: it runs all 43 groups and refuses to print benchmark numbers if any fails. The cost is that the reference models ship in the library; they are small and depend on nothing outside `std`.

## Where this design costs you

Every choice here buys hot-path predictability with memory and flexibility. The honest bill:

- **The tick domain is fixed at 16 bits.** Prices are ladder indices, not values. A venue needing a wider range, or a tick size that varies by price band, needs a mapping layer above the engine or a wider ladder. A 32-bit ladder is not a drop-in change. At 4 billion entries the direct-indexing premise fails, and the level table would have to become sparse.
- **Memory is paid up front, per book.** Each `L3Book` allocates a 2 MiB level table (2 × 65,536 × 16 B) regardless of how many levels are live, plus `4 B × max_order_ids` for the direct-ID index. One instrument with a large ID space is cheap; ten thousand instruments each with their own ladder is not. Multi-instrument deployment wants a shared arena or a smaller per-book ladder, not this shape replicated.
- **Capacity is fixed at construction.** That is what makes operations allocation-free, and it means a full book rejects instead of growing. The full-capacity preflight exists precisely because "reject" has to be correct and atomic under pressure.
- **Order IDs are dense table indices, not identifiers.** `id_to_slot` is indexed by the order ID itself, which is what makes cancel and replace a single load instead of a hash lookup. The price is that IDs must be small contiguous integers. A venue using 64-bit exchange IDs or arbitrary client order IDs needs an allocator in front that maps them onto this index space, and that allocator becomes part of the hot path.
- **Quantity is `u32` and price is a 16-bit tick index.** Neither is a market quantity or a market price. A crypto venue quoting in base units overflows `u32` immediately (satoshis, let alone wei), and prices need a tick↔price mapping layer that is injective across the instrument's full range. Widening quantity to `u64` costs 4 bytes in `OrderSlot`, which breaks the 24-byte layout and its cache-line arithmetic.
- **There is no participant identity.** No account, no owner, no session. That rules out self-trade prevention, fee tiers, per-account risk and fill attribution. `OrderSlot` has no spare bytes either, so an owner ID is a layout decision, not a free field.
- **There is no timestamp and no sequence number.** Arrival priority is implicit in queue order, which is sufficient to match correctly but cannot answer when an order arrived. Audit, surveillance, and dispute resolution all need that, and it has to come from the sequencer above.
- **Only fills are published.** The engine reports executions through the fill callback and nothing else. There is no add/cancel/modify delta stream, so an L2 or L3 market data feed cannot be built from this without adding an event sink to `append` and `unlink`. Market data is not an afterthought at a real venue, and its absence here is structural, not cosmetic.
- **Panics are reachable on the matching path.** One invariant `expect` in the GTC capacity preflight, plus bounds-checked indexing on every slot and level access. A panic on a matching thread takes the venue down; a production build would either prove the indices safe or turn the failure into a deliberate, state-dumping abort.
- **Only three order actions exist.** There is no mass quote, no cancel-on-disconnect, and no good-till-date expiry. Market-maker flow is dominated by bulk requoting, and a dropped session that leaves live quotes in the book is a real risk, so a venue needs all three.
- **The bitmap wins on sparsity, not on density.** Against a dense book where every tick near the touch is occupied, a simple array scan of the top-N region would be competitive. The three-tier structure earns its keep when live prices are spread far apart, which is what this project measures and what a real book looks like away from the touch.

## How this would extend

The matching core is deliberately synchronous, single-threaded, and free of I/O. That is the right shape: a real exchange sequences messages *before* they reach matching, so the engine should be a pure function from an ordered command stream to an ordered event stream. Concretely, the pieces that would sit around it:

- **Sequencing and connectivity.** A gateway decodes the wire protocol and a sequencer assigns a total order. The engine then consumes that ordered stream. Because matching is deterministic and side-effect-free apart from the fill callback, replaying the same sequenced stream reproduces the book exactly. That is what makes journaling, recovery and hot-standby possible. The `deterministic_replay_is_reproducible` check pins this property.
- **Journaling and recovery.** Persist the sequenced input, not the book. Recovery is a replay. The pinned state hash is the integrity check.
- **Price protection.** Market orders currently sweep to the domain extreme. A venue bounds them with a collar around the touch or a price band, which in this design is just a tighter limit price. The matching loop does not change, only the policy that picks the limit.
- **Risk, accounts, and self-trade prevention.** These are pre-trade filters on the command stream plus a participant-ID field on the order slot. The slot is currently 24 bytes with no spare room, so adding an owner ID is a real layout decision, not a free field.
- **Multi-instrument.** Shard by instrument across threads, one book per shard, no shared mutable state. The fixed per-book memory cost above is the constraint that decides how many instruments fit per core.
