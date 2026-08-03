//! The exchange pipeline: sequence, journal, reserve, match, publish.
//!
//! [`Exchange`] is the deterministic core. Feed it the same sequenced commands
//! and it produces the same state and the same events, every time, on any
//! machine. That is what makes replay a real recovery mechanism rather than an
//! approximation, and it is why nothing in here reads a clock, uses randomness,
//! or lets `HashMap` iteration order reach the output.

pub mod accounts;
pub mod book;
pub mod fastmap;
pub mod hub;
pub mod instrument;
pub mod snapshot;

use accounts::Accounts;
use book::{Execution, Outcome};
use bx_journal::{Journal, LogStorage};
use bx_protocol::{
    AccountId, ChannelKind, Command, CommandKind, Event, EventKind, OrderId, Quantity,
    RejectReason, Sequence, Side, SnapshotBalance, SnapshotOrder, SnapshotOrderIdMark,
    SnapshotStoppedAccount, SnapshotSymbolState, SymbolId, Ticks, TimeInForce, TradingState,
    checkpoint_message,
};
use ed25519_dalek::{Signer, SigningKey};
use fastmap::FastMap;
use instrument::{Instrument, Instruments};
use snapshot::{Snapshot, balance_of};
use std::sync::atomic::{AtomicU64, Ordering};
use zerocopy::IntoBytes;

/// Every reason an order can be refused, so a count can be reported against a
/// name rather than a number. Listing them costs a compile-time check that
/// nothing was forgotten, which a `0..N` loop over discriminants would not give.
const REASONS: [RejectReason; 22] = [
    RejectReason::None,
    RejectReason::UnknownAccount,
    RejectReason::UnknownSymbol,
    RejectReason::QuantityZero,
    RejectReason::QuantityTooLarge,
    RejectReason::DuplicateOrderId,
    RejectReason::UnknownOrderId,
    RejectReason::OutsidePriceBand,
    RejectReason::InsufficientBalance,
    RejectReason::OrderLimitReached,
    RejectReason::WouldCross,
    RejectReason::InsufficientLiquidity,
    RejectReason::AmendWouldIncrease,
    RejectReason::SelfMatchPrevented,
    RejectReason::UnsupportedTimeInForce,
    RejectReason::EngineCapacity,
    RejectReason::NotAuthenticated,
    RejectReason::RateLimited,
    RejectReason::OrderIdNotIncreasing,
    RejectReason::SymbolNotTrading,
    RejectReason::AccountNotTrading,
    RejectReason::NotPermitted,
];

/// Slots in the reject table, one per reason.
///
/// Taken from the protocol rather than from `REASONS.len()`. This array is for
/// *reporting* counts against names and a reason missing from it merely goes
/// unreported, but the table is indexed by discriminant -- so sizing it here
/// meant a reason appended to the enum and forgotten in this list panicked on
/// the first refusal that used it.
pub const REJECT_REASONS: usize = RejectReason::COUNT;

const _: () = assert!(
    REASONS.len() == REJECT_REASONS,
    "a reason was added to the protocol but not to REASONS, so its count \
     would never be reported"
);

/// The books, in a table indexed by symbol.
///
/// A `BTreeMap` here cost a tree descent on the one lookup *every* command
/// performs: about ten branchy comparisons and as many pointer hops at a
/// thousand instruments. Measured, that was 175 ns a command at one symbol
/// against 352 at a thousand — a venue paying twice as much per order for the
/// crime of listing a realistic number of instruments.
///
/// Instruments were already held exactly this way and the books beside them were
/// not, which is the kind of asymmetry that survives only because nothing
/// measured the wide case.
///
/// Symbols are venue-assigned and `MAX_SYMBOL` refuses a sparse numbering, so a
/// dense table is bounded by the listing rather than by the largest identifier
/// anybody typed. Iterating it in index order is ascending by symbol, which is
/// the same order the tree gave — so snapshots stay byte-identical.
#[derive(Debug, Default)]
struct Books {
    by_symbol: Vec<Option<book::Book>>,
}

impl Books {
    fn insert(&mut self, symbol: SymbolId, book: book::Book) {
        let index = symbol as usize;
        if index >= self.by_symbol.len() {
            self.by_symbol.resize_with(index + 1, || None);
        }
        self.by_symbol[index] = Some(book);
    }

    #[inline]
    fn get(&self, symbol: SymbolId) -> Option<&book::Book> {
        self.by_symbol.get(symbol as usize)?.as_ref()
    }

    #[inline]
    fn get_mut(&mut self, symbol: SymbolId) -> Option<&mut book::Book> {
        self.by_symbol.get_mut(symbol as usize)?.as_mut()
    }

    /// Every listed book, ascending by symbol.
    fn iter(&self) -> impl Iterator<Item = (SymbolId, &book::Book)> {
        self.by_symbol
            .iter()
            .enumerate()
            .filter_map(|(symbol, held)| held.as_ref().map(|book| (symbol as SymbolId, book)))
    }
}

/// Accounting operations that failed when they should have been impossible.
/// Any non-zero value means value was created or destroyed; tests assert it
/// stays at zero.
static VIOLATIONS: AtomicU64 = AtomicU64::new(0);

/// Number of impossible accounting failures observed since start.
#[must_use]
pub fn accounting_violations() -> u64 {
    VIOLATIONS.load(Ordering::Relaxed)
}

/// What an order tied up, so it is released exactly when the order ends.
#[derive(Clone, Copy, Debug)]
struct Reservation {
    account: AccountId,
    symbol: SymbolId,
    side: Side,
    /// Price the hold was sized at. A buy that trades cheaper gets the
    /// difference back.
    limit_price: Ticks,
    remaining: u128,
    /// Where this order sits in its account's resting list, so taking it out is
    /// a swap rather than a search.
    ///
    /// `u32` because an account cannot have more orders resting on a symbol than
    /// the instrument's slot pool holds, and that is itself a `u32`. It also
    /// costs nothing: the field lands in padding the struct already had.
    at: u32,
}

#[derive(Debug)]
pub struct Exchange<S: LogStorage> {
    journal: Journal<S>,
    accounts: Accounts,
    instruments: Instruments,
    /// A table indexed by symbol, so finding a book is a bounds check.
    books: Books,
    /// Keyed by account *and* client order ID. A hash map, not a tree: this is
    /// looked up several times per command and nothing iterates it, so ordering
    /// is not needed and O(log n) tree descents are pure cost.
    ///
    /// It grows from empty rather than being sized to `max_open_orders`, and
    /// that is deliberate. Pre-sizing it looks obviously right -- the bound is
    /// already declared, every book pre-allocates its slot pool from it, and
    /// rehashing on the way up lands on one unlucky order as a spike. Measured,
    /// it was 27% *worse* on the command path: a table sized for the maximum
    /// spans tens of megabytes and every probe into it misses, where a table
    /// grown to the live set stays dense enough to stay cached. The rehashes
    /// cost less than the misses they would have avoided.
    ///
    /// The account is in the key because IDs were once venue-global, so the
    /// second account to use ID 1 was refused as a duplicate -- no attacker
    /// required, just two clients that both number their orders from one, which
    /// is what a client library does by default and what FIX and OUCH namespace
    /// per client to avoid. The key is sixteen bytes rather than eight; what
    /// that costs is in the README's command-path table, measured either side.
    reservations: FastMap<crate::book::OrderKey, Reservation>,
    /// Orders refused, by reason. A fixed array indexed by the discriminant, so
    /// counting one costs an increment and nothing on the path that accepts.
    rejects: [u64; REJECT_REASONS],
    /// When the group now being applied began matching, supplied from outside.
    ///
    /// An *input*, never a clock reading taken here. Everything after the
    /// sequencer has to be a function of the sequenced stream, so the pipeline
    /// is handed the time rather than asking for it — the same rule that keeps
    /// authentication and rate limiting in the gateway. Zero when the venue runs
    /// without timestamps, and zero on replay, which is why it is published as a
    /// measurement of the run rather than a fact about the order.
    matched_ns: u64,
    /// Which orders each account has resting on each symbol.
    ///
    /// Serves two callers that would otherwise each want their own index.
    /// Self-match prevention asks "could this possibly trade against itself",
    /// which is emptiness and so one lookup -- an account with nothing resting on
    /// the symbol cannot self-match, which is the overwhelming majority of
    /// orders, and they skip the check entirely. An open-orders query asks which
    /// ones, which is the list itself, so answering costs the account's own order
    /// count rather than a scan of every order in the venue.
    ///
    /// One entry per account that is actually resting something, so memory is
    /// bounded by open orders rather than by how many accounts exist.
    resting_per_account: FastMap<(AccountId, SymbolId), Vec<OrderId>>,
    /// The highest order ID each account has ever had accepted.
    ///
    /// Order IDs must increase per account, and this is what enforces it. The
    /// reason is retry safety, not tidiness. A client that loses its connection
    /// after sending an order and before reading the acknowledgement cannot tell
    /// whether the order arrived. Resending risks trading twice; not resending
    /// risks never trading at all. Neither is acceptable, and no amount of care
    /// on the client's side fixes it, because the ambiguity is on the wire.
    ///
    /// With this, a resend of an ID already accepted is refused, so the client
    /// may retry freely: at most one of its attempts can ever be live. The
    /// [`RejectReason::DuplicateOrderId`] check above only covers an order still
    /// *resting* -- an order that had already filled left no trace, and the
    /// resend of it was accepted and filled a second time.
    ///
    /// One `u64` per account that has ever traded, and one comparison on the
    /// order path. It is part of the snapshot because it has to be: recovery
    /// from a snapshot replays only the journal after it, so a mark rebuilt from
    /// that alone would forget every ID from before and start accepting them
    /// again.
    highest_order_id: FastMap<AccountId, OrderId>,
    /// Whether each symbol accepts new orders, indexed by symbol like the books.
    ///
    /// A table rather than a map because it is read on every order and the
    /// symbol is already an index. `Trading` is the default, so a symbol nobody
    /// has touched behaves exactly as before this existed.
    symbol_state: Vec<TradingState>,
    /// Accounts stopped from opening new risk. The kill switch.
    ///
    /// Holds only the stopped ones, so the common case is one lookup that finds
    /// nothing and the table stays empty on a healthy venue. It is in the
    /// snapshot for the same reason the order-ID marks are: an operator's halt
    /// must survive a recovery, or the venue comes back letting through exactly
    /// what was stopped.
    stopped_accounts: FastMap<AccountId, ()>,
    events: Vec<Event>,
    /// Set once [`Self::commit`] has handed the current batch's events out, so
    /// the next enqueue knows to start a fresh batch rather than append to one
    /// the caller has already seen.
    released: bool,
    event_sequence: Sequence,
    /// The last chain head handed to subscribers.
    ///
    /// Compared against the journal's to notice a seal without asking the journal
    /// to remember whether anybody has been told. Zero until the first
    /// checkpoint, which is also the head of an empty chain -- correct either
    /// way, because there is nothing to publish about a venue that has taken no
    /// commands.
    published_head: [u8; 32],
    /// The venue's identity, for signing what it commits to.
    ///
    /// Absent by default, and the chain still works without it -- a client can
    /// still fold the stream and catch a venue that contradicts itself. What the
    /// key adds is the case a self-consistent chain cannot cover: a venue that
    /// rewrites its history and republishes a head over the rewritten stream.
    ///
    /// Held here rather than in the gateway because the signature is emitted as
    /// part of the event stream, and everything in that stream has to come from
    /// the deterministic core.
    chain_key: Option<SigningKey>,
    /// Private outcomes regenerated by a replay, past the last watermark, keyed
    /// by the account they belong to.
    ///
    /// Filled only during recovery and drained as accounts reconnect, so a
    /// client whose outcome event died with the old leader is told rather than
    /// left to query. Bounded by the watermark interval: everything before the
    /// last watermark was already handed to the feed and is dropped on sight.
    pending_outcomes: FastMap<AccountId, Vec<Event>>,
    /// Reused across commands. Swapped into the book for the duration of a
    /// call so the book can fill it while `self` stays borrowable.
    scratch: Outcome,
}

impl<S: LogStorage> Exchange<S> {
    /// # Errors
    /// Fails if the journal cannot be read.
    pub fn new(storage: S, instruments: Instruments) -> bx_journal::Result<Self> {
        let mut books = Books::default();
        for instrument in instruments.iter() {
            books.insert(instrument.symbol, book::Book::new(*instrument));
        }
        Ok(Self {
            journal: Journal::open(storage)?,
            accounts: Accounts::new(),
            instruments,
            books,
            reservations: FastMap::default(),
            rejects: [0; REJECT_REASONS],
            matched_ns: 0,
            resting_per_account: FastMap::default(),
            highest_order_id: FastMap::default(),
            symbol_state: Vec::new(),
            stopped_accounts: FastMap::default(),
            published_head: [0; 32],
            chain_key: None,
            pending_outcomes: FastMap::default(),
            events: Vec::new(),
            released: true,
            event_sequence: 0,
            scratch: Outcome::default(),
        })
    }

    /// Credits an account, journalling it so replay reproduces the balance.
    ///
    /// Balances used to live only in memory, which meant a restart recovered
    /// every order but none of the money behind them, and recovery only looked
    /// correct because the caller re-applied deposits by hand.
    ///
    /// # Errors
    /// Fails only if the journal cannot be written.
    pub fn deposit(
        &mut self,
        account: AccountId,
        asset: instrument::AssetId,
        amount: Quantity,
    ) -> bx_journal::Result<()> {
        let mut command = deposit(account, asset, amount);
        self.submit(&mut command)?;
        Ok(())
    }

    #[must_use]
    pub fn accounts(&self) -> &Accounts {
        &self.accounts
    }

    #[must_use]
    pub fn book(&self, symbol: SymbolId) -> Option<&book::Book> {
        self.books.get(symbol)
    }

    #[must_use]
    pub fn next_sequence(&self) -> Sequence {
        self.journal.next_sequence()
    }

    #[must_use]
    pub fn open_orders(&self) -> usize {
        self.reservations.len()
    }

    /// Hands back the journal storage, so a fresh exchange can be built over
    /// the same log and recovered from it. This is what a restart does.
    pub fn into_storage(self) -> S {
        self.journal.into_storage()
    }

    /// Sequences, journals and applies one command **without making it
    /// durable**, and without releasing its events.
    ///
    /// This is the hot path, and it deliberately makes no `fsync`. A sync costs
    /// milliseconds while everything else here costs nanoseconds, so syncing per
    /// command puts an operation eighteen thousand times the cost of matching
    /// directly in front of matching. The caller enqueues as much as it has and
    /// calls [`Self::commit`] once, which is the group commit every venue does.
    ///
    /// Events are buffered, not returned: nothing may be shown to a client
    /// before the command that caused it is durable, because an acknowledgement
    /// that has to be retracted is worse than one that took longer.
    ///
    /// # Errors
    /// Fails only if the journal cannot be written. A refused command is not an
    /// error: it produces a `Rejected` event.
    pub fn enqueue(&mut self, command: &mut Command) -> bx_journal::Result<()> {
        if self.released {
            self.events.clear();
            self.released = false;
        }
        self.journal.append(command)?;
        self.apply(*command);
        Ok(())
    }

    /// Makes everything enqueued durable, then releases its events.
    ///
    /// One sync covers the whole group. That is not a weakening of durability:
    /// no command in the group is acknowledged until every one of them is on
    /// disk. If the sync fails nothing is released, so no client is ever told
    /// about a command that is not durable.
    ///
    /// # Errors
    /// Fails if the journal cannot be flushed.
    pub fn commit(&mut self) -> bx_journal::Result<&[Event]> {
        self.journal.sync()?;
        // A head that moved during this group is published with it, so a client
        // learns what the venue committed to at the same time it learns the group
        // was durable. Nothing to publish when chaining is off, and nothing when
        // the group did not cross an interval boundary.
        let head = self.journal.chain_head();
        if self.journal.chaining() && head != self.published_head {
            self.published_head = head;
            // What the head covers, not where the log has reached. Between
            // boundaries those differ, and publishing the latter would claim
            // coverage of records the head does not commit to.
            let covered = self.journal.chain_sealed_at();
            let mut event = Event::checkpoint(covered, &head);
            event.sequence = self.event_sequence;
            self.event_sequence += 1;
            self.events.push(event);

            // The venue's own signature over that commitment, when it holds a
            // key. Without one the chain shows only that the venue agrees with
            // itself: a venue that rewrote its history could publish a head over
            // the rewritten stream and nothing would contradict it.
            //
            // Signed here rather than at the publisher so the signature is an
            // ordinary event on the checkpoint channel -- numbered with
            // everything else, retained in the same ring, and served to a
            // resuming client by the same path. Ed25519 is deterministic, so a
            // replay on a node holding the same key reproduces it exactly.
            if let Some(key) = &self.chain_key {
                let signature = key.sign(&checkpoint_message(covered, &head)).to_bytes();
                for mut half in Event::checkpoint_signature(covered, &signature) {
                    half.sequence = self.event_sequence;
                    self.event_sequence += 1;
                    self.events.push(half);
                }
            }
        }
        self.released = true;
        Ok(&self.events)
    }

    /// Enqueues one command and commits immediately.
    ///
    /// The right thing when a command arrives alone, but it pays a full sync
    /// for a single command. Anything driving real traffic should enqueue a
    /// group and [`Self::commit`] once.
    ///
    /// # Errors
    /// Fails only if the journal cannot be written.
    pub fn submit(&mut self, command: &mut Command) -> bx_journal::Result<&[Event]> {
        self.enqueue(command)?;
        self.commit()
    }

    /// Enqueues a whole batch and commits once. The throughput path.
    ///
    /// # Errors
    /// Fails only if the journal cannot be written.
    pub fn submit_batch(&mut self, commands: &mut [Command]) -> bx_journal::Result<&[Event]> {
        for command in commands.iter_mut() {
            self.enqueue(command)?;
        }
        self.commit()
    }

    /// Rebuilds state by replaying the whole journal. Events are not
    /// re-emitted; subscribers were told the first time.
    ///
    /// # Errors
    /// Fails if the journal is unreadable, corrupt, or has a gap.
    pub fn recover(&mut self) -> bx_journal::Result<u64> {
        // Explicit rather than relying on a freshly opened journal already being
        // in this state: a full replay starts the chain from nothing and accounts
        // for every record, which is exactly what makes appending safe afterwards.
        self.journal.restore_chain(bx_journal::EMPTY_CHAIN, 0);
        self.replay_from(0)
    }

    /// Restores a snapshot, then replays only the journal after it.
    ///
    /// The result is identical to [`Self::recover`]; the snapshot is purely an
    /// optimisation, and discarding every snapshot costs recovery time and
    /// nothing else.
    ///
    /// # Errors
    /// Fails if the journal is unreadable, corrupt, or has a gap.
    pub fn recover_from(&mut self, snapshot: &Snapshot) -> bx_journal::Result<u64> {
        self.restore(snapshot);
        // State resumes at the snapshot's sequence. The *chain* has to resume
        // wherever its head was last sealed, which is at or before that: a
        // snapshot taken mid-interval carries a head covering only up to the
        // previous boundary, and the records between the two were folded into a
        // digest nothing persisted. Replaying from the snapshot alone would leave
        // them out of the chain for good, so the venue would publish a head no
        // client could reproduce -- silent divergence, in the one feature whose
        // whole purpose is not needing to be trusted.
        //
        // So the read starts at the boundary and the *application* starts at the
        // snapshot. At most one interval of records is read twice and applied
        // once, which is a thousand records on a recovery.
        //
        // Only when chaining is on. Without it there is no head to keep
        // consistent and `chain_sealed_at` is zero, so taking the minimum would
        // rewind every recovery to the start of the journal and throw away the
        // snapshot's entire purpose.
        let read_from = if self.journal.chaining() {
            snapshot.sequence.min(self.journal.chain_sealed_at())
        } else {
            snapshot.sequence
        };
        self.replay_from_folding(read_from, snapshot.sequence)
    }

    /// Replays from `start` in fixed-size chunks.
    ///
    /// Bounded memory on purpose. Collecting the journal into one `Vec` first
    /// made peak recovery memory scale with the length of the log -- 64 bytes a
    /// command, so a venue that had taken a hundred million orders needed
    /// gigabytes to come back. Recovery is precisely when a node is least able
    /// to find them, since it is usually recovering because something already
    /// went wrong.
    ///
    /// Re-seeking each chunk is free: [`Replay::from_sequence`] is arithmetic on
    /// a fixed record width, not a scan.
    fn replay_from(&mut self, start: Sequence) -> bx_journal::Result<u64> {
        self.replay_from_folding(start, start)
    }

    /// Replays from `read_from`, applying only what is at or after `apply_from`.
    ///
    /// The two differ only when recovering from a snapshot taken between chain
    /// boundaries: everything from the last boundary has to reach the chain, while
    /// only what the snapshot does not already contain may reach the books.
    fn replay_from_folding(
        &mut self,
        read_from: Sequence,
        apply_from: Sequence,
    ) -> bx_journal::Result<u64> {
        /// 256 KiB of commands. Large enough that the per-chunk seek disappears
        /// against the work, small enough to stay a rounding error in RSS.
        const CHUNK: usize = 4_096;

        let mut buffer: Vec<Command> = Vec::with_capacity(CHUNK);
        let mut next = read_from;
        let mut count = 0_u64;

        loop {
            buffer.clear();
            {
                let mut replay = self.journal.replay().from_sequence(next)?;
                while buffer.len() < CHUNK {
                    let Some(command) = replay.next_record()? else {
                        break;
                    };
                    buffer.push(command);
                }
            }
            if buffer.is_empty() {
                break;
            }
            next += buffer.len() as Sequence;
            for command in buffer.drain(..) {
                // Folded before applying, so the chain covers the record as
                // journalled rather than whatever the command becomes -- and
                // folded even for records the snapshot already accounts for,
                // because the chain needs the whole interval and the books do not.
                self.journal
                    .fold_replayed(command.as_bytes(), command.sequence);
                if command.sequence < apply_from {
                    continue;
                }
                // Counted where it is applied, not where it is read: the records
                // re-read to rebuild the chain were already in the snapshot, and
                // reporting them as replayed would overstate the work.
                count += 1;
                self.events.clear();
                let kind = command.kind();
                self.apply(command);
                if kind == Some(CommandKind::Watermark) {
                    // Everything before this marker was handed to the feed by
                    // the venue that journalled it; nothing there needs saying
                    // again.
                    self.pending_outcomes.clear();
                } else {
                    // Private outcomes past the last marker are kept for the
                    // account they belong to. Sequence numbers are zeroed:
                    // these are redelivered out of band, and a stale number
                    // would read as a gap in a channel that has restarted.
                    for event in &self.events {
                        // Outcomes only, never the ack. The ack was delivered
                        // before it was journalled -- durable is what it means
                        // -- so redelivering it would tell every reconnecting
                        // client its orders arrived twice. What dies with a
                        // leader is the outcome, and that is what is kept.
                        if event.kind == EventKind::Received as u8 {
                            continue;
                        }
                        if let Some(hub::Channel::Account(account)) = hub::Channel::of(event) {
                            let mut kept = *event;
                            kept.sequence = 0;
                            self.pending_outcomes.entry(account).or_default().push(kept);
                        }
                    }
                }
            }
        }
        self.events.clear();
        self.released = true;
        self.event_sequence = 0;
        Ok(count)
    }

    /// Captures the state as of the current journal position.
    ///
    /// Resting orders come out in price-then-time priority, so restoring them
    /// in this order reproduces queue position and not merely depth.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        let mut orders = Vec::new();
        for (symbol, book) in self.books.iter() {
            book.for_each_resting(|resting| {
                let held = self.reservations.get(&(resting.account, resting.order));
                orders.push(SnapshotOrder {
                    reserved: held.map_or(0, |r| r.remaining),
                    order_id: resting.order,
                    account: held.map_or(0, |r| r.account),
                    quantity: resting.quantity,
                    price: resting.price,
                    symbol,
                    side: resting.side as u8,
                    _pad: [0; 11],
                });
            });
        }
        let balances = self
            .accounts
            .sorted()
            .into_iter()
            .map(|((account, asset), balance)| SnapshotBalance {
                free: balance.free,
                reserved: balance.reserved,
                account,
                asset,
                _pad: [0; 4],
            })
            .collect();
        // Sorted, for the same reason the balances are: the same state has to
        // write the same bytes, or a golden-hash check compares hash-map
        // iteration order rather than venue state.
        let mut order_id_marks: Vec<SnapshotOrderIdMark> = self
            .highest_order_id
            .iter()
            .map(|(&account, &highest)| SnapshotOrderIdMark { account, highest })
            .collect();
        order_id_marks.sort_unstable_by_key(|mark| mark.account);
        // Only the symbols that are not in the default state, so a venue with
        // nothing halted writes nothing here. Sorted, like everything else in a
        // snapshot, so the same state writes the same bytes.
        let mut symbol_states: Vec<SnapshotSymbolState> = self
            .symbol_state
            .iter()
            .enumerate()
            .filter(|(_, state)| **state != TradingState::default())
            .map(|(symbol, state)| SnapshotSymbolState {
                symbol: symbol as SymbolId,
                state: *state as u8,
                _pad: [0; 3],
            })
            .collect();
        symbol_states.sort_unstable_by_key(|record| record.symbol);
        let mut stopped_accounts: Vec<SnapshotStoppedAccount> = self
            .stopped_accounts
            .keys()
            .map(|&account| SnapshotStoppedAccount { account })
            .collect();
        stopped_accounts.sort_unstable_by_key(|record| record.account);
        Snapshot {
            sequence: self.journal.next_sequence(),
            orders,
            balances,
            order_id_marks,
            symbol_states,
            stopped_accounts,
            chain_head: self.journal.chain_head(),
            chain_sealed_at: self.journal.chain_sealed_at(),
        }
    }

    /// Rebuilds books, balances and holds from a snapshot.
    ///
    /// Anything in the snapshot that will not load is counted as an accounting
    /// violation: a snapshot that silently drops orders would lose client money
    /// and look like a successful recovery.
    pub fn restore(&mut self, snapshot: &Snapshot) {
        for mark in &snapshot.order_id_marks {
            self.highest_order_id.insert(mark.account, mark.highest);
        }
        for record in &snapshot.symbol_states {
            let Some(state) = TradingState::from_wire(u64::from(record.state)) else {
                Self::violation();
                continue;
            };
            let index = record.symbol as usize;
            if index >= self.symbol_state.len() {
                self.symbol_state.resize(index + 1, TradingState::default());
            }
            self.symbol_state[index] = state;
        }
        for record in &snapshot.stopped_accounts {
            self.stopped_accounts.insert(record.account, ());
        }
        // Replay carries on from this head, so snapshot-plus-replay reaches the
        // same one a full replay would. Without it the head would commit to a
        // suffix of the stream and every client checking it would disagree.
        self.journal
            .restore_chain(snapshot.chain_head, snapshot.chain_sealed_at);
        for record in &snapshot.balances {
            self.accounts
                .restore(record.account, record.asset, balance_of(record));
        }
        for record in &snapshot.orders {
            let Some(side) = Side::from_wire(record.side) else {
                Self::violation();
                continue;
            };
            let restored = self.books.get_mut(record.symbol).is_some_and(|book| {
                book.restore(
                    (record.account, record.order_id),
                    side,
                    record.price,
                    record.quantity,
                )
            });
            if !restored {
                Self::violation();
                continue;
            }
            self.hold(
                (record.account, record.order_id),
                Reservation {
                    account: record.account,
                    symbol: record.symbol,
                    side,
                    limit_price: record.price,
                    remaining: record.reserved,
                    // Filled in by `hold`, which is what knows where it landed.
                    at: 0,
                },
            );
        }
    }

    fn apply(&mut self, command: Command) {
        let Some(kind) = command.kind() else {
            self.reject(&command, RejectReason::UnsupportedTimeInForce);
            return;
        };
        match kind {
            CommandKind::NewOrder => self.apply_new_order(&command),
            CommandKind::Cancel => {
                self.apply_cancel(&command);
            }
            CommandKind::AmendDown => self.apply_amend(&command),
            // Session control never reaches here: the gateway handles it and
            // does not journal it. One in the journal means a bug upstream, so
            // it is refused rather than quietly ignored.
            CommandKind::Subscribe
            | CommandKind::Unsubscribe
            | CommandKind::QueryOpenOrders
            | CommandKind::CancelOnDisconnect
            // Revoking a key is gateway business: keys are not deterministic
            // venue state, and a replay that reapplied a revocation would need
            // the key material in the log.
            | CommandKind::RevokeKey
            | CommandKind::Resume => {
                self.reject(&command, RejectReason::UnsupportedTimeInForce);
            }
            // Authentication happens in the gateway, before sequencing, and a
            // proof in the journal would mean a secret had been written to disk
            // and replayed on every recovery. Refused here as the last barrier.
            CommandKind::Authenticate | CommandKind::AuthenticateContinued => {
                self.reject(&command, RejectReason::NotAuthenticated);
            }
            CommandKind::Deposit => self.accounts.deposit(
                command.account,
                command.deposit_asset(),
                u128::from(command.quantity),
            ),
            // Unlike a deposit, this can fail: an operator can ask for more than
            // the partition holds free. Rejected rather than clamped, because a
            // partial transfer that reports success loses the difference.
            CommandKind::Withdraw => {
                if self
                    .accounts
                    .withdraw(
                        command.account,
                        command.deposit_asset(),
                        u128::from(command.quantity),
                    )
                    .is_err()
                {
                    self.reject(&command, RejectReason::InsufficientBalance);
                }
            }
            // The venue's own marker: no state, no events. Its meaning lives
            // entirely in where it sits in the sequence, which replay reads.
            CommandKind::Watermark => {}
            CommandKind::SetSymbolState => self.apply_symbol_state(&command),
            CommandKind::SetAccountTrading => self.apply_account_trading(&command),
            CommandKind::CancelAll => self.apply_cancel_all(&command),
            CommandKind::CancelReplace => {
                // The replacement is only submitted if the original was
                // actually cancelled. Replacing an order that does not exist
                // must reject the whole request, not quietly create a new
                // order the client never asked for.
                if self.apply_cancel(&command) {
                    let mut replacement = command;
                    replacement.order_id = command.replacement_id;
                    self.apply_new_order(&replacement);
                }
            }
        }
    }

    /// The journal's verifiable chain head, sealed at the last interval boundary.
    ///
    /// Zero when the venue runs without chaining. What it covers is
    /// [`Self::chain_sealed_at`].
    #[must_use]
    pub const fn chain_head(&self) -> [u8; 32] {
        self.journal.chain_head()
    }

    /// The first sequence [`Self::chain_head`] does not cover.
    #[must_use]
    pub const fn chain_sealed_at(&self) -> Sequence {
        self.journal.chain_sealed_at()
    }

    /// Private outcomes a replay regenerated for this account, past the last
    /// watermark -- what a reconnecting client is told instead of having to ask.
    /// Draining is deliberate: the first session to act for the account takes
    /// them, and a second redelivery would be a duplicate report.
    pub fn take_pending_outcomes(&mut self, account: AccountId) -> Option<Vec<Event>> {
        self.pending_outcomes.remove(&account)
    }

    /// Gives the venue the key it signs checkpoints with.
    ///
    /// Separate from turning the chain on, because they are separate decisions:
    /// a venue can publish a chain nobody can forge only if it holds a key, and
    /// one that holds no key still publishes a chain worth folding.
    pub fn set_chain_key(&mut self, key: SigningKey) {
        self.chain_key = Some(key);
    }

    /// The public half of the signing key, for a client to check against the one
    /// it was given out of band.
    #[must_use]
    pub fn chain_public_key(&self) -> Option<[u8; 32]> {
        self.chain_key
            .as_ref()
            .map(|key| key.verifying_key().to_bytes())
    }

    /// Turns the verifiable chain on or off. Off by default; see the journal for
    /// what it costs.
    pub fn set_chaining(&mut self, on: bool) {
        self.journal.set_chaining(on);
    }

    /// Records between chain heads.
    #[must_use]
    pub const fn chain_interval(&self) -> u64 {
        self.journal.chain_interval()
    }

    /// Sets how many records fall between chain heads.
    ///
    /// # Panics
    /// If `interval` is zero.
    pub fn set_chain_interval(&mut self, interval: u64) {
        self.journal.set_chain_interval(interval);
    }

    /// Whether a symbol currently accepts new orders.
    #[must_use]
    pub fn symbol_state(&self, symbol: SymbolId) -> TradingState {
        self.symbol_state
            .get(symbol as usize)
            .copied()
            .unwrap_or_default()
    }

    /// Whether an account may open new risk.
    #[must_use]
    pub fn account_may_trade(&self, account: AccountId) -> bool {
        !self.stopped_accounts.contains_key(&account)
    }

    fn apply_symbol_state(&mut self, command: &Command) {
        if self.instruments.get(command.symbol).is_none() {
            self.reject(command, RejectReason::UnknownSymbol);
            return;
        }
        let Some(state) = TradingState::from_wire(command.quantity) else {
            self.reject(command, RejectReason::SymbolNotTrading);
            return;
        };
        let index = command.symbol as usize;
        if index >= self.symbol_state.len() {
            self.symbol_state.resize(index + 1, TradingState::default());
        }
        self.symbol_state[index] = state;
        self.push(command, EventKind::SymbolState, 0, command.quantity, 0, 0);
    }

    fn apply_account_trading(&mut self, command: &Command) {
        if command.quantity == 0 {
            self.stopped_accounts.insert(command.account, ());
        } else {
            self.stopped_accounts.remove(&command.account);
        }
        self.push(
            command,
            EventKind::AccountTrading,
            0,
            command.quantity,
            0,
            0,
        );
    }

    /// Cancels everything an account has resting, on one symbol or all of them.
    ///
    /// Expanded into ordinary cancels so each one journals, publishes and
    /// replays like any other -- the same reason cancel-on-disconnect is built
    /// this way. A symbol of zero means every listed instrument, because zero is
    /// not a valid symbol.
    fn apply_cancel_all(&mut self, command: &Command) {
        let symbols: Vec<SymbolId> = if command.symbol == 0 {
            self.instruments.iter().map(|i| i.symbol).collect()
        } else {
            vec![command.symbol]
        };
        for symbol in symbols {
            // Collected first: cancelling walks the same index this reads.
            let orders: Vec<OrderId> = self
                .open_orders_for(command.account, symbol)
                .into_iter()
                .map(|resting| resting.order)
                .collect();
            for order in orders {
                let mut cancel = *command;
                cancel.kind = CommandKind::Cancel as u8;
                cancel.symbol = symbol;
                cancel.order_id = order;
                self.apply_cancel(&cancel);
            }
        }
    }

    fn apply_new_order(&mut self, command: &Command) {
        let (Some(side), Some(tif)) = (command.side(), command.time_in_force()) else {
            self.reject(command, RejectReason::UnsupportedTimeInForce);
            return;
        };
        // A market order is expressed to the book as a limit at the band
        // extreme, so a resting time-in-force would let its remainder rest
        // *at that extreme* — never what the sender meant, and with a wide
        // band it is also how one cheap order would drag the book's window
        // to its worst case. The engine's own market entry point refuses
        // exactly this; the venue does not use that entry point, so the rule
        // is enforced here, before anything is reserved.
        if command.is_market()
            && !matches!(
                tif,
                TimeInForce::ImmediateOrCancel | TimeInForce::FillOrKill
            )
        {
            self.reject(command, RejectReason::UnsupportedTimeInForce);
            return;
        }
        let Some(instrument) = self.instruments.get(command.symbol).copied() else {
            self.reject(command, RejectReason::UnknownSymbol);
            return;
        };
        // New risk only. A halted or cancel-only symbol still allows cancels and
        // amends down, which are handled elsewhere -- a venue that stops an
        // account from reducing its exposure is more dangerous than one that
        // lets it trade.
        if self.symbol_state(command.symbol) != TradingState::Trading {
            self.reject(command, RejectReason::SymbolNotTrading);
            return;
        }
        if !self.account_may_trade(command.account) {
            self.reject(command, RejectReason::AccountNotTrading);
            return;
        }
        if command.quantity == 0 {
            self.reject(command, RejectReason::QuantityZero);
            return;
        }
        if command.quantity > instrument.max_quantity {
            self.reject(command, RejectReason::QuantityTooLarge);
            return;
        }
        if self
            .reservations
            .contains_key(&(command.account, command.order_id))
        {
            self.reject(command, RejectReason::DuplicateOrderId);
            return;
        }
        // Order IDs increase per account, so an ID already used is refused even
        // once the order it named is gone. Without this, a client retrying an
        // order it never got an answer for traded twice whenever the first
        // attempt had already filled -- the duplicate check above finds only
        // what is still resting.
        if let Some(&highest) = self.highest_order_id.get(&command.account)
            && command.order_id <= highest
        {
            self.reject(command, RejectReason::OrderIdNotIncreasing);
            return;
        }

        // Self-match prevention, cancel-newest: an order that would trade
        // against its own account is refused, and the resting side is left
        // alone. Protecting resting liquidity is the point -- the alternative,
        // cancelling the resting order, lets anyone destroy their own queue
        // position by accident and is worse for the book. This matches CME's
        // self-match protection and Binance's EXPIRE_TAKER.
        //
        // Checked before matching rather than during it. The engine knows
        // nothing about accounts, and the pipeline already knows every resting
        // order's owner, so the ownership question is answerable here without
        // widening the engine's order record.
        if self.would_self_match(command, side) {
            self.reject(command, RejectReason::SelfMatchPrevented);
            return;
        }

        let market = command.is_market();

        let (asset, amount) = match side {
            Side::Bid => {
                // The engine matches on quantity and knows nothing about money,
                // so a buy has to hold the quote asset up front. A market buy
                // has no limit to price that against and holds at the worst
                // price the band can represent, getting the unspent part back
                // when the order ends. Reserving merely "whatever is free" let
                // a market buy sweep more than the account could pay for, and
                // the shortfall surfaced as a failed settle after the trade had
                // already happened.
                let price = if market {
                    instrument.ceiling_ticks
                } else {
                    command.price
                };
                let Some(amount) = instrument.notional(price, command.quantity) else {
                    self.reject(command, RejectReason::QuantityTooLarge);
                    return;
                };
                (instrument.quote, amount)
            }
            Side::Ask => (instrument.base, u128::from(command.quantity)),
        };

        if amount == 0
            || self
                .accounts
                .reserve(command.account, asset, amount)
                .is_err()
        {
            self.reject(command, RejectReason::InsufficientBalance);
            return;
        }
        // Accepted, so the ID is spent whatever the matching outcome turns out
        // to be: it may rest, fill outright, or be cancelled back by its
        // time-in-force, and in all three cases the order happened and must not
        // be sendable again. Only a *rejected* order leaves its ID free, which
        // is what a client expects -- nothing happened, so the same ID may be
        // corrected and resent.
        self.highest_order_id
            .insert(command.account, command.order_id);
        self.hold(
            (command.account, command.order_id),
            Reservation {
                account: command.account,
                symbol: command.symbol,
                side,
                limit_price: command.price,
                remaining: amount,
                at: 0,
            },
        );

        let mut outcome = std::mem::take(&mut self.scratch);
        let Some(book) = self.books.get_mut(command.symbol) else {
            self.scratch = outcome;
            self.release_all((command.account, command.order_id));
            self.reject(command, RejectReason::UnknownSymbol);
            return;
        };
        book.submit_into(
            &mut outcome,
            (command.account, command.order_id),
            side,
            command.price,
            command.quantity,
            tif,
            market,
        );

        if let Some(reason) = outcome.reject {
            self.scratch = outcome;
            self.release_all((command.account, command.order_id));
            self.reject(command, reason);
            return;
        }

        self.settle(command, &instrument, side, &outcome);

        // Anything that neither traded nor rested is gone; release its hold.
        if outcome.resting_quantity == 0 {
            self.release_all((command.account, command.order_id));
        }
        self.emit_outcome(command, side, &outcome);
        self.scratch = outcome;
    }

    /// One account's resting orders on one symbol, as they currently stand.
    ///
    /// Costs that account's own order count, not the venue's. A client can
    /// rebuild a book from a snapshot but not its own orders, so this is what a
    /// trader needs after reconnecting before it can act.
    #[must_use]
    pub fn open_orders_for(&self, account: AccountId, symbol: SymbolId) -> Vec<book::Resting> {
        let Some(book) = self.books.get(symbol) else {
            return Vec::new();
        };
        self.resting_per_account
            .get(&(account, symbol))
            .map(|held| {
                held.iter()
                    .filter_map(|order| book.resting_order((account, *order)))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether this order would actually trade against its own account.
    ///
    /// Walks the resting orders in the order the engine would consume them and
    /// stops once this order's quantity is exhausted. Asking merely "does this
    /// account own anything I could reach" is not the same question and gets
    /// the answer wrong: a market order reaches every level, so an account with
    /// any resting order at all would never be able to take liquidity again,
    /// even when it would be filled several levels before meeting itself.
    ///
    /// Stops at the first order it owns, so an order that crosses nothing pays
    /// for nothing and an aggressive one pays only for what it would sweep.
    fn would_self_match(&self, command: &Command, side: Side) -> bool {
        // One lookup, and almost every order stops here: an account with nothing
        // resting on this symbol cannot possibly trade against itself, so the
        // common case never touches the book at all.
        if !self
            .resting_per_account
            .contains_key(&(command.account, command.symbol))
        {
            return false;
        }
        let Some(book) = self.books.get(command.symbol) else {
            return false;
        };
        let mut remaining = command.quantity;
        let mut ours = false;
        book.for_each_crossable(side, command.price, command.is_market(), |resting| {
            // The book now says who owns a resting order, so preventing a
            // self-match is a comparison rather than a hash lookup per
            // crossable order -- the account had to be carried anyway once IDs
            // stopped being unique, and this path gets it for nothing.
            if resting.account == command.account {
                ours = true;
                return false;
            }
            remaining = remaining.saturating_sub(resting.quantity);
            // Everything past this point is beyond what the order can fill.
            remaining > 0
        });
        ours
    }

    /// Returns whether the order was actually cancelled.
    fn apply_cancel(&mut self, command: &Command) -> bool {
        let mut outcome = std::mem::take(&mut self.scratch);
        let Some(book) = self.books.get_mut(command.symbol) else {
            self.scratch = outcome;
            self.reject(command, RejectReason::UnknownSymbol);
            return false;
        };
        book.cancel_into(&mut outcome, (command.account, command.order_id));
        if let Some(reason) = outcome.reject {
            self.scratch = outcome;
            self.reject(command, reason);
            return false;
        }
        self.release_all((command.account, command.order_id));
        self.push(command, EventKind::Canceled, command.order_id, 0, 0, 0);
        self.emit_levels(command, &outcome);
        self.scratch = outcome;
        true
    }

    fn apply_amend(&mut self, command: &Command) {
        let mut outcome = std::mem::take(&mut self.scratch);
        let Some(book) = self.books.get_mut(command.symbol) else {
            self.scratch = outcome;
            self.reject(command, RejectReason::UnknownSymbol);
            return;
        };
        book.amend_down_into(
            &mut outcome,
            (command.account, command.order_id),
            command.quantity,
        );
        if let Some(reason) = outcome.reject {
            self.scratch = outcome;
            self.reject(command, reason);
            return;
        }
        // Give back the part of the hold a smaller order no longer needs.
        if let Some(reservation) = self
            .reservations
            .get(&(command.account, command.order_id))
            .copied()
        {
            let still_needed = match reservation.side {
                Side::Bid => self
                    .instruments
                    .get(command.symbol)
                    .and_then(|i| i.notional(reservation.limit_price, command.quantity))
                    .unwrap_or(0),
                Side::Ask => u128::from(command.quantity),
            };
            let excess = reservation.remaining.saturating_sub(still_needed);
            if excess > 0 {
                self.release((command.account, command.order_id), excess);
            }
        }
        if command.quantity == 0 {
            self.release_all((command.account, command.order_id));
        }
        self.push(
            command,
            EventKind::Resting,
            command.order_id,
            command.quantity,
            0,
            0,
        );
        self.emit_levels(command, &outcome);
        self.scratch = outcome;
    }

    /// Moves assets for every execution, on both sides of each trade.
    fn settle(
        &mut self,
        command: &Command,
        instrument: &Instrument,
        side: Side,
        outcome: &Outcome,
    ) {
        for execution in &outcome.executions {
            let quantity = u128::from(execution.quantity);
            // Both of these are unreachable: entry validation bounds the
            // notional, and a resting order always has a hold. Skipping either
            // would drop the settlement of a trade the engine has already
            // matched and published, so they are counted like any other broken
            // invariant rather than passed over.
            let Some(notional) = instrument.notional(execution.price, execution.quantity) else {
                Self::violation();
                continue;
            };
            let Some(maker) = self
                .reservations
                .get(&(execution.resting_account, execution.resting_order))
                .copied()
            else {
                Self::violation();
                continue;
            };

            let (buyer, seller) = match side {
                Side::Bid => (command.account, maker.account),
                Side::Ask => (maker.account, command.account),
            };

            // Buyer pays quote out of its hold and receives base; seller gives
            // base out of its hold and receives quote.
            //
            // None of these can fail if reservation was sized correctly, so a
            // failure means an accounting invariant is broken. Swallowing it
            // would move one side of a trade and not the other, which creates
            // or destroys value silently. It is counted and asserted on in
            // tests instead.
            Self::record(self.accounts.settle_out(buyer, instrument.quote, notional));
            Self::record(self.accounts.settle_in(buyer, instrument.base, quantity));
            Self::record(self.accounts.settle_out(seller, instrument.base, quantity));
            Self::record(self.accounts.settle_in(seller, instrument.quote, notional));

            let (taker_used, maker_used) = match side {
                Side::Bid => (notional, quantity),
                Side::Ask => (quantity, notional),
            };
            self.consume((command.account, command.order_id), taker_used);
            self.consume(
                (execution.resting_account, execution.resting_order),
                maker_used,
            );

            // A buy that traded below its limit over-reserved; give it back. A
            // market buy has no limit to compare against: it was reserved at
            // the band ceiling and gets the whole unspent remainder back when
            // the order ends.
            if side == Side::Bid
                && !command.is_market()
                && let Some(at_limit) = instrument.notional(command.price, execution.quantity)
                && at_limit > notional
            {
                self.release((command.account, command.order_id), at_limit - notional);
            }

            // A maker the engine fully consumed releases whatever is left.
            let maker_gone = !self
                .books
                .get(command.symbol)
                .is_some_and(|b| b.contains((execution.resting_account, execution.resting_order)));
            if maker_gone {
                self.release_all((execution.resting_account, execution.resting_order));
            }
        }
    }

    /// Notes an accounting operation that should have been impossible to fail.
    fn record<T>(result: Result<T, accounts::BalanceError>) {
        if result.is_err() {
            Self::violation();
        }
    }

    fn violation() {
        VIOLATIONS.fetch_add(1, Ordering::Relaxed);
    }

    /// Reduces a hold because the asset left the account.
    fn consume(&mut self, order: book::OrderKey, amount: u128) {
        if let Some(reservation) = self.reservations.get_mut(&order) {
            reservation.remaining = reservation.remaining.saturating_sub(amount);
        }
    }

    /// Returns part of a hold to the free balance.
    fn release(&mut self, order: book::OrderKey, amount: u128) {
        let Some(reservation) = self.reservations.get_mut(&order) else {
            return;
        };
        let amount = amount.min(reservation.remaining);
        if amount == 0 {
            return;
        }
        reservation.remaining -= amount;
        let (account, symbol, side) = (reservation.account, reservation.symbol, reservation.side);
        if let Some(instrument) = self.instruments.get(symbol) {
            let asset = match side {
                Side::Bid => instrument.quote,
                Side::Ask => instrument.base,
            };
            Self::record(self.accounts.release(account, asset, amount));
        }
    }

    /// Records a hold, and that the account now has one more order resting.
    fn hold(&mut self, order: book::OrderKey, mut reservation: Reservation) {
        match self.reservations.get(&order) {
            // Already indexed. It keeps its place, or the swap that removes it
            // later would take out somebody else's order.
            Some(existing) => reservation.at = existing.at,
            None => {
                let held = self
                    .resting_per_account
                    .entry((reservation.account, reservation.symbol))
                    .or_default();
                debug_assert!(held.len() < u32::MAX as usize, "resting list overflowed");
                reservation.at = held.len() as u32;
                held.push(order.1);
            }
        }
        self.reservations.insert(order, reservation);
    }

    /// Takes an order out of its account's resting list in constant time.
    ///
    /// The position is carried on the reservation, which is already being looked
    /// up here. Searching for it instead — which is what this did — made every
    /// fill and every cancel cost a walk of that account's open orders: an
    /// account resting fifty thousand quotes paid a fifty-thousand-element scan
    /// per fill, and the benchmark measured 3 microseconds against the 159 ns the
    /// same path costs with one order resting. Swapping without shifting was
    /// already here; it was the search in front of it that dominated. This is
    /// worst for exactly the client the venue exists to serve, since a market
    /// maker is defined by having a great many orders resting at once.
    fn release_all(&mut self, order: book::OrderKey) {
        // Taken out first, and the hold given back from what came out. This used
        // to read the reservation, call `release` -- which read it again -- and
        // then remove it: three lookups of a key it was about to discard, on the
        // path a market maker uses most.
        let Some(reservation) = self.reservations.remove(&order) else {
            return;
        };
        if reservation.remaining > 0
            && let Some(instrument) = self.instruments.get(reservation.symbol)
        {
            let asset = match reservation.side {
                Side::Bid => instrument.quote,
                Side::Ask => instrument.base,
            };
            Self::record(
                self.accounts
                    .release(reservation.account, asset, reservation.remaining),
            );
        }
        let key = (reservation.account, reservation.symbol);
        let Some(held) = self.resting_per_account.get_mut(&key) else {
            return;
        };

        let at = reservation.at as usize;
        if held.get(at).copied() == Some(order.1) {
            held.swap_remove(at);
            // Whatever was moved into the gap is no longer where its own
            // reservation says it is.
            // The list is one account's, so the account half of the key is the
            // same for everything in it.
            if let Some(moved) = held.get(at).copied()
                && let Some(entry) = self.reservations.get_mut(&(order.0, moved))
            {
                entry.at = at as u32;
            }
        } else {
            // The index disagrees with the list. That is a bug rather than a
            // condition, and it is counted -- but the order still has to come
            // out, because leaving it would let a cancelled order keep blocking
            // a self-match and be reported as open. Correct first, fast second.
            Self::violation();
            if let Some(found) = held.iter().position(|held| *held == order.1) {
                held.swap_remove(found);
                if let Some(moved) = held.get(found).copied()
                    && let Some(entry) = self.reservations.get_mut(&(order.0, moved))
                {
                    entry.at = found as u32;
                }
            }
        }

        // Dropped rather than left empty, so the map stays the size of what is
        // actually resting.
        if held.is_empty() {
            self.resting_per_account.remove(&key);
        }
    }

    fn emit_outcome(&mut self, command: &Command, side: Side, outcome: &Outcome) {
        // The acknowledgement carries the timing, because it is the one event
        // every command produces and these two fields are otherwise zero on it.
        self.push(
            command,
            EventKind::Received,
            command.order_id,
            command.ingress_ns,
            self.matched_ns as i64,
            0,
        );
        for execution in &outcome.executions {
            self.push_fill(command, side, execution);
        }
        if outcome.resting_quantity > 0 {
            self.push(
                command,
                EventKind::Resting,
                command.order_id,
                outcome.resting_quantity,
                command.price,
                0,
            );
        }
        self.emit_levels(command, outcome);
    }

    fn emit_levels(&mut self, command: &Command, outcome: &Outcome) {
        for change in &outcome.level_changes {
            self.push_level(command, change.side, change.price, change.quantity);
        }
        // Almost always empty. An order resting behind the touch moves the depth
        // feed and nothing here, which is the entire point of the channel.
        for change in &outcome.top_changes {
            self.push_top(command, change.side, change.price, change.quantity);
        }
    }

    /// Tells both sides of a trade, then the tape.
    ///
    /// The maker used to be told nothing at all. Its resting order was consumed,
    /// the taker got a fill, the public tape got a print carrying no identities,
    /// and the participant whose position had just changed could only find out
    /// by asking. For a market maker -- the client this venue exists to serve,
    /// defined by having many orders resting -- that is the difference between a
    /// feed and a poll.
    ///
    /// It went unnoticed because it needed the maker's account, and until orders
    /// were keyed per account there was nowhere here to get one: the engine
    /// reports a fill by slot, and the pipeline resolved that to a bare ID. The
    /// slot table carries the owner now, so the maker's event costs a field
    /// read.
    fn push_fill(&mut self, command: &Command, side: Side, execution: &Execution) {
        let sequence = self.take_sequence();
        self.events.push(Event {
            sequence,
            cause_sequence: command.sequence,
            account: command.account,
            order_id: command.order_id,
            counterparty_order_id: execution.resting_order,
            quantity: execution.quantity,
            price: execution.price,
            symbol: command.symbol,
            kind: EventKind::Filled as u8,
            side: side as u8,
            reject_reason: RejectReason::None as u8,
            _pad: [0; 1],
        });
        // The maker's own copy: its order, the taker's as the counterparty, and
        // its own side rather than the aggressor's.
        let sequence = self.take_sequence();
        self.events.push(Event {
            sequence,
            cause_sequence: command.sequence,
            account: execution.resting_account,
            order_id: execution.resting_order,
            counterparty_order_id: command.order_id,
            quantity: execution.quantity,
            price: execution.price,
            symbol: command.symbol,
            kind: EventKind::Filled as u8,
            side: side.opposite() as u8,
            reject_reason: RejectReason::None as u8,
            _pad: [0; 1],
        });
        // The public print carries no account and no order IDs.
        let sequence = self.take_sequence();
        self.events.push(Event {
            sequence,
            cause_sequence: command.sequence,
            account: 0,
            order_id: 0,
            counterparty_order_id: 0,
            quantity: execution.quantity,
            price: execution.price,
            symbol: command.symbol,
            kind: EventKind::Trade as u8,
            side: side as u8,
            reject_reason: RejectReason::None as u8,
            _pad: [0; 1],
        });
    }

    fn push_top(&mut self, command: &Command, side: Side, price: Ticks, quantity: u64) {
        let sequence = self.take_sequence();
        self.events.push(Event {
            sequence,
            cause_sequence: command.sequence,
            account: 0,
            order_id: 0,
            counterparty_order_id: 0,
            quantity,
            price,
            symbol: command.symbol,
            kind: EventKind::Bbo as u8,
            side: side as u8,
            reject_reason: RejectReason::None as u8,
            _pad: [0; 1],
        });
    }

    fn push_level(&mut self, command: &Command, side: Side, price: Ticks, quantity: u64) {
        let sequence = self.take_sequence();
        self.events.push(Event {
            sequence,
            cause_sequence: command.sequence,
            account: 0,
            order_id: 0,
            counterparty_order_id: 0,
            quantity,
            price,
            symbol: command.symbol,
            kind: EventKind::BookDelta as u8,
            side: side as u8,
            reject_reason: RejectReason::None as u8,
            _pad: [0; 1],
        });
    }

    fn push(
        &mut self,
        command: &Command,
        kind: EventKind,
        order: OrderId,
        quantity: Quantity,
        price: Ticks,
        reject: u8,
    ) {
        let sequence = self.take_sequence();
        self.events.push(Event {
            sequence,
            cause_sequence: command.sequence,
            account: command.account,
            order_id: order,
            counterparty_order_id: 0,
            quantity,
            price,
            symbol: command.symbol,
            kind: kind as u8,
            side: command.side,
            reject_reason: reject,
            _pad: [0; 1],
        });
    }

    fn reject(&mut self, command: &Command, reason: RejectReason) {
        // Counted here rather than by scanning the event stream: this is the
        // only path a rejection takes, so one increment covers it and the
        // accepting path pays nothing at all. "Why is this client's fill rate
        // down" is answered by which of these is climbing.
        self.rejects[reason as usize] += 1;
        self.push(
            command,
            EventKind::Rejected,
            command.order_id,
            0,
            0,
            reason as u8,
        );
    }

    /// Tells the pipeline when the group about to be applied began matching.
    ///
    /// Called by whatever owns the clock — the gateway — once per group, never
    /// per command, and never from inside the deterministic path. Leaving it
    /// unset means the venue publishes no match time, which is what a
    /// measurement run and a replay both do.
    pub const fn matching_now(&mut self, ns: u64) {
        self.matched_ns = ns;
    }

    /// How many orders each reason has refused, indexed by
    /// [`RejectReason`] as `usize`.
    #[must_use]
    pub const fn rejects(&self) -> &[u64; REJECT_REASONS] {
        &self.rejects
    }

    /// The reasons that have actually refused something, worst first.
    #[must_use]
    pub fn rejects_by_reason(&self) -> Vec<(RejectReason, u64)> {
        let mut seen: Vec<(RejectReason, u64)> = REASONS
            .iter()
            .filter(|reason| self.rejects[**reason as usize] > 0)
            .map(|reason| (*reason, self.rejects[*reason as usize]))
            .collect();
        seen.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
        seen
    }

    fn take_sequence(&mut self) -> Sequence {
        let sequence = self.event_sequence;
        self.event_sequence += 1;
        sequence
    }
}

/// A market order: takes the best available price, and never rests.
#[must_use]
pub fn market_order(
    account: AccountId,
    symbol: SymbolId,
    order_id: OrderId,
    side: Side,
    quantity: Quantity,
) -> Command {
    Command::new(
        CommandKind::NewOrder,
        account,
        symbol,
        order_id,
        side,
        0,
        quantity,
        TimeInForce::ImmediateOrCancel,
    )
    .taking()
}

/// Credits an account. `symbol` carries the asset, `quantity` the amount.
#[must_use]
pub fn deposit(account: AccountId, asset: instrument::AssetId, amount: Quantity) -> Command {
    Command::new(
        CommandKind::Deposit,
        account,
        asset,
        0,
        Side::Bid,
        0,
        amount,
        TimeInForce::GoodTillCancel,
    )
}

/// Debits an account, so an allotment can be moved to another partition.
///
/// Same layout as [`deposit`], and the operator sends the pair: this one first,
/// against the partition giving the funds up, then a deposit against the one
/// receiving them.
#[must_use]
pub fn withdraw(account: AccountId, asset: instrument::AssetId, amount: Quantity) -> Command {
    Command::new(
        CommandKind::Withdraw,
        account,
        asset,
        0,
        Side::Bid,
        0,
        amount,
        TimeInForce::GoodTillCancel,
    )
}

/// Cancels one resting order.
#[must_use]
pub fn cancel_order(account: AccountId, symbol: SymbolId, order_id: OrderId) -> Command {
    Command::new(
        CommandKind::Cancel,
        account,
        symbol,
        order_id,
        Side::Bid,
        0,
        0,
        TimeInForce::GoodTillCancel,
    )
}

/// Sets whether a symbol accepts new orders. Admin only.
#[must_use]
pub fn set_symbol_state(account: AccountId, symbol: SymbolId, state: TradingState) -> Command {
    Command::new(
        CommandKind::SetSymbolState,
        account,
        symbol,
        0,
        Side::Bid,
        0,
        state as u64,
        TimeInForce::GoodTillCancel,
    )
}

/// Stops or resumes an account's ability to open new risk. Admin only.
#[must_use]
pub fn set_account_trading(admin: AccountId, account: AccountId, may_trade: bool) -> Command {
    let mut command = Command::new(
        CommandKind::SetAccountTrading,
        account,
        0,
        0,
        Side::Bid,
        0,
        u64::from(may_trade),
        TimeInForce::GoodTillCancel,
    );
    // The subject travels in `account`, which is what the pipeline acts on, so
    // the sender is carried separately for the gateway's permission check.
    command.replacement_id = admin;
    command
}

/// Cancels everything an account has resting. `symbol` of zero means all.
#[must_use]
pub fn cancel_all(account: AccountId, symbol: SymbolId) -> Command {
    Command::new(
        CommandKind::CancelAll,
        account,
        symbol,
        0,
        Side::Bid,
        0,
        0,
        TimeInForce::GoodTillCancel,
    )
}

/// Turns cancel-on-disconnect on or off for the session that sends it.
#[must_use]
pub fn cancel_on_disconnect(account: AccountId, on: bool) -> Command {
    Command::new(
        CommandKind::CancelOnDisconnect,
        account,
        0,
        0,
        Side::Bid,
        0,
        u64::from(on),
        TimeInForce::GoodTillCancel,
    )
}

/// Asks the venue for this account's resting orders on one symbol.
#[must_use]
pub fn query_open_orders(account: AccountId, symbol: SymbolId) -> Command {
    Command::new(
        CommandKind::QueryOpenOrders,
        account,
        symbol,
        0,
        Side::Bid,
        0,
        0,
        TimeInForce::GoodTillCancel,
    )
}

/// One resting order, as a reply to [`query_open_orders`].
#[must_use]
pub fn order_state(account: AccountId, symbol: SymbolId, resting: &book::Resting) -> Event {
    Event {
        sequence: 0,
        cause_sequence: 0,
        account,
        order_id: resting.order,
        counterparty_order_id: 0,
        quantity: resting.quantity,
        price: resting.price,
        symbol,
        kind: EventKind::OrderState as u8,
        side: resting.side as u8,
        reject_reason: RejectReason::None as u8,
        _pad: [0; 1],
    }
}

/// Asks for a feed. `symbol` names the instrument, `quantity` the channel.
#[must_use]
pub fn subscribe(account: AccountId, symbol: SymbolId, channel: ChannelKind) -> Command {
    control(CommandKind::Subscribe, account, symbol, channel)
}

/// Stops a feed.
#[must_use]
pub fn unsubscribe(account: AccountId, symbol: SymbolId, channel: ChannelKind) -> Command {
    control(CommandKind::Unsubscribe, account, symbol, channel)
}

fn control(
    kind: CommandKind,
    account: AccountId,
    symbol: SymbolId,
    channel: ChannelKind,
) -> Command {
    Command::new(
        kind,
        account,
        symbol,
        0,
        Side::Bid,
        0,
        channel as u64,
        TimeInForce::GoodTillCancel,
    )
}

/// A limit order that rests until cancelled.
#[must_use]
pub fn limit_order(
    account: AccountId,
    symbol: SymbolId,
    order_id: OrderId,
    side: Side,
    price: Ticks,
    quantity: Quantity,
) -> Command {
    Command::new(
        CommandKind::NewOrder,
        account,
        symbol,
        order_id,
        side,
        price,
        quantity,
        TimeInForce::GoodTillCancel,
    )
}
