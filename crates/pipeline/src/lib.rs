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
    RejectReason, Sequence, Side, SnapshotBalance, SnapshotOrder, SymbolId, Ticks, TimeInForce,
};
use fastmap::FastMap;
use instrument::{Instrument, Instruments};
use snapshot::{Snapshot, balance_of};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

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
}

#[derive(Debug)]
pub struct Exchange<S: LogStorage> {
    journal: Journal<S>,
    accounts: Accounts,
    instruments: Instruments,
    /// Ordered, so any iteration is deterministic.
    books: BTreeMap<SymbolId, book::Book>,
    /// Keyed by client order ID. A hash map, not a tree: this is looked up
    /// several times per command and nothing iterates it, so ordering is not
    /// needed and O(log n) tree descents are pure cost.
    reservations: FastMap<OrderId, Reservation>,
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
    events: Vec<Event>,
    /// Set once [`Self::commit`] has handed the current batch's events out, so
    /// the next enqueue knows to start a fresh batch rather than append to one
    /// the caller has already seen.
    released: bool,
    event_sequence: Sequence,
    /// Reused across commands. Swapped into the book for the duration of a
    /// call so the book can fill it while `self` stays borrowable.
    scratch: Outcome,
}

impl<S: LogStorage> Exchange<S> {
    /// # Errors
    /// Fails if the journal cannot be read.
    pub fn new(storage: S, instruments: Instruments) -> bx_journal::Result<Self> {
        let mut books = BTreeMap::new();
        for instrument in instruments.iter() {
            books.insert(instrument.symbol, book::Book::new(*instrument));
        }
        Ok(Self {
            journal: Journal::open(storage)?,
            accounts: Accounts::new(),
            instruments,
            books,
            reservations: FastMap::default(),
            resting_per_account: FastMap::default(),
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
        self.books.get(&symbol)
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
        let commands = self.journal.replay().collect_all()?;
        Ok(self.apply_all(commands))
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
        let commands = self
            .journal
            .replay()
            .from_sequence(snapshot.sequence)?
            .collect_all()?;
        Ok(self.apply_all(commands))
    }

    fn apply_all(&mut self, commands: Vec<Command>) -> u64 {
        let count = commands.len() as u64;
        for command in commands {
            self.events.clear();
            self.apply(command);
        }
        self.events.clear();
        self.released = true;
        self.event_sequence = 0;
        count
    }

    /// Captures the state as of the current journal position.
    ///
    /// Resting orders come out in price-then-time priority, so restoring them
    /// in this order reproduces queue position and not merely depth.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        let mut orders = Vec::new();
        for (symbol, book) in &self.books {
            book.for_each_resting(|resting| {
                let held = self.reservations.get(&resting.order);
                orders.push(SnapshotOrder {
                    reserved: held.map_or(0, |r| r.remaining),
                    order_id: resting.order,
                    account: held.map_or(0, |r| r.account),
                    quantity: resting.quantity,
                    price: resting.price,
                    symbol: *symbol,
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
        Snapshot {
            sequence: self.journal.next_sequence(),
            orders,
            balances,
        }
    }

    /// Rebuilds books, balances and holds from a snapshot.
    ///
    /// Anything in the snapshot that will not load is counted as an accounting
    /// violation: a snapshot that silently drops orders would lose client money
    /// and look like a successful recovery.
    pub fn restore(&mut self, snapshot: &Snapshot) {
        for record in &snapshot.balances {
            self.accounts
                .restore(record.account, record.asset, balance_of(record));
        }
        for record in &snapshot.orders {
            let Some(side) = Side::from_wire(record.side) else {
                Self::violation();
                continue;
            };
            let restored = self.books.get_mut(&record.symbol).is_some_and(|book| {
                book.restore(record.order_id, side, record.price, record.quantity)
            });
            if !restored {
                Self::violation();
                continue;
            }
            self.hold(
                record.order_id,
                Reservation {
                    account: record.account,
                    symbol: record.symbol,
                    side,
                    limit_price: record.price,
                    remaining: record.reserved,
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
            CommandKind::Subscribe | CommandKind::Unsubscribe | CommandKind::QueryOpenOrders => {
                self.reject(&command, RejectReason::UnsupportedTimeInForce);
            }
            CommandKind::Deposit => self.accounts.deposit(
                command.account,
                command.deposit_asset(),
                u128::from(command.quantity),
            ),
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

    fn apply_new_order(&mut self, command: &Command) {
        let (Some(side), Some(tif)) = (command.side(), command.time_in_force()) else {
            self.reject(command, RejectReason::UnsupportedTimeInForce);
            return;
        };
        let Some(instrument) = self.instruments.get(command.symbol).copied() else {
            self.reject(command, RejectReason::UnknownSymbol);
            return;
        };
        if command.quantity == 0 {
            self.reject(command, RejectReason::QuantityZero);
            return;
        }
        if command.quantity > instrument.max_quantity {
            self.reject(command, RejectReason::QuantityTooLarge);
            return;
        }
        if self.reservations.contains_key(&command.order_id) {
            self.reject(command, RejectReason::DuplicateOrderId);
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
                    instrument.ceiling_ticks()
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
        self.hold(
            command.order_id,
            Reservation {
                account: command.account,
                symbol: command.symbol,
                side,
                limit_price: command.price,
                remaining: amount,
            },
        );

        let mut outcome = std::mem::take(&mut self.scratch);
        let Some(book) = self.books.get_mut(&command.symbol) else {
            self.scratch = outcome;
            self.release_all(command.order_id);
            self.reject(command, RejectReason::UnknownSymbol);
            return;
        };
        book.submit_into(
            &mut outcome,
            command.order_id,
            side,
            command.price,
            command.quantity,
            tif,
            market,
        );

        if let Some(reason) = outcome.reject {
            self.scratch = outcome;
            self.release_all(command.order_id);
            self.reject(command, reason);
            return;
        }

        self.settle(command, &instrument, side, &outcome);

        // Anything that neither traded nor rested is gone; release its hold.
        if outcome.resting_quantity == 0 {
            self.release_all(command.order_id);
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
        let Some(book) = self.books.get(&symbol) else {
            return Vec::new();
        };
        self.resting_per_account
            .get(&(account, symbol))
            .map(|held| {
                held.iter()
                    .filter_map(|order| book.resting_order(*order))
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
        let Some(book) = self.books.get(&command.symbol) else {
            return false;
        };
        let mut remaining = command.quantity;
        let mut ours = false;
        book.for_each_crossable(side, command.price, command.is_market(), |resting| {
            if self
                .reservations
                .get(&resting.order)
                .is_some_and(|held| held.account == command.account)
            {
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
        let Some(book) = self.books.get_mut(&command.symbol) else {
            self.scratch = outcome;
            self.reject(command, RejectReason::UnknownSymbol);
            return false;
        };
        book.cancel_into(&mut outcome, command.order_id);
        if let Some(reason) = outcome.reject {
            self.scratch = outcome;
            self.reject(command, reason);
            return false;
        }
        self.release_all(command.order_id);
        self.push(command, EventKind::Canceled, command.order_id, 0, 0, 0);
        self.emit_levels(command, &outcome);
        self.scratch = outcome;
        true
    }

    fn apply_amend(&mut self, command: &Command) {
        let mut outcome = std::mem::take(&mut self.scratch);
        let Some(book) = self.books.get_mut(&command.symbol) else {
            self.scratch = outcome;
            self.reject(command, RejectReason::UnknownSymbol);
            return;
        };
        book.amend_down_into(&mut outcome, command.order_id, command.quantity);
        if let Some(reason) = outcome.reject {
            self.scratch = outcome;
            self.reject(command, reason);
            return;
        }
        // Give back the part of the hold a smaller order no longer needs.
        if let Some(reservation) = self.reservations.get(&command.order_id).copied() {
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
                self.release(command.order_id, excess);
            }
        }
        if command.quantity == 0 {
            self.release_all(command.order_id);
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
            let Some(maker) = self.reservations.get(&execution.resting_order).copied() else {
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
            self.consume(command.order_id, taker_used);
            self.consume(execution.resting_order, maker_used);

            // A buy that traded below its limit over-reserved; give it back. A
            // market buy has no limit to compare against: it was reserved at
            // the band ceiling and gets the whole unspent remainder back when
            // the order ends.
            if side == Side::Bid
                && !command.is_market()
                && let Some(at_limit) = instrument.notional(command.price, execution.quantity)
                && at_limit > notional
            {
                self.release(command.order_id, at_limit - notional);
            }

            // A maker the engine fully consumed releases whatever is left.
            let maker_gone = !self
                .books
                .get(&command.symbol)
                .is_some_and(|b| b.contains(execution.resting_order));
            if maker_gone {
                self.release_all(execution.resting_order);
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
    fn consume(&mut self, order: OrderId, amount: u128) {
        if let Some(reservation) = self.reservations.get_mut(&order) {
            reservation.remaining = reservation.remaining.saturating_sub(amount);
        }
    }

    /// Returns part of a hold to the free balance.
    fn release(&mut self, order: OrderId, amount: u128) {
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
    fn hold(&mut self, order: OrderId, reservation: Reservation) {
        let key = (reservation.account, reservation.symbol);
        if self.reservations.insert(order, reservation).is_none() {
            self.resting_per_account.entry(key).or_default().push(order);
        }
    }

    fn release_all(&mut self, order: OrderId) {
        if let Some(reservation) = self.reservations.get(&order).copied() {
            self.release(order, reservation.remaining);
            self.reservations.remove(&order);
            let key = (reservation.account, reservation.symbol);
            if let Some(held) = self.resting_per_account.get_mut(&key) {
                // Swap-remove: order within an account's list carries no meaning,
                // and a market maker with thousands resting should not pay a
                // shift for each cancel.
                if let Some(at) = held.iter().position(|held| *held == order) {
                    held.swap_remove(at);
                }
                // Dropped rather than left empty, so the map stays the size of
                // what is actually resting.
                if held.is_empty() {
                    self.resting_per_account.remove(&key);
                }
            }
        }
    }

    fn emit_outcome(&mut self, command: &Command, side: Side, outcome: &Outcome) {
        self.push(command, EventKind::Received, command.order_id, 0, 0, 0);
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
    }

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
        self.push(
            command,
            EventKind::Rejected,
            command.order_id,
            0,
            0,
            reason as u8,
        );
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
