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
pub mod instrument;

use accounts::Accounts;
use book::{Execution, Outcome};
use bx_journal::{Journal, LogStorage};
use bx_protocol::{
    AccountId, Command, CommandKind, Event, EventKind, OrderId, Quantity, RejectReason, Sequence,
    Side, SymbolId, Ticks, TimeInForce,
};
use fastmap::FastMap;
use instrument::{Instrument, Instruments};
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

/// Orders one book holds before rejecting for capacity.
const DEFAULT_BOOK_CAPACITY: u32 = 65_535;

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
    events: Vec<Event>,
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
            books.insert(
                instrument.symbol,
                book::Book::new(*instrument, DEFAULT_BOOK_CAPACITY),
            );
        }
        Ok(Self {
            journal: Journal::open(storage)?,
            accounts: Accounts::new(),
            instruments,
            books,
            reservations: FastMap::default(),
            events: Vec::new(),
            event_sequence: 0,
            scratch: Outcome::default(),
        })
    }

    pub fn deposit(&mut self, account: AccountId, asset: instrument::AssetId, amount: u128) {
        self.accounts.deposit(account, asset, amount);
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

    /// Sequences, journals and applies one command, returning its events.
    ///
    /// The command is durable before it is applied, which is what lets a client
    /// be told "received" without that ever needing to be retracted.
    ///
    /// # Errors
    /// Fails only if the journal cannot be written. A refused command is not an
    /// error: it produces a `Rejected` event.
    pub fn submit(&mut self, command: &mut Command) -> bx_journal::Result<&[Event]> {
        self.journal.append(command)?;
        self.journal.sync()?;
        self.events.clear();
        self.apply(*command);
        Ok(&self.events)
    }

    /// Sequences and journals a whole batch, syncs **once**, then applies it.
    ///
    /// This is the throughput path. Durability is per batch rather than per
    /// command, which is not a weakening: no command in the batch is
    /// acknowledged until every one of them is durable. On a real disk the sync
    /// dominates everything else, so batching is the difference between tens of
    /// thousands and millions of commands per second.
    ///
    /// # Errors
    /// Fails only if the journal cannot be written.
    pub fn submit_batch(&mut self, commands: &mut [Command]) -> bx_journal::Result<&[Event]> {
        for command in commands.iter_mut() {
            self.journal.append(command)?;
        }
        self.journal.sync()?;
        self.events.clear();
        for command in commands.iter() {
            self.apply(*command);
        }
        Ok(&self.events)
    }

    /// Rebuilds state by replaying the journal. Events are not re-emitted;
    /// subscribers were told the first time.
    ///
    /// # Errors
    /// Fails if the journal is unreadable, corrupt, or has a gap.
    pub fn recover(&mut self) -> bx_journal::Result<u64> {
        let commands = self.journal.replay().collect_all()?;
        let count = commands.len() as u64;
        for command in commands {
            self.events.clear();
            self.apply(command);
        }
        self.events.clear();
        self.event_sequence = 0;
        Ok(count)
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

        // A market order carries no usable limit, so a buy holds the whole free
        // quote balance and gets the unspent part back afterwards.
        let market = command.price == Ticks::MIN;

        let (asset, amount) = match side {
            Side::Bid => {
                let quote = instrument.quote;
                let amount = if market {
                    // The engine matches on quantity and knows nothing about
                    // money, so the reservation has to cover the worst price
                    // the ladder can represent. Reserving merely "whatever is
                    // free" let a market buy sweep more than the account could
                    // pay for, and the shortfall surfaced as a failed settle
                    // after the trade had already happened.
                    //
                    // This is conservative: a proper affordability walk would
                    // price the actual sweep, which needs an early-exit
                    // traversal the engine does not expose today.
                    let Some(amount) =
                        instrument.notional(instrument.ceiling_ticks(), command.quantity)
                    else {
                        self.reject(command, RejectReason::QuantityTooLarge);
                        return;
                    };
                    amount
                } else {
                    let Some(amount) = instrument.notional(command.price, command.quantity) else {
                        self.reject(command, RejectReason::QuantityTooLarge);
                        return;
                    };
                    amount
                };
                (quote, amount)
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
        self.reservations.insert(
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
            let Some(notional) = instrument.notional(execution.price, execution.quantity) else {
                continue;
            };
            let Some(maker) = self.reservations.get(&execution.resting_order).copied() else {
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

            // A buy that traded below its limit over-reserved; give it back.
            if side == Side::Bid
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
            VIOLATIONS.fetch_add(1, Ordering::Relaxed);
        }
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
            let _ = self.accounts.release(account, asset, amount);
        }
    }

    fn release_all(&mut self, order: OrderId) {
        if let Some(reservation) = self.reservations.get(&order).copied() {
            self.release(order, reservation.remaining);
            self.reservations.remove(&order);
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

/// A market order: no usable limit, and a time in force that never rests.
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
        Ticks::MIN,
        quantity,
        TimeInForce::ImmediateOrCancel,
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
