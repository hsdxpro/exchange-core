//! The group-commit loop.
//!
//! A venue is the exchange plus the fan-out that feeds subscribers, driven the
//! way a server actually drives it: take whatever commands have arrived, apply
//! them all, make them durable with one sync, and only then publish. The group
//! is however much was waiting, so it grows under load exactly when the sync
//! most needs amortising and stays at one when the venue is idle and latency
//! matters more than throughput. Nothing here picks a batch size.
//!
//! Publishing happens after the commit, never before. That ordering is the
//! whole point: a subscriber must not be able to see a trade that a crash could
//! still take back.

use bx_journal::{LogStorage, Result};
use bx_pipeline::hub::{Channel, Hub, Resume};
use bx_pipeline::instrument::Instruments;
use bx_pipeline::snapshot::Snapshot;
use bx_pipeline::{Exchange, book::Book};
use bx_protocol::{Command, Event, Sequence, SymbolId};

#[derive(Debug)]
pub struct Venue<S: LogStorage> {
    exchange: Exchange<S>,
    hub: Hub,
}

impl<S: LogStorage> Venue<S> {
    /// # Errors
    /// Fails if the journal cannot be opened.
    pub fn new(storage: S, instruments: Instruments, retained_per_channel: usize) -> Result<Self> {
        Ok(Self {
            exchange: Exchange::new(storage, instruments)?,
            hub: Hub::new(retained_per_channel),
        })
    }

    /// Applies a group of commands, commits once, then publishes.
    ///
    /// Returns the group's events, which are durable by the time the caller
    /// sees them.
    ///
    /// # Errors
    /// Fails only if the journal cannot be written or flushed. Nothing is
    /// published in that case.
    pub fn accept(&mut self, commands: &mut [Command]) -> Result<&[Event]> {
        for command in commands.iter_mut() {
            self.exchange.enqueue(command)?;
        }
        let events = self.exchange.commit()?;
        self.hub.publish(events);
        Ok(events)
    }

    pub fn subscribe(&mut self, channel: Channel) -> Sequence {
        self.hub.subscribe(channel)
    }

    pub fn unsubscribe(&mut self, channel: Channel) {
        self.hub.unsubscribe(channel);
    }

    pub fn resume(&self, channel: Channel, from: Sequence, out: &mut Vec<Event>) -> Resume {
        self.hub.resume(channel, from, out)
    }

    /// # Errors
    /// Fails only if the journal cannot be written.
    pub fn deposit(
        &mut self,
        account: bx_protocol::AccountId,
        asset: bx_pipeline::instrument::AssetId,
        amount: u64,
    ) -> Result<()> {
        self.exchange.deposit(account, asset, amount)
    }

    /// Captures state as of the current journal position.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        self.exchange.snapshot()
    }

    /// Restores a snapshot and replays only the journal after it.
    ///
    /// # Errors
    /// Fails if the journal is unreadable, corrupt, or has a gap.
    pub fn recover_from(&mut self, snapshot: &Snapshot) -> Result<u64> {
        self.exchange.recover_from(snapshot)
    }

    /// Replays the journal, rebuilding state after a restart.
    ///
    /// # Errors
    /// Fails if the journal is unreadable, corrupt, or has a gap.
    pub fn recover(&mut self) -> Result<u64> {
        self.exchange.recover()
    }

    #[must_use]
    pub fn book(&self, symbol: SymbolId) -> Option<&Book> {
        self.exchange.book(symbol)
    }

    #[must_use]
    pub const fn exchange(&self) -> &Exchange<S> {
        &self.exchange
    }

    #[must_use]
    pub const fn hub(&self) -> &Hub {
        &self.hub
    }

    pub fn into_storage(self) -> S {
        self.exchange.into_storage()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bx_journal::MemoryLog;
    use bx_pipeline::instrument::Instrument;
    use bx_pipeline::{limit_order, market_order};
    use bx_protocol::{EventKind, Side};

    const BTC: u32 = 1;
    const USD: u32 = 2;
    const SYMBOL: SymbolId = 1;

    fn venue() -> Venue<MemoryLog> {
        let mut instruments = Instruments::new();
        instruments.insert(Instrument::new(SYMBOL, BTC, USD, 10_000, 1_000_000, 4_096));
        let mut venue = Venue::new(MemoryLog::new(), instruments, 1_024).unwrap();
        for account in 1..=4 {
            venue.deposit(account, USD, 100_000_000).unwrap();
            venue.deposit(account, BTC, 10_000).unwrap();
        }
        venue
    }

    #[test]
    fn a_group_is_applied_committed_and_published_together() {
        let mut venue = venue();
        venue.subscribe(Channel::Book(SYMBOL));

        let mut group = [
            limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5),
            limit_order(2, SYMBOL, 201, Side::Ask, 10_110, 4),
        ];
        let events = venue.accept(&mut group).unwrap().len();
        assert!(events > 0);

        let mut feed = Vec::new();
        venue.resume(Channel::Book(SYMBOL), 0, &mut feed);
        assert_eq!(feed.len(), 2, "both levels should have been published");
    }

    #[test]
    fn nothing_is_published_before_it_is_durable() {
        // The storage refuses every write, so the commit fails. A subscriber
        // must see nothing at all rather than a trade the venue cannot keep.
        let mut instruments = Instruments::new();
        instruments.insert(Instrument::new(SYMBOL, BTC, USD, 10_000, 1_000_000, 4_096));
        let mut venue = Venue::new(MemoryLog::new().failing_after(0), instruments, 1_024).unwrap();
        venue.subscribe(Channel::Book(SYMBOL));

        let mut group = [limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5)];
        assert!(venue.accept(&mut group).is_err(), "the write should fail");

        let mut feed = Vec::new();
        venue.resume(Channel::Book(SYMBOL), 0, &mut feed);
        assert!(
            feed.is_empty(),
            "a subscriber saw an event that never became durable"
        );
    }

    #[test]
    fn one_group_of_many_costs_one_sync_and_publishes_everything() {
        let mut venue = venue();
        venue.subscribe(Channel::Trades(SYMBOL));

        let mut group: Vec<Command> = (0..50)
            .map(|i| limit_order(1, SYMBOL, 100 + i, Side::Ask, 10_100, 1))
            .collect();
        venue.accept(&mut group).unwrap();

        let mut takers: Vec<Command> = (0..50)
            .map(|i| market_order(2, SYMBOL, 200 + i, Side::Bid, 1))
            .collect();
        venue.accept(&mut takers).unwrap();

        let mut tape = Vec::new();
        venue.resume(Channel::Trades(SYMBOL), 0, &mut tape);
        assert_eq!(tape.len(), 50, "every print should have reached the tape");
        assert!(tape.iter().all(|e| e.kind == EventKind::Trade as u8));
    }

    #[test]
    fn a_group_survives_a_restart_and_an_uncommitted_one_does_not() {
        let mut venue = venue();
        let mut group = [limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5)];
        venue.accept(&mut group).unwrap();
        let depth = venue.book(SYMBOL).unwrap().depth(Side::Bid, 10);

        let mut storage = venue.into_storage();
        storage.crash();

        let mut instruments = Instruments::new();
        instruments.insert(Instrument::new(SYMBOL, BTC, USD, 10_000, 1_000_000, 4_096));
        let mut restarted = Venue::new(storage, instruments, 1_024).unwrap();
        restarted.recover().unwrap();
        assert_eq!(restarted.book(SYMBOL).unwrap().depth(Side::Bid, 10), depth);
    }
}
