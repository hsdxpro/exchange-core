//! Point-in-time state, so recovery does not replay from the beginning.
//!
//! The journal is still the source of truth. A snapshot is only an optimisation
//! and is never required to be correct: deleting every snapshot costs recovery
//! time and nothing else. That is deliberate, because a snapshot format is the
//! easiest place in a venue to introduce a bug that silently corrupts state, and
//! the cheapest defence is that the slow path is always available and always
//! authoritative.
//!
//! Recovery is therefore: load the newest snapshot, then replay the journal from
//! the sequence it records. Records are fixed width and sequences are contiguous
//! from zero, so that replay seeks straight to its first record rather than
//! reading past everything before it.
//!
//! Orders are written in price-then-time priority and restored in that same
//! order, so a restored book has the queue positions the lost one had — not
//! merely the same depth. A snapshot that only restored aggregate depth would
//! silently reorder the queue and change who gets filled first.

use crate::accounts::Balance;
use bx_protocol::{
    SNAPSHOT_MAGIC, Sequence, SnapshotBalance, SnapshotHeader, SnapshotOrder, SnapshotOrderIdMark,
    SnapshotStoppedAccount, SnapshotSymbolState,
};
use std::fmt;
use std::io::{self, Read, Write};
use zerocopy::{FromBytes, IntoBytes};

#[derive(Debug)]
pub enum SnapshotError {
    Io(io::Error),
    /// The bytes do not begin with our magic, or the version differs.
    NotASnapshot,
    /// The header promises more records than the file holds.
    Truncated,
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "snapshot I/O: {e}"),
            Self::NotASnapshot => f.write_str("not a snapshot, or its version differs"),
            Self::Truncated => f.write_str("snapshot ends before the records it promises"),
        }
    }
}

impl std::error::Error for SnapshotError {}

impl From<io::Error> for SnapshotError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, SnapshotError>;

/// Everything needed to rebuild the venue as of one journal position.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Snapshot {
    /// The first journal sequence **not** included here.
    pub sequence: Sequence,
    /// Resting orders, in the priority order they must be restored in.
    pub orders: Vec<SnapshotOrder>,
    /// Account holdings, sorted, so the same state always writes the same bytes.
    pub balances: Vec<SnapshotBalance>,
    /// Highest order ID accepted per account, sorted for the same reason.
    ///
    /// Carried because recovery from a snapshot replays only the journal after
    /// it. Rebuilt from that alone, the mark would forget every ID used before
    /// the snapshot and start accepting them again -- which is exactly the
    /// replayed order it exists to refuse.
    pub order_id_marks: Vec<SnapshotOrderIdMark>,
    /// Symbols whose trading state is not the default, sorted.
    ///
    /// An operator's halt is venue state and has to survive a recovery. A venue
    /// that came back trading a symbol somebody had stopped would be the worst
    /// kind of recovery bug: it looks like success.
    pub symbol_states: Vec<SnapshotSymbolState>,
    /// Accounts stopped from opening new risk, sorted.
    pub stopped_accounts: Vec<SnapshotStoppedAccount>,
}

impl Snapshot {
    /// # Errors
    /// Returns the underlying I/O error if writing fails.
    pub fn write_to(&self, out: &mut impl Write) -> Result<()> {
        let header = SnapshotHeader {
            magic: SNAPSHOT_MAGIC,
            sequence: self.sequence,
            orders: self.orders.len() as u64,
            balances: self.balances.len() as u64,
            order_id_marks: self.order_id_marks.len() as u64,
            symbol_states: self.symbol_states.len() as u64,
            stopped_accounts: self.stopped_accounts.len() as u64,
            _pad: [0; 8],
        };
        out.write_all(header.as_bytes())?;
        out.write_all(self.orders.as_bytes())?;
        out.write_all(self.balances.as_bytes())?;
        out.write_all(self.order_id_marks.as_bytes())?;
        out.write_all(self.symbol_states.as_bytes())?;
        out.write_all(self.stopped_accounts.as_bytes())?;
        Ok(())
    }

    /// # Errors
    /// [`SnapshotError::NotASnapshot`] if the magic does not match, and
    /// [`SnapshotError::Truncated`] if the file is shorter than its header says.
    pub fn read_from(input: &mut impl Read) -> Result<Self> {
        let mut bytes = Vec::new();
        input.read_to_end(&mut bytes)?;

        let Ok((header, rest)) = SnapshotHeader::read_from_prefix(&bytes) else {
            return Err(SnapshotError::NotASnapshot);
        };
        if header.magic != SNAPSHOT_MAGIC {
            return Err(SnapshotError::NotASnapshot);
        }

        let order_bytes = header.orders as usize * size_of::<SnapshotOrder>();
        if rest.len() < order_bytes {
            return Err(SnapshotError::Truncated);
        }
        let (orders, rest) = rest.split_at(order_bytes);
        let Ok(orders) = <[SnapshotOrder]>::ref_from_bytes(orders) else {
            return Err(SnapshotError::Truncated);
        };

        let balance_bytes = header.balances as usize * size_of::<SnapshotBalance>();
        if rest.len() < balance_bytes {
            return Err(SnapshotError::Truncated);
        }
        let (balances, rest) = rest.split_at(balance_bytes);
        let Ok(balances) = <[SnapshotBalance]>::ref_from_bytes(balances) else {
            return Err(SnapshotError::Truncated);
        };

        let mark_bytes = header.order_id_marks as usize * size_of::<SnapshotOrderIdMark>();
        if rest.len() < mark_bytes {
            return Err(SnapshotError::Truncated);
        }
        let (marks, rest) = rest.split_at(mark_bytes);
        let Ok(order_id_marks) = <[SnapshotOrderIdMark]>::ref_from_bytes(marks) else {
            return Err(SnapshotError::Truncated);
        };

        let state_bytes = header.symbol_states as usize * size_of::<SnapshotSymbolState>();
        if rest.len() < state_bytes {
            return Err(SnapshotError::Truncated);
        }
        let (states, rest) = rest.split_at(state_bytes);
        let Ok(symbol_states) = <[SnapshotSymbolState]>::ref_from_bytes(states) else {
            return Err(SnapshotError::Truncated);
        };

        let stopped_bytes = header.stopped_accounts as usize * size_of::<SnapshotStoppedAccount>();
        if rest.len() < stopped_bytes {
            return Err(SnapshotError::Truncated);
        }
        let Ok(stopped_accounts) =
            <[SnapshotStoppedAccount]>::ref_from_bytes(&rest[..stopped_bytes])
        else {
            return Err(SnapshotError::Truncated);
        };

        Ok(Self {
            sequence: header.sequence,
            orders: orders.to_vec(),
            balances: balances.to_vec(),
            order_id_marks: order_id_marks.to_vec(),
            symbol_states: symbol_states.to_vec(),
            stopped_accounts: stopped_accounts.to_vec(),
        })
    }
}

/// Converts a stored holding back into the in-memory form.
#[must_use]
pub fn balance_of(record: &SnapshotBalance) -> Balance {
    Balance {
        free: record.free,
        reserved: record.reserved,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Snapshot {
        Snapshot {
            sequence: 4_096,
            orders: vec![
                SnapshotOrder {
                    reserved: 1_000,
                    order_id: 1,
                    account: 7,
                    quantity: 5,
                    price: 10_100,
                    symbol: 1,
                    side: 0,
                    _pad: [0; 11],
                },
                SnapshotOrder {
                    reserved: 2_000,
                    order_id: 2,
                    account: 8,
                    quantity: 3,
                    price: 10_110,
                    symbol: 1,
                    side: 1,
                    _pad: [0; 11],
                },
            ],
            balances: vec![SnapshotBalance {
                free: u128::from(u64::MAX) * 4,
                reserved: 3_000,
                account: 7,
                asset: 2,
                _pad: [0; 4],
            }],
            symbol_states: vec![SnapshotSymbolState {
                symbol: 1,
                state: 1,
                _pad: [0; 3],
            }],
            stopped_accounts: vec![SnapshotStoppedAccount { account: 9 }],
            order_id_marks: vec![
                SnapshotOrderIdMark {
                    account: 7,
                    highest: 41,
                },
                SnapshotOrderIdMark {
                    account: 8,
                    highest: 900,
                },
            ],
        }
    }

    #[test]
    fn a_snapshot_round_trips_through_bytes() {
        let snapshot = sample();
        let mut bytes = Vec::new();
        snapshot.write_to(&mut bytes).unwrap();
        assert_eq!(
            Snapshot::read_from(&mut bytes.as_slice()).unwrap(),
            snapshot
        );
    }

    #[test]
    fn the_same_state_always_writes_the_same_bytes() {
        let mut first = Vec::new();
        sample().write_to(&mut first).unwrap();
        let mut second = Vec::new();
        sample().write_to(&mut second).unwrap();
        assert_eq!(first, second, "snapshots are not reproducible");
    }

    #[test]
    fn an_empty_snapshot_is_valid() {
        let snapshot = Snapshot::default();
        let mut bytes = Vec::new();
        snapshot.write_to(&mut bytes).unwrap();
        assert_eq!(bytes.len(), size_of::<SnapshotHeader>());
        assert_eq!(
            Snapshot::read_from(&mut bytes.as_slice()).unwrap(),
            snapshot
        );
    }

    #[test]
    fn a_foreign_file_is_refused_rather_than_misread() {
        let bytes = vec![0_u8; 128];
        assert!(matches!(
            Snapshot::read_from(&mut bytes.as_slice()),
            Err(SnapshotError::NotASnapshot)
        ));
    }

    #[test]
    fn a_file_shorter_than_its_header_promises_is_refused() {
        let mut bytes = Vec::new();
        sample().write_to(&mut bytes).unwrap();
        bytes.truncate(bytes.len() - 8);
        assert!(matches!(
            Snapshot::read_from(&mut bytes.as_slice()),
            Err(SnapshotError::Truncated)
        ));
    }

    #[test]
    fn a_header_alone_with_a_record_count_is_refused() {
        // A crash between writing the header and writing the records.
        let mut bytes = Vec::new();
        sample().write_to(&mut bytes).unwrap();
        bytes.truncate(size_of::<SnapshotHeader>());
        assert!(matches!(
            Snapshot::read_from(&mut bytes.as_slice()),
            Err(SnapshotError::Truncated)
        ));
    }
}
