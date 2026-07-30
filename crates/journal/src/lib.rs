//! Append-only log of sequenced commands, and replay.
//!
//! The journal is the exchange's source of truth. Every other piece of state is
//! derived from it, so recovery is: replay from a snapshot to the end. Because
//! the pipeline downstream of the sequencer is deterministic, the recovered
//! state is identical to the state that was lost.
//!
//! Storage sits behind [`LogStorage`] rather than being `std::fs` directly, so
//! the simulator can run a whole cluster in memory and inject torn writes and
//! I/O errors that are impractical to provoke on a real disk.

pub mod replication;

use bx_protocol::{Command, Sequence};
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use zerocopy::{FromBytes, IntoBytes};

/// Every record is one command, one cache line.
pub const RECORD_LEN: usize = size_of::<Command>();

/// Identifies the file and the layout version. A journal written by a different
/// version is refused rather than misread.
const MAGIC: [u8; 8] = *b"BXJRNL\x01\x00";
const HEADER_LEN: u64 = MAGIC.len() as u64;

#[derive(Debug)]
pub enum JournalError {
    Io(io::Error),
    /// The file does not begin with our magic, or the version differs.
    NotAJournal,
    /// A record decoded but its discriminants are not values this version
    /// defines. The journal is corrupt, not merely truncated.
    CorruptRecord {
        offset: u64,
    },
    /// Sequences must be contiguous. A hole means records were lost, which is
    /// materially different from the log simply ending.
    SequenceGap {
        expected: Sequence,
        found: Sequence,
    },
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "journal I/O: {e}"),
            Self::NotAJournal => f.write_str("file is not a journal, or its version differs"),
            Self::CorruptRecord { offset } => write!(f, "corrupt record at offset {offset}"),
            Self::SequenceGap { expected, found } => {
                write!(f, "sequence gap: expected {expected}, found {found}")
            }
        }
    }
}

impl std::error::Error for JournalError {}

impl From<io::Error> for JournalError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, JournalError>;

pub use replication::{Replica, ReplicatedLog, bound_listener};

/// Somewhere bytes can be appended and read back.
///
/// Deliberately narrow: append, durably flush, read at an offset, report
/// length. Anything a real disk can do that this cannot, the journal does not
/// depend on.
pub trait LogStorage {
    /// # Errors
    /// Returns the underlying I/O error if the write fails.
    fn append(&mut self, bytes: &[u8]) -> io::Result<()>;

    /// Makes every prior append durable. Until this returns, nothing written is
    /// guaranteed to survive a crash.
    ///
    /// # Errors
    /// Returns the underlying I/O error if the flush fails.
    fn sync(&mut self) -> io::Result<()>;

    /// Hands buffered appends to the operating system without waiting for the
    /// device.
    ///
    /// The distinction matters when durability comes from somewhere else. A
    /// replicated leader is acknowledged once a majority holds a group, so it must
    /// not wait on its own platter -- but the bytes still have to leave the
    /// process, or a log that buffers until `sync` never gets written at all.
    /// Surviving a crashed process and surviving lost power are different
    /// promises, and this is the first one.
    ///
    /// # Errors
    /// Returns the underlying I/O error if the write fails.
    fn flush(&mut self) -> io::Result<()> {
        self.sync()
    }

    /// # Errors
    /// Returns the underlying I/O error if the read fails.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

    /// Byte offset one past the last byte: where the next append will land.
    ///
    /// Named for the position rather than a length, because a log always carries a
    /// header and "empty" would be ambiguous about whether that counts.
    ///
    /// Asked for directly rather than found by probing. A promotion needs to know
    /// how far behind it is before it can catch up, and walking the log a record
    /// at a time to measure it turned that into one syscall per record -- minutes
    /// on a large log, on the one path where the venue is already down.
    ///
    /// # Errors
    /// Returns the underlying I/O error if the length cannot be read.
    fn end(&self) -> io::Result<u64>;

    /// Discards everything past `len`, so a partial trailing record left by a
    /// crash is removed before anything is appended after it.
    ///
    /// # Errors
    /// Returns the underlying I/O error if the truncation fails.
    fn truncate(&mut self, len: u64) -> io::Result<()>;
}

/// A journal in a real file.
///
/// Appends are buffered and written once per [`LogStorage::sync`]. That is the
/// other half of group commit, and it was missing: syncing once per group while
/// still issuing one `write` per record meant a group of sixteen thousand
/// commands made sixteen thousand syscalls, which dominated everything. Durable
/// throughput was stuck near 345,000 commands a second no matter how large the
/// group grew, because the cost per command never fell.
///
/// The buffer holds one group. Its size is therefore whatever the caller
/// enqueues before committing, which the gateway bounds per pass; nothing here
/// grows without an unbounded caller above it.
#[derive(Debug)]
pub struct FileLog {
    file: File,
    /// Appended but not yet written. Empty whenever a sync has succeeded.
    pending: Vec<u8>,
}

impl FileLog {
    /// Opens an existing journal, or creates one.
    ///
    /// # Errors
    /// Fails if the file cannot be opened, or exists but is not a journal.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let len = file.metadata()?.len();

        if len == 0 {
            file.write_all(&MAGIC)?;
            file.sync_all()?;
            return Ok(Self {
                file,
                pending: Vec::new(),
            });
        }

        let mut magic = [0_u8; MAGIC.len()];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(JournalError::NotAJournal);
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            pending: Vec::new(),
        })
    }
}

impl LogStorage for FileLog {
    /// Buffers. Nothing reaches the file until [`Self::sync`], which is exactly
    /// the guarantee the journal already relied on: an append is not durable
    /// until a sync says so.
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.pending.extend_from_slice(bytes);
        Ok(())
    }

    /// Writes the whole group in one call, then makes it durable.
    ///
    /// The buffer is only cleared once the write succeeds, so a failed write
    /// leaves the group pending and retryable rather than silently dropped.
    fn sync(&mut self) -> io::Result<()> {
        self.flush()?;
        self.file.sync_data()
    }

    /// Writes without waiting for the device.
    fn flush(&mut self) -> io::Result<()> {
        if !self.pending.is_empty() {
            self.file.write_all(&self.pending)?;
            self.pending.clear();
        }
        Ok(())
    }

    fn end(&self) -> io::Result<u64> {
        // The file, not the buffer: a pending append is not part of the log until
        // it is written, which is the same boundary reads already see.
        self.file.metadata().map(|m| m.len())
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        // Reads see what is on the file, not what is buffered. That is the
        // correct boundary: replaying an unsynced append would reconstruct a
        // command no client was ever told about.
        //
        // Appends write at the cursor, so reading has to put it back. A cloned
        // handle would not help: `dup` shares the offset, so seeking the clone
        // seeks the original and the next append lands mid-log.
        let mut file = &self.file;
        file.seek(SeekFrom::Start(offset))?;
        let read = file.read(buf)?;
        file.seek(SeekFrom::End(0))?;
        Ok(read)
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        // Anything buffered was never durable, and the truncation is undoing a
        // torn tail, so keeping it would append past the very hole being cut.
        self.pending.clear();
        self.file.set_len(len)?;
        self.file.seek(SeekFrom::End(0))?;
        self.file.sync_all()
    }
}

/// A journal in memory, for component tests and simulation.
///
/// The failure injectors exist so tests can stage the two failures that matter
/// and are otherwise very hard to provoke: an I/O error mid-write, and a torn
/// record left by a crash between the write and the flush.
#[derive(Debug, Default)]
pub struct MemoryLog {
    bytes: Vec<u8>,
    synced_len: u64,
    fail_after: Option<usize>,
    appends: usize,
    tear_at_append: Option<usize>,
    fail_sync_after: Option<usize>,
    syncs: usize,
}

impl MemoryLog {
    #[must_use]
    pub fn new() -> Self {
        Self {
            bytes: MAGIC.to_vec(),
            synced_len: HEADER_LEN,
            ..Self::default()
        }
    }

    /// Every append after `n` fails with an I/O error.
    #[must_use]
    pub fn failing_after(mut self, n: usize) -> Self {
        self.fail_after = Some(n);
        self
    }

    /// Append number `n` writes only half its bytes, as a crash between the
    /// write and the flush would leave it.
    #[must_use]
    pub fn tearing_append(mut self, n: usize) -> Self {
        self.tear_at_append = Some(n);
        self
    }

    /// Every sync after `n` fails, which is where a full disk actually bites.
    ///
    /// `failing_after` breaks the append, but `FileLog` buffers appends in
    /// memory and touches the device only at sync -- so on the real storage,
    /// ENOSPC surfaces here and nowhere else. A suite that only failed appends
    /// was testing a failure the shipped storage cannot have.
    #[must_use]
    pub fn failing_sync_after(mut self, n: usize) -> Self {
        self.fail_sync_after = Some(n);
        self
    }

    /// Clears injected failures: the operator freed disk space, or the device
    /// came back. What was appended but never synced is still not durable.
    pub fn repair(&mut self) {
        self.fail_after = None;
        self.fail_sync_after = None;
    }

    /// Discards everything not yet synced, simulating power loss.
    pub fn crash(&mut self) {
        self.bytes.truncate(self.synced_len as usize);
    }

    /// Overwrites bytes in place, to stage corruption a test needs.
    pub fn overwrite(&mut self, offset: usize, bytes: &[u8]) {
        self.bytes[offset..offset + bytes.len()].copy_from_slice(bytes);
    }
}

impl LogStorage for MemoryLog {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.appends += 1;
        if self.fail_after.is_some_and(|n| self.appends > n) {
            return Err(io::Error::other("simulated device failure"));
        }
        if self.tear_at_append == Some(self.appends) {
            let half = bytes.len() / 2;
            self.bytes.extend_from_slice(&bytes[..half]);
            return Ok(());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.syncs += 1;
        if self.fail_sync_after.is_some_and(|n| self.syncs > n) {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "simulated full disk",
            ));
        }
        self.synced_len = self.bytes.len() as u64;
        Ok(())
    }

    fn end(&self) -> io::Result<u64> {
        Ok(self.bytes.len() as u64)
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(self.bytes.len());
        let available = &self.bytes[start..];
        let n = available.len().min(buf.len());
        buf[..n].copy_from_slice(&available[..n]);
        Ok(n)
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.bytes.truncate(len as usize);
        self.synced_len = self.synced_len.min(len);
        Ok(())
    }
}

/// Bytes in a chain head. A full SHA-256 digest, untruncated.
pub const CHAIN_LEN: usize = 32;

/// The chain over an empty journal, which is what a fresh venue starts from.
pub const EMPTY_CHAIN: [u8; CHAIN_LEN] = [0; CHAIN_LEN];

/// Records between one chain head and the next.
///
/// A power of two and deliberately coarse. Finalising a digest is what costs, so
/// this divides that cost by 1,024 -- and a client verifying the feed wants a
/// head it can check against a run of records, not one per order it would have
/// to store. The interval bounds how far behind the newest command a checkable
/// head can be, which for a venue publishing at millions a second is under a
/// millisecond.
pub const CHAIN_INTERVAL: u64 = 1_024;

/// Smallest interval that means anything: every record.
///
/// Allowed, and expensive -- it is the per-record digest the interval exists to
/// avoid. Useful to a venue that wants a checkable head behind every single
/// command and is willing to pay 45 to 60 ns for it.
pub const MIN_CHAIN_INTERVAL: u64 = 1;

/// A digest primed with a chain head, ready for the records that follow it.
fn seeded(head: &[u8; CHAIN_LEN]) -> Sha256 {
    let mut hasher = Sha256::new();
    hasher.update(head);
    hasher
}

/// Extends a chain by one group of records.
///
/// One function, so the venue that publishes a head and anyone recomputing it
/// from the feed cannot disagree about what is hashed or in what order. A client
/// with the previous head and the records of a group arrives at the next head or
/// finds it does not, and there is nothing else to know.
#[must_use]
pub fn chain_next<'a>(
    head: &[u8; CHAIN_LEN],
    records: impl IntoIterator<Item = &'a [u8]>,
) -> [u8; CHAIN_LEN] {
    let mut hasher = seeded(head);
    for record in records {
        hasher.update(record);
    }
    hasher.finalize().into()
}

/// Appends sequenced commands and replays them.
#[derive(Debug)]
pub struct Journal<S: LogStorage> {
    storage: S,
    next_sequence: Sequence,
    /// Running hash over every record appended, in sequence order.
    ///
    /// `h[n] = SHA-256(h[n-1] ‖ record[n])`, starting from [`EMPTY_CHAIN`]. It
    /// commits to the *order* as well as the contents: swapping two records, or
    /// inserting one, changes every head after that point. That is what lets a
    /// client hold the venue to what it published -- given the stream and a head
    /// the venue signed, the client can recompute and see for itself that its
    /// order was included where it was told and that nothing was slipped in
    /// front of it. Answering "did the sequencer front-run me" otherwise takes
    /// trust, and this is a venue whose whole point is not needing any.
    ///
    /// This is the Certificate Transparency shape: an append-only log whose head
    /// commits to everything before it. Pointing it at a matching engine's
    /// sequencer is the part worth having, and it is cheap here because the
    /// sequencer is a single writer over fixed 64-byte records -- one hash per
    /// command, no tree, no extra structure.
    ///
    /// A hash chain rather than a Merkle tree, deliberately. A tree buys compact
    /// inclusion proofs for a client that does *not* hold the stream; every
    /// client here can follow the feed, so the chain gives the same guarantee for
    /// one hash instead of a logarithmic path per query. The tree is the upgrade
    /// if that ever stops being true.
    ///
    /// ## The head advances every [`CHAIN_INTERVAL`] records
    ///
    /// A digest finalised for every record cost 45 to 60 ns on a path that takes
    /// about 200 -- half again on a cancel -- and almost none of that is the
    /// hashing. SHA-256 works in 64-byte blocks, a command *is* one block, and
    /// the expense is the setup and padding around it, paid once per call. So
    /// records are folded in as they arrive and the digest is finalised only
    /// every so often, which spreads that cost over the interval.
    ///
    /// The boundary is a fixed count of records, and it has to be. Sealing per
    /// *group* was the obvious choice -- a group is one sync and one
    /// acknowledgement, so it is the unit the venue already commits at -- and it
    /// is wrong, because group boundaries are not written to the journal. A
    /// replay reads records and has no idea where one group ended and the next
    /// began, so it could not reproduce the head, and a chain a replay cannot
    /// reproduce is a chain nobody can check. A count of records is in the
    /// stream by construction.
    ///
    /// Ordering is still fully covered: records are folded in sequence, and the
    /// digest changes if any two of them swap or one is inserted.
    chain: [u8; CHAIN_LEN],
    /// The digest in progress: the committed head, then every record appended
    /// since. Finalised by [`Journal::sync`].
    pending: Sha256,
    /// Records folded into `pending` since the last sync, so a sync with nothing
    /// to commit leaves the head alone rather than hashing it again.
    pending_records: usize,
    /// Whether to maintain the chain at all.
    ///
    /// Off by default, because it is not free and the cost falls entirely on the
    /// latency-sensitive shape. Measured: a group of one pays about 70 ns on a
    /// path of roughly 200, since a group of one means a digest finalised per
    /// command. Under batching it disappears -- at a group of 1,024 the same
    /// digest covers a thousand records.
    ///
    /// So a venue that acknowledges one order at a time and cares about
    /// microseconds turns it off; a venue that wants clients to be able to check
    /// its ordering turns it on and pays for it in a place where it is already
    /// batching. Neither is the right answer for both, which is why this is a
    /// setting rather than a decision made here.
    chaining: bool,
    /// Records this journal has appended since it was opened.
    ///
    /// Not the sequence: a journal opened over existing records starts with a
    /// high sequence and nothing appended, and those two cases need telling
    /// apart. Only used to refuse turning chaining on mid-stream.
    appended: u64,
    /// Records between heads. See [`CHAIN_INTERVAL`].
    ///
    /// Settable because it is a trade an operator owns: a shorter interval means
    /// a client can check a more recent head and the venue finalises a digest
    /// more often. Whatever it is set to has to be told to clients, since a
    /// verifier needs to know where the boundaries fall.
    chain_interval: u64,
}

impl<S: LogStorage> Journal<S> {
    /// Opens a journal over `storage`, scanning it to find where it left off.
    ///
    /// # Errors
    /// Fails on I/O error, or if the log contains a corrupt record or a gap.
    pub fn open(mut storage: S) -> Result<Self> {
        let (last_sequence, intact_len) = Replay::new(&storage).scan()?;
        // Drop any partial trailing record before appending. Leaving it would
        // put the next append after the torn bytes, and every record from there
        // on would be unreachable — replay stops at the tear.
        storage.truncate(intact_len)?;
        Ok(Self {
            storage,
            next_sequence: last_sequence.map_or(0, |s| s + 1),
            // Rebuilt by whoever recovers, from a snapshot or by replay. Opening
            // a journal does not read every record, so it cannot be computed
            // here without paying for a full scan on every start.
            chain: EMPTY_CHAIN,
            pending: seeded(&EMPTY_CHAIN),
            pending_records: 0,
            chaining: false,
            appended: 0,
            chain_interval: CHAIN_INTERVAL,
        })
    }

    /// The sequence the next appended command will receive.
    #[must_use]
    pub fn next_sequence(&self) -> Sequence {
        self.next_sequence
    }

    /// Stamps `command` with the next sequence and appends it. Does not flush;
    /// call [`Journal::sync`] before treating it as durable.
    ///
    /// # Errors
    /// Returns the underlying I/O error if the append fails.
    pub fn append(&mut self, command: &mut Command) -> Result<Sequence> {
        command.sequence = self.next_sequence;
        let bytes = command.as_bytes();
        self.storage.append(bytes)?;
        if self.chaining {
            // Folded in, not finalised: see the note on `chain`.
            self.pending.update(bytes);
            self.pending_records += 1;
            // Sealed on a boundary the journal itself defines, so a replay lands
            // on the same heads.
            if self
                .next_sequence
                .wrapping_add(1)
                .is_multiple_of(self.chain_interval)
            {
                self.seal();
            }
        }
        self.next_sequence += 1;
        self.appended += 1;
        Ok(command.sequence)
    }

    /// The chain over everything appended so far, sealed at the last group.
    ///
    /// [`EMPTY_CHAIN`] when chaining is off.
    #[must_use]
    pub const fn chain_head(&self) -> [u8; CHAIN_LEN] {
        self.chain
    }

    /// Whether this journal maintains a verifiable chain over its records.
    #[must_use]
    pub const fn chaining(&self) -> bool {
        self.chaining
    }

    /// Records between chain heads.
    #[must_use]
    pub const fn chain_interval(&self) -> u64 {
        self.chain_interval
    }

    /// Sets how many records fall between heads.
    ///
    /// # Panics
    /// If `interval` is zero, which would mean no boundary at all and a head
    /// that never advanced.
    pub fn set_chain_interval(&mut self, interval: u64) {
        assert!(
            interval >= MIN_CHAIN_INTERVAL,
            "a chain interval of zero has no boundary, so the head would never advance"
        );
        self.chain_interval = interval;
    }

    /// Turns chaining on. See the note on the field for what it costs.
    ///
    /// Only before the first record. A chain cannot be retrofitted: switching it
    /// on over a journal that already holds records gives a head covering the
    /// suffix from here, while a replay of that journal hashes all of it and
    /// arrives somewhere else. The venue and every client checking it would then
    /// disagree, for a reason neither could see -- so it is refused instead.
    ///
    /// This makes chaining a decision taken before a venue accepts its first
    /// order, which is what it is: a client cannot be given a commitment over
    /// history nobody hashed.
    ///
    /// A journal opened over existing records may still turn it on, because what
    /// follows is a recovery: the chain is rebuilt by replaying from the start or
    /// restored from a snapshot, and either way it ends up covering everything.
    /// What is refused is switching it on *after this journal has appended*,
    /// which is the mid-stream case.
    ///
    /// # Panics
    /// If this journal has already appended a record.
    pub fn set_chaining(&mut self, on: bool) {
        assert!(
            self.appended == 0,
            "chaining has to be set before appending: {} records have gone in              already, and a chain started here would cover only what follows              while a replay would cover everything",
            self.appended
        );
        self.chaining = on;
        self.pending = seeded(&self.chain);
        self.pending_records = 0;
    }

    /// Folds a replayed record into the chain, sealing on the interval.
    ///
    /// The replay counterpart of [`Self::append`]: recovery reads records rather
    /// than writing them, so without this a recovered venue would publish a head
    /// over nothing.
    pub fn fold_replayed(&mut self, record: &[u8], sequence: Sequence) {
        if !self.chaining {
            return;
        }
        self.pending.update(record);
        self.pending_records += 1;
        if sequence.wrapping_add(1).is_multiple_of(self.chain_interval) {
            self.seal();
        }
    }

    /// Sets the chain head, for a recovery that restored one from a snapshot.
    ///
    /// Replay carries on from here, so a snapshot-based recovery reaches the same
    /// head a full replay would -- which is the property that makes the chain
    /// worth publishing at all.
    pub fn restore_chain(&mut self, head: [u8; CHAIN_LEN]) {
        self.chain = head;
        self.pending = seeded(&head);
        self.pending_records = 0;
    }

    /// # Errors
    /// Returns the underlying I/O error if the flush fails.
    pub fn sync(&mut self) -> Result<()> {
        self.storage.sync()?;
        Ok(())
    }

    /// Finalises the digest over the records folded in since the last seal.
    ///
    /// Called on every [`CHAIN_INTERVAL`] boundary, from appends and from replay
    /// alike, which is what makes the two agree.
    pub fn seal(&mut self) {
        if self.pending_records == 0 {
            return;
        }
        self.chain = std::mem::replace(&mut self.pending, Sha256::new())
            .finalize()
            .into();
        self.pending = seeded(&self.chain);
        self.pending_records = 0;
    }

    #[must_use]
    pub fn replay(&self) -> Replay<'_, S> {
        Replay::new(&self.storage)
    }

    #[must_use]
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// The storage underneath, mutably. For tests and tools that need to
    /// interfere with it — a deployment drives it through the journal.
    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }

    pub fn into_storage(self) -> S {
        self.storage
    }
}

/// Reads a journal back, in order.
///
/// A trailing partial record is the end of the log, not corruption: that is
/// what a crash between write and flush leaves behind, and it is expected. A
/// record that is complete but malformed, or a sequence that jumps, is an
/// error.
#[derive(Debug)]
pub struct Replay<'a, S: LogStorage> {
    storage: &'a S,
    offset: u64,
    expected_sequence: Option<Sequence>,
}

impl<'a, S: LogStorage> Replay<'a, S> {
    fn new(storage: &'a S) -> Self {
        Self {
            storage,
            offset: HEADER_LEN,
            expected_sequence: None,
        }
    }

    /// Seeks to `sequence`, so recovery can start from a snapshot.
    ///
    /// Arithmetic rather than a search. Records are a fixed width, the log
    /// begins at sequence zero, and [`Self::next_record`] refuses a sequence
    /// that jumps -- so a sequence's offset is `HEADER_LEN + sequence *
    /// RECORD_LEN` and nothing has to be read to find it. This used to scan
    /// from the start, which made recovering from a snapshot still cost a walk
    /// of the whole journal and left the snapshot saving only the *applying*.
    ///
    /// The trade is that records before `sequence` are no longer read, so
    /// corruption among them is not reported here. Those records are already
    /// folded into the snapshot being restored and will not be applied again;
    /// [`Journal::open`] is what validates the log as a whole.
    ///
    /// A `sequence` past the end leaves the cursor past the end, and the first
    /// read returns `None` -- the same as scanning off the end did.
    ///
    /// # Errors
    /// Fails if the offset for `sequence` does not fit in a `u64`.
    pub fn from_sequence(mut self, sequence: Sequence) -> Result<Self> {
        self.offset = sequence
            .checked_mul(RECORD_LEN as u64)
            .and_then(|scaled| scaled.checked_add(HEADER_LEN))
            .ok_or(JournalError::CorruptRecord { offset: u64::MAX })?;
        self.expected_sequence = Some(sequence);
        Ok(self)
    }

    /// Walks the whole log, returning the highest sequence it holds and the
    /// offset where the last intact record ends.
    ///
    /// # Errors
    /// Propagates any error encountered while scanning.
    fn scan(mut self) -> Result<(Option<Sequence>, u64)> {
        let mut last = None;
        while let Some(command) = self.next_record()? {
            last = Some(command.sequence);
        }
        Ok((last, self.offset))
    }

    /// # Errors
    /// Fails on I/O error, a malformed complete record, or a sequence gap.
    pub fn next_record(&mut self) -> Result<Option<Command>> {
        let mut buf = [0_u8; RECORD_LEN];
        let read = self.storage.read_at(self.offset, &mut buf)?;
        if read < RECORD_LEN {
            // Either the clean end of the log, or a torn trailing write.
            return Ok(None);
        }

        let command = Command::read_from_bytes(&buf).map_err(|_| JournalError::CorruptRecord {
            offset: self.offset,
        })?;
        if !command.is_well_formed() {
            return Err(JournalError::CorruptRecord {
                offset: self.offset,
            });
        }
        if let Some(expected) = self.expected_sequence
            && command.sequence != expected
        {
            return Err(JournalError::SequenceGap {
                expected,
                found: command.sequence,
            });
        }

        self.offset += RECORD_LEN as u64;
        self.expected_sequence = Some(command.sequence + 1);
        Ok(Some(command))
    }

    /// Collects the remaining records.
    ///
    /// # Errors
    /// Propagates any error encountered while reading.
    pub fn collect_all(mut self) -> Result<Vec<Command>> {
        let mut out = Vec::new();
        while let Some(command) = self.next_record()? {
            out.push(command);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bx_protocol::{CommandKind, Side, TimeInForce};

    fn command(order_id: u64) -> Command {
        Command::new(
            CommandKind::NewOrder,
            1,
            1,
            order_id,
            Side::Bid,
            100,
            5,
            TimeInForce::GoodTillCancel,
        )
    }

    fn journal_with(n: u64) -> Journal<MemoryLog> {
        let mut journal = Journal::open(MemoryLog::new()).unwrap();
        for i in 0..n {
            journal.append(&mut command(i)).unwrap();
        }
        journal.sync().unwrap();
        journal
    }

    #[test]
    fn sequences_are_assigned_contiguously_from_zero() {
        let mut journal = Journal::open(MemoryLog::new()).unwrap();
        assert_eq!(journal.next_sequence(), 0);
        for expected in 0..5 {
            assert_eq!(journal.append(&mut command(expected)).unwrap(), expected);
        }
        assert_eq!(journal.next_sequence(), 5);
    }

    #[test]
    fn replay_returns_exactly_what_was_appended_in_order() {
        let journal = journal_with(100);
        let replayed = journal.replay().collect_all().unwrap();
        assert_eq!(replayed.len(), 100);
        for (i, command) in replayed.iter().enumerate() {
            assert_eq!(command.sequence, i as u64);
            assert_eq!(command.order_id, i as u64);
        }
    }

    #[test]
    fn an_arrival_time_survives_the_journal() {
        // The whole reason the stamp goes on the command rather than on the
        // event: a replayed venue reproduces when each order arrived instead of
        // re-reading a clock, which would make recovery a different run.
        let mut journal = Journal::open(MemoryLog::new()).unwrap();
        for id in 0..8_u64 {
            let mut record = command(id);
            record.ingress_ns = 1_700_000_000_000_000_000 + id;
            journal.append(&mut record).unwrap();
        }
        let replayed = journal.replay().collect_all().unwrap();
        for (id, record) in replayed.iter().enumerate() {
            assert_eq!(
                record.ingress_ns,
                1_700_000_000_000_000_000 + id as u64,
                "the arrival time did not survive replay"
            );
        }
    }

    #[test]
    fn replay_can_start_from_a_snapshot_sequence() {
        let journal = journal_with(100);
        let tail = journal
            .replay()
            .from_sequence(60)
            .unwrap()
            .collect_all()
            .unwrap();
        assert_eq!(tail.len(), 40);
        assert_eq!(tail[0].sequence, 60);
        assert_eq!(tail.last().unwrap().sequence, 99);
    }

    /// Every seek target, not just one.
    ///
    /// `from_sequence` computes an offset instead of scanning for it, so the
    /// failure it can have is landing on the wrong record -- and landing one
    /// record out is invisible to a test that only checks how many records come
    /// back. `order_id` carries the sequence independently of the sequence
    /// field, so this catches a cursor that is off by any amount, at any target,
    /// rather than only in the middle of the log.
    #[test]
    fn seeking_to_any_sequence_lands_on_exactly_that_record() {
        const RECORDS: u64 = 64;
        let journal = journal_with(RECORDS);

        for target in 0..RECORDS {
            let tail = journal
                .replay()
                .from_sequence(target)
                .unwrap()
                .collect_all()
                .unwrap();
            assert_eq!(
                tail.len() as u64,
                RECORDS - target,
                "wrong number of records after seeking to {target}"
            );
            assert_eq!(tail[0].sequence, target, "seek to {target} landed wrong");
            assert_eq!(
                tail[0].order_id, target,
                "seek to {target} landed on another record's payload"
            );
        }
    }

    #[test]
    fn seeking_past_the_end_reads_nothing() {
        let journal = journal_with(8);
        for target in [8, 9, 1_000, u32::MAX as u64] {
            let tail = journal
                .replay()
                .from_sequence(target)
                .unwrap()
                .collect_all()
                .unwrap();
            assert!(tail.is_empty(), "seeking to {target} should read nothing");
        }
    }

    /// A sequence whose byte offset would not fit in a `u64` is refused rather
    /// than wrapping into the middle of the log.
    #[test]
    fn a_sequence_that_cannot_be_addressed_is_refused() {
        let journal = journal_with(1);
        assert!(journal.replay().from_sequence(u64::MAX).is_err());
    }

    #[test]
    fn reopening_resumes_after_the_last_record() {
        let journal = journal_with(7);
        let reopened = Journal::open(journal.into_storage()).unwrap();
        assert_eq!(reopened.next_sequence(), 7);
    }

    #[test]
    fn a_torn_trailing_write_reads_as_the_end_of_the_log() {
        let mut journal = Journal::open(MemoryLog::new().tearing_append(4)).unwrap();
        for i in 0..4 {
            journal.append(&mut command(i)).unwrap();
        }
        let replayed = journal.replay().collect_all().unwrap();
        assert_eq!(replayed.len(), 3, "the torn record must not be replayed");

        let reopened = Journal::open(journal.into_storage()).unwrap();
        assert_eq!(reopened.next_sequence(), 3);
    }

    #[test]
    fn records_written_after_a_torn_write_are_still_reachable() {
        // A crash leaves half a record. On restart the venue keeps trading, so
        // it appends more. If the torn bytes are still there, every record
        // after them is unreachable: replay stops at the tear and the new
        // commands vanish silently, which is the worst possible failure.
        let mut journal = Journal::open(MemoryLog::new().tearing_append(3)).unwrap();
        for i in 0..3 {
            journal.append(&mut command(i)).unwrap();
        }
        let mut reopened = Journal::open(journal.into_storage()).unwrap();
        assert_eq!(
            reopened.next_sequence(),
            2,
            "the torn record is not durable"
        );

        for i in 2..5 {
            reopened.append(&mut command(i)).unwrap();
        }
        reopened.sync().unwrap();

        let replayed = reopened.replay().collect_all().unwrap();
        assert_eq!(
            replayed.len(),
            5,
            "records appended after a torn write must be reachable"
        );
    }

    #[test]
    fn crashing_before_sync_discards_only_the_unsynced_tail() {
        let mut journal = journal_with(10);
        for i in 10..15 {
            journal.append(&mut command(i)).unwrap();
        }
        // No sync, then power loss.
        let mut storage = journal.into_storage();
        storage.crash();

        let recovered = Journal::open(storage).unwrap();
        assert_eq!(recovered.next_sequence(), 10);
        assert_eq!(recovered.replay().collect_all().unwrap().len(), 10);
    }

    #[test]
    fn a_corrupt_record_is_an_error_not_a_silent_skip() {
        let mut journal = Journal::open(MemoryLog::new()).unwrap();
        journal.append(&mut command(0)).unwrap();
        journal.sync().unwrap();

        // Scribble over the command-kind byte with a value this version does not
        // define. Seven 8-byte fields, then symbol (u32), puts `kind` at 60.
        const KIND_OFFSET: usize = 60;
        let mut storage = journal.into_storage();
        storage.overwrite(HEADER_LEN as usize + KIND_OFFSET, &[250]);

        match Journal::open(storage) {
            Err(JournalError::CorruptRecord { offset }) => assert_eq!(offset, HEADER_LEN),
            other => panic!("expected CorruptRecord, got {other:?}"),
        }
    }

    #[test]
    fn a_sequence_gap_is_detected() {
        let mut journal = Journal::open(MemoryLog::new()).unwrap();
        journal.append(&mut command(0)).unwrap();
        journal.append(&mut command(1)).unwrap();
        journal.sync().unwrap();

        // Rewrite the second record's sequence to 5, as a lost record would look.
        let mut storage = journal.into_storage();
        storage.overwrite(HEADER_LEN as usize + RECORD_LEN, &5_u64.to_le_bytes());

        match Journal::open(storage) {
            Err(JournalError::SequenceGap { expected, found }) => {
                assert_eq!((expected, found), (1, 5));
            }
            other => panic!("expected SequenceGap, got {other:?}"),
        }
    }

    #[test]
    fn a_device_failure_surfaces_rather_than_being_swallowed() {
        let mut journal = Journal::open(MemoryLog::new().failing_after(3)).unwrap();
        for i in 0..3 {
            journal.append(&mut command(i)).unwrap();
        }
        assert!(matches!(
            journal.append(&mut command(3)),
            Err(JournalError::Io(_))
        ));
    }
}

#[cfg(test)]
mod file_tests {
    use super::*;
    use bx_protocol::{CommandKind, Side, TimeInForce};
    use std::env;
    use std::fs;

    /// A scratch path that cleans itself up.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let path = env::temp_dir().join(format!("bxjrnl-{name}-{}.log", std::process::id()));
            let _ = fs::remove_file(&path);
            Self(path)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn command(order_id: u64) -> Command {
        Command::new(
            CommandKind::NewOrder,
            1,
            1,
            order_id,
            Side::Bid,
            100,
            5,
            TimeInForce::GoodTillCancel,
        )
    }

    #[test]
    fn a_real_file_round_trips_and_survives_reopening() {
        let scratch = Scratch::new("roundtrip");
        {
            let mut journal = Journal::open(FileLog::open(&scratch.0).unwrap()).unwrap();
            for i in 0..500 {
                journal.append(&mut command(i)).unwrap();
            }
            journal.sync().unwrap();
        }
        // Reopen from disk, as a restart would.
        let journal = Journal::open(FileLog::open(&scratch.0).unwrap()).unwrap();
        assert_eq!(journal.next_sequence(), 500);
        let replayed = journal.replay().collect_all().unwrap();
        assert_eq!(replayed.len(), 500);
        assert_eq!(replayed[499].order_id, 499);
    }

    #[test]
    fn appending_continues_across_a_reopen() {
        let scratch = Scratch::new("append");
        {
            let mut journal = Journal::open(FileLog::open(&scratch.0).unwrap()).unwrap();
            for i in 0..10 {
                journal.append(&mut command(i)).unwrap();
            }
            journal.sync().unwrap();
        }
        {
            let mut journal = Journal::open(FileLog::open(&scratch.0).unwrap()).unwrap();
            for i in 10..20 {
                journal.append(&mut command(i)).unwrap();
            }
            journal.sync().unwrap();
        }
        let journal = Journal::open(FileLog::open(&scratch.0).unwrap()).unwrap();
        let replayed = journal.replay().collect_all().unwrap();
        assert_eq!(replayed.len(), 20, "records were lost across a reopen");
        for (i, command) in replayed.iter().enumerate() {
            assert_eq!(command.sequence, i as u64);
        }
    }

    #[test]
    fn a_torn_record_on_disk_is_truncated_rather_than_poisoning_the_log() {
        let scratch = Scratch::new("torn");
        {
            let mut journal = Journal::open(FileLog::open(&scratch.0).unwrap()).unwrap();
            for i in 0..5 {
                journal.append(&mut command(i)).unwrap();
            }
            journal.sync().unwrap();
        }
        // Simulate a crash mid-write: append half a record's worth of bytes.
        {
            use std::io::Write;
            let mut file = OpenOptions::new().append(true).open(&scratch.0).unwrap();
            file.write_all(&[0_u8; RECORD_LEN / 2]).unwrap();
            file.sync_all().unwrap();
        }
        // Restart: the tear must be dropped, and new records must be reachable.
        let mut journal = Journal::open(FileLog::open(&scratch.0).unwrap()).unwrap();
        assert_eq!(journal.next_sequence(), 5);
        for i in 5..8 {
            journal.append(&mut command(i)).unwrap();
        }
        journal.sync().unwrap();

        let journal = Journal::open(FileLog::open(&scratch.0).unwrap()).unwrap();
        let replayed = journal.replay().collect_all().unwrap();
        assert_eq!(replayed.len(), 8, "records after the tear were lost");
    }

    #[test]
    fn replaying_then_appending_does_not_overwrite_the_log() {
        // Replay reads at absolute offsets. If reading moves the file cursor,
        // the next append lands in the middle of the log and silently destroys
        // records that were already durable.
        let scratch = Scratch::new("replay-then-append");
        let mut journal = Journal::open(FileLog::open(&scratch.0).unwrap()).unwrap();
        for i in 0..10 {
            journal.append(&mut command(i)).unwrap();
        }
        journal.sync().unwrap();

        // Replay, as recovery would, then keep trading on the same instance.
        assert_eq!(journal.replay().collect_all().unwrap().len(), 10);
        for i in 10..20 {
            journal.append(&mut command(i)).unwrap();
        }
        journal.sync().unwrap();

        let replayed = journal.replay().collect_all().unwrap();
        assert_eq!(
            replayed.len(),
            20,
            "appending after a replay overwrote existing records"
        );
        for (i, command) in replayed.iter().enumerate() {
            assert_eq!(command.sequence, i as u64, "record {i} is wrong");
        }
    }

    #[test]
    fn a_file_that_is_not_a_journal_is_refused() {
        let scratch = Scratch::new("foreign");
        fs::write(&scratch.0, b"this is not a journal at all").unwrap();
        assert!(matches!(
            FileLog::open(&scratch.0),
            Err(JournalError::NotAJournal)
        ));
    }
}
