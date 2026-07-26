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

pub use replication::{Replica, ReplicatedLog};

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

    /// # Errors
    /// Returns the underlying I/O error if the read fails.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;

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
        if !self.pending.is_empty() {
            self.file.write_all(&self.pending)?;
            self.pending.clear();
        }
        self.file.sync_data()
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
        self.synced_len = self.bytes.len() as u64;
        Ok(())
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

/// Appends sequenced commands and replays them.
#[derive(Debug)]
pub struct Journal<S: LogStorage> {
    storage: S,
    next_sequence: Sequence,
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
        self.storage.append(command.as_bytes())?;
        self.next_sequence += 1;
        Ok(command.sequence)
    }

    /// # Errors
    /// Returns the underlying I/O error if the flush fails.
    pub fn sync(&mut self) -> Result<()> {
        self.storage.sync()?;
        Ok(())
    }

    #[must_use]
    pub fn replay(&self) -> Replay<'_, S> {
        Replay::new(&self.storage)
    }

    #[must_use]
    pub fn storage(&self) -> &S {
        &self.storage
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

    /// Skips forward to `sequence`, so recovery can start from a snapshot.
    ///
    /// # Errors
    /// Propagates any error encountered while scanning.
    pub fn from_sequence(mut self, sequence: Sequence) -> Result<Self> {
        while let Some(command) = self.next_record()? {
            if command.sequence >= sequence {
                self.offset -= RECORD_LEN as u64;
                self.expected_sequence = Some(command.sequence);
                break;
            }
        }
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
