//! Where the leadership log and its state machine live.
//!
//! This log is tiny **by construction**, and that fact is what the whole file
//! rests on. Raft appends an entry when a term begins and when membership
//! changes; heartbeats are not entries, and no venue traffic is ever written
//! here. A cluster that failed over once an hour for a year would hold nine
//! thousand entries of about a hundred bytes.
//!
//! So the log is  in memory and persisted by rewriting one small file,
//! rather than by an append format with its own torn-write recovery. The
//! command log needs all of that and has it; copying it here would be several
//! hundred lines defending against a file that never grows.
//!
//! What is *not* relaxed is durability of the vote. Raft's safety rests on a
//! node never voting twice in one term, so a vote that reached memory and not
//! the platter would let a restarted node vote again — and two leaders in one
//! term is exactly the state the whole design exists to prevent. Every state
//! change is written and `fsync`ed before it is acknowledged.

use crate::types::{Announce, Held, Leadership, NodeId};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    AnyError, BasicNode, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend,
    RaftLogReader, RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError,
    StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Everything that has to survive a restart, in one file.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct Persisted {
    vote: Option<Vote<NodeId>>,
    log: BTreeMap<u64, Entry<Leadership>>,
    last_purged: Option<LogId<NodeId>>,
    applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, BasicNode>,
    held: Held,
}

#[derive(Debug)]
struct Inner {
    path: PathBuf,
    state: Persisted,
    /// Rebuilt on demand rather than stored: the state machine is one integer,
    /// so a snapshot of it costs nothing to make and storing it would be a
    /// second copy to keep in step.
    snapshot: Option<(SnapshotMeta<NodeId, BasicNode>, Vec<u8>)>,
    snapshot_index: u64,
}

/// The leadership log and its state machine, shared by the two traits openraft
/// wants them behind.
#[derive(Clone, Debug)]
pub struct Store {
    inner: Arc<Mutex<Inner>>,
}

fn io_error<E: std::error::Error + 'static>(
    subject: ErrorSubject<NodeId>,
    verb: ErrorVerb,
    error: &E,
) -> StorageError<NodeId> {
    StorageError::IO {
        source: StorageIOError::new(subject, verb, AnyError::new(error)),
    }
}

impl Store {
    /// Opens the store at `path`, restoring whatever is there.
    ///
    /// # Errors
    /// Fails if the file exists and cannot be read or parsed. A leadership state
    /// that will not load is not skipped: starting fresh would mean forgetting a
    /// vote, and forgetting a vote is how one term gets two leaders.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let state = if path.exists() {
            let text = std::fs::read_to_string(path)?;
            serde_json::from_str(&text).map_err(|e| {
                std::io::Error::other(format!(
                    "leadership state at {} will not parse, and starting fresh would \
                     forget a vote: {e}",
                    path.display()
                ))
            })?
        } else {
            Persisted::default()
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                path: path.to_path_buf(),
                state,
                snapshot: None,
                snapshot_index: 0,
            })),
        })
    }
}

impl Inner {
    /// Writes the whole state and waits for the platter.
    ///
    /// Through a temporary file and a rename, so a crash midway leaves the
    /// previous state rather than half of this one — the same rule the venue's
    /// snapshots follow, and for the same reason: a leadership state that cannot
    /// be trusted is worse than one that is merely old.
    fn persist(&self) -> std::io::Result<()> {
        use std::io::Write;
        let staging = self.path.with_extension("writing");
        {
            let mut file = std::fs::File::create(&staging)?;
            file.write_all(serde_json::to_string(&self.state)?.as_bytes())?;
            file.sync_all()?;
        }
        std::fs::rename(&staging, &self.path)
    }
}

impl RaftLogReader<Leadership> for Store {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<Leadership>>, StorageError<NodeId>> {
        let held = self.inner.lock().expect("leadership state lock");
        Ok(held
            .state
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftSnapshotBuilder<Leadership> for Store {
    async fn build_snapshot(&mut self) -> Result<Snapshot<Leadership>, StorageError<NodeId>> {
        let mut held = self.inner.lock().expect("leadership state lock");
        let data = serde_json::to_vec(&held.state.held)
            .map_err(|e| io_error(ErrorSubject::StateMachine, ErrorVerb::Read, &e))?;
        held.snapshot_index += 1;
        let meta = SnapshotMeta {
            last_log_id: held.state.applied,
            last_membership: held.state.membership.clone(),
            snapshot_id: format!(
                "{}-{}",
                held.state.applied.map_or(0, |id| id.index),
                held.snapshot_index
            ),
        };
        held.snapshot = Some((meta.clone(), data.clone()));
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftLogStorage<Leadership> for Store {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<Leadership>, StorageError<NodeId>> {
        let held = self.inner.lock().expect("leadership state lock");
        let last = held
            .state
            .log
            .values()
            .next_back()
            .map(|entry| entry.log_id)
            .or(held.state.last_purged);
        Ok(LogState {
            last_purged_log_id: held.state.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut held = self.inner.lock().expect("leadership state lock");
        held.state.vote = Some(*vote);
        // Durable before this returns. A vote that reached memory and not the
        // platter lets a restarted node vote twice in one term, and two leaders
        // in one term is the failure everything here exists to prevent.
        held.persist()
            .map_err(|e| io_error(ErrorSubject::Vote, ErrorVerb::Write, &e))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().expect("leadership state lock").state.vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<Leadership>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<Leadership>> + OptionalSend,
    {
        let mut held = self.inner.lock().expect("leadership state lock");
        for entry in entries {
            held.state.log.insert(entry.log_id.index, entry);
        }
        held.persist()
            .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Write, &e))?;
        // Only once it is on the platter. openraft treats this as "durable", and
        // acknowledging early would make the log a suggestion.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut held = self.inner.lock().expect("leadership state lock");
        held.state.log.split_off(&log_id.index);
        held.persist()
            .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Delete, &e))
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut held = self.inner.lock().expect("leadership state lock");
        held.state.last_purged = Some(log_id);
        held.state.log = held.state.log.split_off(&(log_id.index + 1));
        held.persist()
            .map_err(|e| io_error(ErrorSubject::Logs, ErrorVerb::Delete, &e))
    }
}

impl RaftStateMachine<Leadership> for Store {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let held = self.inner.lock().expect("leadership state lock");
        Ok((held.state.applied, held.state.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Held>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<Leadership>> + OptionalSend,
    {
        let mut held = self.inner.lock().expect("leadership state lock");
        let mut answers = Vec::new();
        for entry in entries {
            held.state.applied = Some(entry.log_id);
            match entry.payload {
                // Raft writes one of these when a term begins. It carries no
                // decision, only the term itself.
                EntryPayload::Blank => {}
                EntryPayload::Normal(Announce { leader }) => held.state.held.leader = leader,
                EntryPayload::Membership(membership) => {
                    held.state.membership = StoredMembership::new(Some(entry.log_id), membership);
                }
            }
            answers.push(held.state.held.clone());
        }
        held.persist()
            .map_err(|e| io_error(ErrorSubject::StateMachine, ErrorVerb::Write, &e))?;
        Ok(answers)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let restored: Held = serde_json::from_slice(&data)
            .map_err(|e| io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, &e))?;
        let mut held = self.inner.lock().expect("leadership state lock");
        held.state.held = restored;
        held.state.applied = meta.last_log_id;
        held.state.membership = meta.last_membership.clone();
        held.snapshot = Some((meta.clone(), data));
        held.persist()
            .map_err(|e| io_error(ErrorSubject::StateMachine, ErrorVerb::Write, &e))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<Leadership>>, StorageError<NodeId>> {
        let held = self.inner.lock().expect("leadership state lock");
        Ok(held.snapshot.as_ref().map(|(meta, data)| Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(data.clone())),
        }))
    }
}
