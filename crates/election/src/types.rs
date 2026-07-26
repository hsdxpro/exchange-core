//! What the leadership cluster agrees about.
//!
//! One fact, and deliberately only one: which node is the leader. The exchange's
//! commands never come near this log — see the crate documentation for why that
//! separation is the whole point.

use serde::{Deserialize, Serialize};
use std::io::Cursor;

/// A node in the leadership cluster. Small, stable, and assigned by the
/// deployment rather than discovered.
pub type NodeId = u64;

/// The state machine's entire contents.
///
/// Raft needs a state machine; this one holds the smallest thing that can
/// usefully be agreed on. Everything a venue actually does is decided by the
/// command log, which openraft never sees.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Held {
    /// The node that most recently announced itself leader.
    pub leader: NodeId,
}

/// The only thing ever written to the leadership log.
///
/// A new leader appends one of these so the fact is agreed rather than merely
/// believed. It is not what makes it the leader — Raft's own election does that
/// — but it is what lets any node read the answer out of the state machine.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Announce {
    pub leader: NodeId,
}

openraft::declare_raft_types!(
    /// The leadership cluster's types. `SnapshotData` is a plain byte cursor
    /// because the whole state machine is one integer: a snapshot of it is a few
    /// bytes, and a streaming format would be scaffolding around nothing.
    pub Leadership:
        D = Announce,
        R = Held,
        NodeId = NodeId,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<Leadership>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
