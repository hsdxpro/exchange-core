//! Deciding which node is the leader, so a promotion needs no person.
//!
//! Everything else about failover was already here and already safe. A group is
//! acknowledged once a majority of followers holds it. A promoted node catches
//! up to the longest log a majority holds *before* it serves anybody, and the
//! two majorities intersect, so nothing a client was told about can be missing.
//! A follower refuses anything from a term older than the highest it has seen,
//! so a leader that has been replaced cannot keep writing. What was missing was
//! the part that *notices* a dead leader and *chooses* the replacement — which
//! meant a person editing a term into a file and starting a process.
//!
//! That part is consensus and it is not written here. It is the one place in
//! this system where being subtly wrong loses money quietly instead of loudly:
//! the failure is two nodes each believing they lead, both acknowledging orders,
//! into logs that will never agree again. `openraft` does it.
//!
//! ## Consensus is kept off the command log
//!
//! openraft runs a **separate leadership log** whose state machine holds one
//! fact: which node leads. The exchange's commands never touch it.
//!
//! That split is the reason this is affordable. An openraft entry is
//! variable-length and heterogeneous; routing a venue's command log through it
//! would cost the fixed 64-byte record, and with it the zero-copy replay and the
//! O(1) seek to a sequence that make a restart measurable in milliseconds. It
//! would also put an async runtime on the path every order takes. Instead
//! openraft answers one question — *am I the leader, and under what term* — and
//! the term feeds the fencing that already exists.
//!
//! This is the CORFU and Aeron Cluster shape: consensus for leadership, plain
//! leader-to-follower replication on the data path. An election happens once per
//! failure and may take milliseconds; replication happens constantly and must
//! not.
//!
//! ## What crosses the boundary
//!
//! Two atomics. The Raft node runs on its own runtime, and the venue's loop
//! reads [`Leadership::is_leader`] and [`Leadership::term`] — two loads, no
//! locks, no async, nothing that can block a commit.
//!
//! The term is what ties the halves together. Raft guarantees at most one leader
//! per term, so a term is a fencing token by construction: monotonic across
//! elections, and never shared by two leaders. That is exactly what
//! `ReplicatedLog` already refuses on.

pub mod network;
pub mod store;
pub mod types;

use openraft::{BasicNode, Config, Raft};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use store::Store;
use types::Leadership as Types;

pub use types::NodeId;

/// How long a follower waits without hearing from the leader before standing for
/// election, and how often a leader says it is still there.
///
/// The gap between them is what stops a healthy leader being deposed by a busy
/// moment: a heartbeat every 250 ms against a 1–2 second patience means four to
/// eight have to go missing before anybody stands. Failover is therefore about a
/// second, which is the honest cost of *not* needing a person — a human notices
/// a dead venue in minutes at best.
const HEARTBEAT_MS: u64 = 250;
const ELECTION_MIN_MS: u64 = 1_000;
const ELECTION_MAX_MS: u64 = 2_000;

/// How long a node may take to leave before it is abandoned.
///
/// Generous for something that normally takes milliseconds, and bounded because
/// the alternative -- the unbounded wait a bare `Runtime` drop performs -- means
/// one stuck task hangs a venue's shutdown.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// What the venue reads. Written by the Raft runtime, read by the venue's loop.
#[derive(Debug, Default)]
struct Shared {
    term: AtomicU64,
    leading: AtomicBool,
}

/// One node's view of who leads.
///
/// Dropping this stops the Raft node, so a venue holds it for as long as it
/// intends to take part.
pub struct Leadership {
    id: NodeId,
    shared: Arc<Shared>,
    /// Kept alive so the runtime and its tasks outlive this handle.
    ///
    /// In an `Option` so [`Drop`] can take it out and shut it down on a bound.
    /// Dropping a `Runtime` where it sits waits for every task to wind up, with
    /// no limit -- so a node that would not stop took the venue's shutdown with
    /// it. Not hypothetical: a test that killed a leader and then waited for a
    /// replacement spent its whole timeout inside the drop and reported the
    /// election as broken.
    runtime: Option<tokio::runtime::Runtime>,
    raft: Raft<Types>,
}

/// Written by hand because `Raft` has no `Debug`, and printing what it holds
/// would be a page of internal state nobody debugging a venue wants. What
/// matters here is the answer, not the machinery that reached it.
impl std::fmt::Debug for Leadership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Leadership")
            .field("id", &self.id)
            .field("leading", &self.is_leader())
            .field("term", &self.term())
            .finish_non_exhaustive()
    }
}

impl Leadership {
    /// Joins the leadership cluster and starts taking part.
    ///
    /// `peers` is every node including this one, so each node is configured with
    /// the same list and the membership is not discovered — a cluster that
    /// disagrees about who is in it disagrees about what a majority is.
    ///
    /// # Errors
    /// Fails if the leadership state cannot be opened, the address cannot be
    /// bound, or Raft cannot start.
    ///
    /// # Panics
    /// If the peer list does not contain this node, which is a configuration
    /// error rather than a condition: a node that is not a member cannot lead
    /// and would wait forever.
    pub fn join(
        id: NodeId,
        peers: &[(NodeId, String)],
        state: &Path,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let listen = peers
            .iter()
            .find(|(peer, _)| *peer == id)
            .map(|(_, address)| address.clone())
            .unwrap_or_else(|| panic!("node {id} is not in its own peer list"));

        let runtime = tokio::runtime::Builder::new_multi_thread()
            // Two: one for the Raft node's own work, one so an RPC being served
            // cannot stop it. Consensus here is a heartbeat twice a second, not
            // a workload.
            .worker_threads(2)
            .enable_all()
            .build()?;

        let store = Store::open(state)?;
        let network = network::Network::new(peers.to_vec());
        let config = Arc::new(
            Config {
                heartbeat_interval: HEARTBEAT_MS,
                election_timeout_min: ELECTION_MIN_MS,
                election_timeout_max: ELECTION_MAX_MS,
                ..Config::default()
            }
            .validate()?,
        );

        let members: BTreeMap<NodeId, BasicNode> = peers
            .iter()
            .map(|(peer, address)| (*peer, BasicNode::new(address)))
            .collect();

        let raft = runtime.block_on(async {
            let raft = Raft::new(id, config, network, store.clone(), store).await?;
            let listener = tokio::net::TcpListener::bind(&listen).await?;
            tokio::spawn(network::serve(listener, raft.clone()));
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(raft)
        })?;

        // Exactly one node bootstraps, and which one is decided by the
        // configuration rather than by whoever starts first. Every node calling
        // `initialize` would have each of them write its own first entry, and a
        // cluster whose logs disagree at index one has diverged before it has
        // done anything.
        if peers.iter().map(|(peer, _)| *peer).min() == Some(id) {
            let raft = raft.clone();
            runtime.spawn(async move {
                // Fails once the cluster exists, which is the ordinary case on
                // every restart after the first.
                let _ = raft.initialize(members).await;
            });
        }

        let shared = Arc::new(Shared::default());
        let watching = Arc::clone(&shared);
        let mut metrics = raft.metrics();
        runtime.spawn(async move {
            // Woken by Raft rather than polled, so a leadership change is
            // noticed as it happens.
            while metrics.changed().await.is_ok() {
                let (term, leading) = {
                    let held = metrics.borrow_and_update();
                    (held.current_term, held.current_leader == Some(id))
                };
                watching.term.store(term, Ordering::Relaxed);
                // Released last, so a venue that sees itself leading is
                // guaranteed to read the term it leads under and not the
                // previous one.
                watching.leading.store(leading, Ordering::Release);
            }
        });

        Ok(Self {
            id,
            shared,
            runtime: Some(runtime),
            raft,
        })
    }

    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// True while this node is the established leader.
    ///
    /// One load. Cheap enough to ask every pass, which is what the venue does: a
    /// node that has been deposed must stop serving before it takes another
    /// order.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.shared.leading.load(Ordering::Acquire)
    }

    /// The term this node leads under, or the highest it has seen.
    ///
    /// The fencing token. Raft allows at most one leader per term, so two
    /// leaders can never present the same one, and a replaced leader's writes
    /// are refused by followers that have seen a higher one.
    #[must_use]
    pub fn term(&self) -> u64 {
        self.shared.term.load(Ordering::Relaxed)
    }

    /// Waits until this node leads, and returns the term it leads under.
    ///
    /// `None` if `within` passes first, so a caller can report a cluster that
    /// never settled rather than hang.
    #[must_use]
    pub fn await_leadership(&self, within: Duration) -> Option<u64> {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if self.is_leader() {
                return Some(self.term());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        None
    }

    /// Records in the leadership log that this node leads.
    ///
    /// Not what makes it the leader — Raft's election already did that — but it
    /// puts the fact in the agreed state machine, so "who leads" is a question
    /// any node can answer rather than one each node guesses at. It also proves
    /// the node can still reach a majority *before* it starts taking orders.
    ///
    /// # Errors
    /// Fails if the write does not reach a majority, which means this node is no
    /// longer the leader and must not serve.
    pub fn announce(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let raft = self.raft.clone();
        let id = self.id;
        self.runtime()
            .block_on(async move { raft.client_write(types::Announce { leader: id }).await })
            .map(|_| ())
            .map_err(Into::into)
    }

    /// The runtime, which exists for as long as this handle does.
    fn runtime(&self) -> &tokio::runtime::Runtime {
        self.runtime
            .as_ref()
            .expect("the runtime is taken only by Drop, after which nothing runs")
    }
}

/// Leaves the cluster on the way out, on a bound.
///
/// Two steps, and the order matters. Raft is asked to stop first, so the node
/// stops answering heartbeats and standing for election -- which is what lets the
/// remaining nodes elect a replacement promptly rather than waiting out an
/// election timeout against a peer that is still nominally there. Then the
/// runtime is given a deadline to wind up, so a task that will not finish costs
/// seconds instead of the process.
impl Drop for Leadership {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        let raft = self.raft.clone();
        // Bounded: `shutdown` waits for the node's own task, and a node wedged
        // for any reason must not be able to hold this.
        let _ = runtime
            .block_on(async move { tokio::time::timeout(SHUTDOWN_GRACE, raft.shutdown()).await });
        runtime.shutdown_timeout(SHUTDOWN_GRACE);
    }
}
