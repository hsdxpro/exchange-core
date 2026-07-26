//! Durability by quorum instead of by platter.
//!
//! A local `fsync` measured about three milliseconds on the machine this was
//! developed on. A round trip to another machine on the same network is tens of
//! microseconds. So the cheapest way to make a record survive the loss of one
//! machine is not to push it harder onto one disk — it is to put it in the
//! memory of several machines and stop waiting for the disk. That is why real
//! venues acknowledge after a quorum rather than after a flush, and the measured
//! gap is two orders of magnitude.
//!
//! This is a [`LogStorage`] wrapper, not a new layer. `Exchange` already talks
//! to storage through that trait and calls `sync` when a group must be durable,
//! so replacing "durable" with "a quorum has it" changes nothing above:
//! `Exchange<ReplicatedLog<FileLog>>` needs no pipeline changes at all.
//!
//! What this deliberately does **not** do is elect a leader. Failover requires
//! consensus, hand-rolled consensus is how distributed systems lose data
//! quietly, and the right answer is a reviewed implementation such as
//! `openraft` rather than a few hundred lines written here. What this does give
//! is the durability and throughput half — the part the measurement showed
//! matters — with an explicit boundary where the election belongs.

use crate::{LogStorage, RECORD_LEN};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};

/// A follower's reply: how many bytes it now holds.
const ACK_LEN: usize = size_of::<u64>();

/// One follower, as the leader sees it.
#[derive(Debug)]
struct Follower {
    stream: TcpStream,
    /// A follower that has failed is left in place and not spoken to again, so
    /// the quorum arithmetic keeps counting against the configured cluster size
    /// rather than silently shrinking to whoever is still answering.
    live: bool,
}

/// A local log whose records are also sent to followers.
///
/// `sync` returns once a quorum holds the group. The local log is appended to
/// but **not** flushed, which is the point: the acknowledgement is the quorum.
#[derive(Debug)]
pub struct ReplicatedLog<L: LogStorage> {
    local: L,
    followers: Vec<Follower>,
    /// Followers that must confirm, not counting the leader.
    quorum: usize,
    /// Bytes appended since the last sync, awaiting replication.
    pending: Vec<u8>,
}

impl<L: LogStorage> ReplicatedLog<L> {
    /// Connects to every follower and prepares the quorum.
    ///
    /// The quorum is a majority of the cluster including the leader, so two
    /// followers means both plus the leader is three and one confirmation is
    /// enough to survive losing any single machine.
    ///
    /// # Errors
    /// Fails if a follower cannot be reached.
    pub fn connect(local: L, addresses: &[String]) -> io::Result<Self> {
        let mut followers = Vec::with_capacity(addresses.len());
        for address in addresses {
            let stream = TcpStream::connect(address)?;
            stream.set_nodelay(true)?;
            followers.push(Follower { stream, live: true });
        }
        // Majority of (followers + leader), minus the leader's own vote.
        let cluster = followers.len() + 1;
        let quorum = cluster / 2 + 1 - 1;
        Ok(Self {
            local,
            followers,
            quorum,
            pending: Vec::new(),
        })
    }

    /// Followers that must confirm before a group is acknowledged.
    #[must_use]
    pub const fn quorum(&self) -> usize {
        self.quorum
    }

    /// Followers still answering.
    #[must_use]
    pub fn live_followers(&self) -> usize {
        self.followers.iter().filter(|f| f.live).count()
    }

    /// Sends the pending bytes to every live follower and waits for a quorum.
    fn replicate(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let length = u32::try_from(self.pending.len())
            .map_err(|_| io::Error::other("replication group too large"))?;
        let header = length.to_le_bytes();

        for follower in self.followers.iter_mut().filter(|f| f.live) {
            if follower.stream.write_all(&header).is_err()
                || follower.stream.write_all(&self.pending).is_err()
            {
                follower.live = false;
            }
        }

        let mut confirmed = 0;
        let mut ack = [0_u8; ACK_LEN];
        for follower in self.followers.iter_mut().filter(|f| f.live) {
            if follower.stream.read_exact(&mut ack).is_ok() {
                confirmed += 1;
            } else {
                follower.live = false;
            }
        }

        if confirmed < self.quorum {
            // Refusing is the only safe answer. Reporting success here would
            // acknowledge a command to a client that one machine's failure
            // could still erase.
            return Err(io::Error::other(format!(
                "replication reached {confirmed} of {} followers",
                self.quorum
            )));
        }
        self.pending.clear();
        Ok(())
    }
}

impl<L: LogStorage> LogStorage for ReplicatedLog<L> {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.local.append(bytes)?;
        self.pending.extend_from_slice(bytes);
        Ok(())
    }

    /// Durable means "a quorum holds it". The local disk is not flushed,
    /// because waiting on it would give up the whole advantage.
    fn sync(&mut self) -> io::Result<()> {
        self.replicate()
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.local.read_at(offset, buf)
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.local.truncate(len)
    }
}

/// The follower side: accept a leader, append what it sends, confirm.
#[derive(Debug)]
pub struct Replica<L: LogStorage> {
    log: L,
    /// Set when the follower should also flush before confirming. Off by
    /// default: a quorum in memory on separate machines is what the leader is
    /// buying, and making each follower wait on its own disk hands back exactly
    /// the cost the design set out to avoid. Turn it on when the replicas share
    /// a power domain and the quorum is therefore not independent.
    flush_before_ack: bool,
    held: u64,
}

impl<L: LogStorage> Replica<L> {
    #[must_use]
    pub const fn new(log: L, flush_before_ack: bool) -> Self {
        Self {
            log,
            flush_before_ack,
            held: 0,
        }
    }

    /// Bytes accepted from the leader.
    #[must_use]
    pub const fn held(&self) -> u64 {
        self.held
    }

    pub fn into_log(self) -> L {
        self.log
    }

    /// Serves one leader until it disconnects.
    ///
    /// # Errors
    /// Fails on an I/O error that is not the leader simply going away.
    pub fn serve(&mut self, stream: &mut TcpStream) -> io::Result<()> {
        stream.set_nodelay(true)?;
        let mut header = [0_u8; size_of::<u32>()];
        let mut group = Vec::new();

        loop {
            match stream.read_exact(&mut header) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e),
            }
            let length = u32::from_le_bytes(header) as usize;
            if length == 0 || !length.is_multiple_of(RECORD_LEN) {
                return Err(io::Error::other(
                    "leader sent a group that is not whole records",
                ));
            }
            group.resize(length, 0);
            stream.read_exact(&mut group)?;

            self.log.append(&group)?;
            if self.flush_before_ack {
                self.log.sync()?;
            }
            self.held += length as u64;
            stream.write_all(&self.held.to_le_bytes())?;
        }
    }

    /// Accepts one leader connection and serves it.
    ///
    /// # Errors
    /// Fails if the listener cannot accept.
    pub fn serve_one(&mut self, listener: &TcpListener) -> io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        self.serve(&mut stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Journal, MemoryLog};
    use bx_protocol::{Command, CommandKind, Side, TimeInForce};
    use std::thread;

    fn command(order_id: u64) -> Command {
        Command::new(
            CommandKind::NewOrder,
            1,
            1,
            order_id,
            Side::Bid,
            100,
            1,
            TimeInForce::GoodTillCancel,
        )
    }

    /// A follower on a real socket.
    ///
    /// It serves exactly one leader and then stops, which is all any test here
    /// needs and avoids an accept loop a test would have to interrupt. Joining
    /// returns the bytes it accepted, but only once the leader has closed its
    /// side, so every test drops the journal before joining.
    struct RunningReplica {
        address: String,
        thread: Option<thread::JoinHandle<u64>>,
    }

    impl RunningReplica {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let thread = thread::spawn(move || {
                let mut replica = Replica::new(MemoryLog::new(), false);
                let _ = replica.serve_one(&listener);
                replica.held()
            });
            Self {
                address,
                thread: Some(thread),
            }
        }

        /// Bytes the follower accepted. Only valid once the leader has hung up.
        fn accepted(mut self) -> u64 {
            self.thread.take().map_or(0, |t| t.join().unwrap_or(0))
        }
    }

    #[test]
    fn a_group_is_acknowledged_once_a_quorum_holds_it() {
        let replica = RunningReplica::start();
        let log = ReplicatedLog::connect(MemoryLog::new(), std::slice::from_ref(&replica.address))
            .unwrap();
        // Two machines: a majority of two is two, so the one follower must
        // confirm before anything is acknowledged.
        assert_eq!(log.quorum(), 1);

        let mut journal = Journal::open(log).unwrap();
        for id in 0..10 {
            journal.append(&mut command(id)).unwrap();
        }
        journal.sync().unwrap();
        drop(journal);

        assert_eq!(
            replica.accepted(),
            10 * RECORD_LEN as u64,
            "the follower does not hold what the leader acknowledged"
        );
    }

    #[test]
    fn one_sync_replicates_a_whole_group_not_each_record() {
        let replica = RunningReplica::start();
        let log = ReplicatedLog::connect(MemoryLog::new(), std::slice::from_ref(&replica.address))
            .unwrap();
        let mut journal = Journal::open(log).unwrap();

        for group in [1_u64, 5, 20] {
            for id in 0..group {
                journal.append(&mut command(id)).unwrap();
            }
            journal.sync().unwrap();
        }
        drop(journal);
        assert_eq!(replica.accepted(), 26 * RECORD_LEN as u64);
    }

    #[test]
    fn three_machines_tolerate_losing_one_follower() {
        let first = RunningReplica::start();
        let second = RunningReplica::start();
        let log = ReplicatedLog::connect(
            MemoryLog::new(),
            &[first.address.clone(), second.address.clone()],
        )
        .unwrap();
        // Three machines: a majority is two, so one confirmation besides the
        // leader's own copy survives the loss of any single machine.
        assert_eq!(log.quorum(), 1);
        assert_eq!(log.live_followers(), 2);

        let mut journal = Journal::open(log).unwrap();
        journal.append(&mut command(1)).unwrap();
        journal.sync().unwrap();
        journal.append(&mut command(2)).unwrap();
        journal.sync().unwrap();
        drop(journal);

        assert_eq!(first.accepted(), 2 * RECORD_LEN as u64);
        assert_eq!(second.accepted(), 2 * RECORD_LEN as u64);
    }

    #[test]
    fn a_lone_leader_needs_no_confirmations() {
        let log = ReplicatedLog::connect(MemoryLog::new(), &[]).unwrap();
        assert_eq!(log.quorum(), 0);
        assert_eq!(log.live_followers(), 0);
        let mut journal = Journal::open(log).unwrap();
        journal.append(&mut command(1)).unwrap();
        journal.sync().unwrap();
    }

    #[test]
    fn losing_the_only_follower_refuses_rather_than_acknowledging() {
        // A follower that serves one group and then dies. The first group is
        // confirmed; after that no quorum exists and the leader must refuse.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let follower = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut header = [0_u8; size_of::<u32>()];
            stream.read_exact(&mut header).unwrap();
            let length = u32::from_le_bytes(header) as usize;
            let mut group = vec![0_u8; length];
            stream.read_exact(&mut group).unwrap();
            stream.write_all(&(length as u64).to_le_bytes()).unwrap();
        });

        let log = ReplicatedLog::connect(MemoryLog::new(), &[address]).unwrap();
        let mut journal = Journal::open(log).unwrap();
        journal.append(&mut command(1)).unwrap();
        journal.sync().unwrap();
        follower.join().unwrap();

        journal.append(&mut command(2)).unwrap();
        assert!(
            journal.sync().is_err(),
            "acknowledged a group that no quorum holds"
        );
    }

    #[test]
    fn the_replica_refuses_a_group_that_is_not_whole_records() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = thread::spawn(move || {
            let mut replica = Replica::new(MemoryLog::new(), false);
            replica.serve_one(&listener)
        });

        let mut leader = TcpStream::connect(address).unwrap();
        // A length that is not a whole number of records: corruption, or a peer
        // speaking a different protocol version.
        leader.write_all(&30_u32.to_le_bytes()).unwrap();
        leader.write_all(&[0_u8; 30]).unwrap();
        drop(leader);

        assert!(
            server.join().unwrap().is_err(),
            "the replica accepted a partial record"
        );
    }

    #[test]
    fn the_leaders_own_log_is_still_replayable() {
        let replica = RunningReplica::start();
        let log = ReplicatedLog::connect(MemoryLog::new(), std::slice::from_ref(&replica.address))
            .unwrap();
        let mut journal = Journal::open(log).unwrap();
        for id in 0..5 {
            journal.append(&mut command(id)).unwrap();
        }
        journal.sync().unwrap();

        let records = journal.replay().collect_all().unwrap();
        drop(journal);
        replica.accepted();

        assert_eq!(
            records.iter().map(|c| c.order_id).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4],
            "replicating broke the local log"
        );
    }
}
