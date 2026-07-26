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
//!
//! It does, however, **fence**. Every group carries the leader's term, and a
//! follower refuses anything from a term older than the highest it has seen. That
//! is what makes a promotion safe whoever performs it: a leader that has been
//! replaced cannot keep writing, so two leaders cannot acknowledge orders into
//! divergent logs. Without it, election would be the *second* thing missing and
//! the first would be silent corruption -- a partitioned leader appending happily
//! to followers that already have a newer one.

use crate::{HEADER_LEN as FILE_HEADER, LogStorage, RECORD_LEN};

/// Where records start in a log, past the file's identifying header. Offsets on
/// the wire are relative to the first record, so a leader and a follower agree on
/// what "byte zero of the log" means regardless of the header.
const HEADER_OFFSET: u64 = FILE_HEADER;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

/// A follower's reply: the highest term it has seen, then the bytes it holds.
///
/// The term comes back so a deposed leader finds out. It asked for a quorum and
/// is told, by the followers themselves, that someone newer has taken over.
const ACK_LEN: usize = 2 * size_of::<u64>();

/// A request header: what is being asked, the asker's term, then a length.
const HEADER_LEN: usize = size_of::<u8>() + size_of::<u64>() + size_of::<u32>();

/// How long to wait before trying a follower again during a promotion.
const RETRY_PAUSE: Duration = Duration::from_millis(50);

/// Groups between attempts to bring a dropped follower back in.
///
/// Not every group. Connecting to a machine that is genuinely gone costs a
/// syscall and, depending on the platform and the failure, a wait — and it would
/// be paid on the commit path, in front of orders. Once every few hundred groups
/// is frequent enough that a follower rejoins within a moment of coming back,
/// and rare enough that a permanently dead one costs nothing measurable.
const READMIT_EVERY: u32 = 256;

/// Bytes of backfill sent to a rejoining follower at a time.
///
/// A follower that was away for an hour is behind by more than fits in memory,
/// so the gap is shipped in bounded pieces rather than read into one buffer.
const BACKFILL_CHUNK: usize = 1 << 20;

/// Append this group and confirm.
const APPEND: u8 = 0;
/// Report what you hold, without changing anything.
const QUERY: u8 = 1;
/// Send back a range of your log.
const FETCH: u8 = 2;

/// Why a follower cannot be brought back in.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Refusal {
    /// It has seen a newer leader, so this one is finished.
    Deposed,
    /// It holds records this leader does not, which can only mean it followed
    /// somebody else. Appending on top would be exactly the divergence that
    /// fencing exists to prevent, so it stays out and the cluster runs degraded.
    Ahead,
}

/// Where a rejoining follower's backfill should start, or why there is none.
///
/// Pulled out of the socket work so the decision can be checked on its own. It
/// is three comparisons, and every one of them is the difference between a
/// consistent cluster and a quietly corrupt one.
const fn backfill_from(seen: u64, term: u64, held: u64, settled: u64) -> Result<u64, Refusal> {
    if seen > term {
        return Err(Refusal::Deposed);
    }
    if held > settled {
        return Err(Refusal::Ahead);
    }
    Ok(held)
}

fn request(kind: u8, term: u64, length: u32) -> [u8; HEADER_LEN] {
    let mut header = [0_u8; HEADER_LEN];
    header[0] = kind;
    header[1..9].copy_from_slice(&term.to_le_bytes());
    header[9..].copy_from_slice(&length.to_le_bytes());
    header
}

/// One follower, as the leader sees it.
#[derive(Debug)]
struct Follower {
    address: String,
    stream: TcpStream,
    /// A follower that has failed is left in place rather than removed, so the
    /// quorum arithmetic keeps counting against the configured cluster size
    /// rather than silently shrinking to whoever is still answering.
    ///
    /// It is periodically offered a way back — see `readmit`. Leaving it out
    /// permanently meant one slow moment cost a node for the lifetime of the
    /// process, and the next slow moment stopped the venue.
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
    /// Kept so a socket can be replaced during a promotion.
    ack_timeout: Duration,
    /// This leader's term. Monotonic across promotions, and the only thing that
    /// lets a follower tell a current leader from a replaced one.
    term: u64,
    /// Set once a follower reports a newer term. A deposed leader stops
    /// acknowledging rather than continuing to write.
    deposed: bool,
    /// Groups since a dropped follower was last offered a way back.
    since_readmit: u32,
    /// Followers brought back in after dropping out, and the bytes shipped to
    /// catch them up. An operator watching the first climb knows a node is
    /// flapping even though the venue never stopped.
    readmitted: u64,
    backfilled: u64,
}

impl<L: LogStorage> ReplicatedLog<L> {
    /// Connects to every follower and prepares the quorum.
    ///
    /// The quorum is a majority of the cluster including the leader, so two
    /// followers means both plus the leader is three and one confirmation is
    /// enough to survive losing any single machine.
    ///
    /// `ack_timeout` is how long the leader will wait for one follower to
    /// confirm. It must be set, and it is the difference between surviving a
    /// partition and not: a *crashed* follower closes its socket and is noticed
    /// immediately, but a **hung** one -- a frozen machine, a partitioned
    /// network, a stalled process -- leaves the socket open and silent. Without
    /// a deadline the leader blocks inside `sync` forever and the whole venue
    /// stops acknowledging anything, which is worse than the single-machine
    /// failure the replication was there to survive. Pick it from the network
    /// the cluster actually runs on; a value below the real round trip turns
    /// healthy followers into dead ones.
    ///
    /// `term` must increase every time leadership moves. Whatever performs the
    /// promotion owns that number; this only enforces it.
    ///
    /// # Errors
    /// Fails if a follower cannot be reached.
    pub fn connect(
        local: L,
        addresses: &[String],
        ack_timeout: Duration,
        term: u64,
    ) -> io::Result<Self> {
        let mut followers = Vec::with_capacity(addresses.len());
        for address in addresses {
            let stream = TcpStream::connect(address)?;
            stream.set_nodelay(true)?;
            stream.set_read_timeout(Some(ack_timeout))?;
            followers.push(Follower {
                address: address.clone(),
                stream,
                live: true,
            });
        }
        // Majority of (followers + leader), minus the leader's own vote.
        let cluster = followers.len() + 1;
        let quorum = cluster / 2 + 1 - 1;
        Ok(Self {
            local,
            followers,
            quorum,
            pending: Vec::new(),
            ack_timeout,
            term,
            deposed: false,
            since_readmit: 0,
            readmitted: 0,
            backfilled: 0,
        })
    }

    /// Followers that dropped out and were brought back.
    #[must_use]
    pub const fn readmitted(&self) -> u64 {
        self.readmitted
    }

    /// Bytes shipped to followers to close the gap they missed.
    #[must_use]
    pub const fn backfilled(&self) -> u64 {
        self.backfilled
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

    /// True once a follower has reported a newer term.
    ///
    /// A deposed leader must stop: it cannot reach a quorum any more, and
    /// pretending otherwise would acknowledge orders the cluster has not kept.
    #[must_use]
    pub const fn deposed(&self) -> bool {
        self.deposed
    }

    #[must_use]
    pub const fn term(&self) -> u64 {
        self.term
    }

    /// Brings this leader's log up to the longest any majority holds.
    ///
    /// This is what makes a promotion complete rather than merely exclusive. A
    /// group is acknowledged only once a majority holds it, and this contacts a
    /// majority, so the two majorities intersect: whatever was acknowledged is on
    /// at least one node answering here. Taking the longest log therefore recovers
    /// everything any client was ever told about.
    ///
    /// Consensus is not being reinvented -- something else must already have
    /// decided this node is the leader and given it a term. This is the recovery
    /// step that election alone does not provide when the data lives outside the
    /// elected log.
    ///
    /// # Errors
    /// Fails if a majority cannot be reached, in which case this node must not
    /// serve: it cannot know what it is missing.
    pub fn catch_up(&mut self, within: Duration) -> io::Result<u64> {
        let mut answered = 0;
        let mut longest = (self.local_len()?, usize::MAX);
        let deadline = Instant::now() + within;

        // Every follower is asked once per pass, and the passes repeat until the
        // deadline. Retrying matters because a follower serves one leader at a
        // time and may still be finishing with the one that just died. Going
        // round rather than exhausting one at a time matters because otherwise a
        // single unreachable node spends the entire window before a node that is
        // up and ready is asked at all.
        let mut pending: Vec<usize> = (0..self.followers.len()).collect();
        while !pending.is_empty() {
            let last_pass = Instant::now() >= deadline;
            pending.retain(|&index| match self.ask(index, QUERY, &[]) {
                Ok((_, held)) => {
                    answered += 1;
                    if held > longest.0 {
                        longest = (held, index);
                    }
                    false
                }
                Err(_) if last_pass => {
                    self.followers[index].live = false;
                    false
                }
                // A timed-out socket cannot be reused: the late reply would be
                // read as the answer to the next question.
                Err(_) => {
                    let _ = self.reconnect(index);
                    true
                }
            });
            if !pending.is_empty() && !last_pass {
                std::thread::sleep(RETRY_PAUSE);
            }
        }

        // A majority of the cluster, counting this node. Refusing here is the
        // point: a leader that cannot see a majority cannot know whether it is
        // behind, and serving would risk losing an acknowledged order.
        if answered < self.quorum {
            return Err(io::Error::other(format!(
                "reached {answered} of {} followers; a promotion needs a majority \
                 to know what it is missing",
                self.quorum
            )));
        }

        let (target, source) = longest;
        let have = self.local_len()?;
        if source == usize::MAX || target <= have {
            return Ok(0);
        }

        // Pull the tail this node never saw, in the order it was written.
        let mut request = [0_u8; size_of::<u64>() + size_of::<u32>()];
        request[..8].copy_from_slice(&have.to_le_bytes());
        let wanted = u32::try_from(target - have)
            .map_err(|_| io::Error::other("the tail to recover is too large for one fetch"))?;
        request[8..].copy_from_slice(&wanted.to_le_bytes());

        let (_, _) = self.ask(source, FETCH, &request)?;
        let mut tail = vec![0_u8; wanted as usize];
        self.followers[source].stream.read_exact(&mut tail)?;
        if !tail.len().is_multiple_of(RECORD_LEN) {
            return Err(io::Error::other("recovered tail is not whole records"));
        }
        self.local.append(&tail)?;
        self.local.sync()?;
        Ok(tail.len() as u64)
    }

    /// Opens a fresh connection to one follower, replacing a socket that timed
    /// out. A timed-out stream cannot be reused: the reply to the request that
    /// timed out may still arrive and would be read as the answer to the next one.
    fn reconnect(&mut self, index: usize) -> io::Result<()> {
        std::thread::sleep(RETRY_PAUSE);
        self.dial(index)?;
        self.followers[index].live = true;
        Ok(())
    }

    /// Replaces a follower's socket. No pause: this is also called from the
    /// commit path, where sleeping would put the delay in front of orders.
    fn dial(&mut self, index: usize) -> io::Result<()> {
        let stream = TcpStream::connect(&self.followers[index].address)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(self.ack_timeout))?;
        self.followers[index].stream = stream;
        Ok(())
    }

    /// Offers every dropped follower a way back into the quorum.
    ///
    /// Without this a follower that failed once was never spoken to again, so a
    /// single slow moment cost a node for the life of the process and the next
    /// slow moment stopped the venue. That is what a three-node cluster did
    /// under load while both replica processes were alive and healthy: the
    /// leader lost one, carried on with no margin, and fail-stopped on the
    /// second hiccup.
    ///
    /// `settled` is everything the leader holds *except* the group about to be
    /// sent, which goes out through the ordinary path — backfilling it here too
    /// would write it twice.
    fn readmit(&mut self, settled: u64) {
        for index in 0..self.followers.len() {
            if self.followers[index].live {
                continue;
            }
            if self.rejoin(index, settled).is_ok() {
                self.followers[index].live = true;
                self.readmitted += 1;
            } else {
                // Still gone, or ahead of this leader. Either way it stays out
                // and the quorum keeps counting it against the cluster size.
                self.followers[index].live = false;
            }
        }
    }

    /// Reconnects one follower and closes the gap it missed.
    ///
    /// Reconnecting alone is not enough and would be worse than leaving it out:
    /// a follower that was away has a hole in its log, and appending new groups
    /// on top would leave it holding a journal that looks complete and is not.
    /// So it is asked what it holds, and the missing range is shipped from the
    /// leader's own log before it counts towards a quorum again.
    fn rejoin(&mut self, index: usize, settled: u64) -> io::Result<()> {
        self.dial(index)?;
        let (seen, held) = self.ask(index, QUERY, &[])?;
        let start = match backfill_from(seen, self.term, held, settled) {
            Ok(from) => from,
            Err(Refusal::Deposed) => {
                self.deposed = true;
                return Err(io::Error::other(
                    "a follower reported a newer term; this leader has been replaced",
                ));
            }
            Err(Refusal::Ahead) => {
                return Err(io::Error::other(
                    "follower holds more than this leader; refusing to write over it",
                ));
            }
        };

        let mut cursor = start;
        let mut buffer = vec![0_u8; BACKFILL_CHUNK];
        while cursor < settled {
            let want = usize::try_from((settled - cursor).min(BACKFILL_CHUNK as u64))
                .unwrap_or(BACKFILL_CHUNK);
            let read = self
                .local
                .read_at(HEADER_OFFSET + cursor, &mut buffer[..want])?;
            if read == 0 {
                return Err(io::Error::other(
                    "the leader's own log is shorter than it believes",
                ));
            }
            let length =
                u32::try_from(read).map_err(|_| io::Error::other("backfill chunk too large"))?;
            let follower = &mut self.followers[index];
            follower
                .stream
                .write_all(&request(APPEND, self.term, length))?;
            follower.stream.write_all(&buffer[..read])?;
            let mut ack = [0_u8; ACK_LEN];
            follower.stream.read_exact(&mut ack)?;
            cursor += read as u64;
            self.backfilled += read as u64;
        }
        Ok(())
    }

    /// Bytes of records this node's own log holds, excluding the file header.
    ///
    /// # Errors
    /// Fails if the underlying log cannot report its length.
    pub fn local_len(&self) -> io::Result<u64> {
        Ok(self.local.end()?.saturating_sub(HEADER_OFFSET))
    }

    /// Makes a follower look like it failed, which is what a hiccup does: the
    /// socket the leader is holding stops working and it drops out of the
    /// quorum. Exists so a test can produce that state without a real network
    /// fault.
    #[cfg(test)]
    fn drop_follower(&mut self, index: usize) {
        let _ = self.followers[index]
            .stream
            .shutdown(std::net::Shutdown::Both);
        self.followers[index].live = false;
    }

    /// Sends one request and reads the fixed reply.
    fn ask(&mut self, index: usize, kind: u8, payload: &[u8]) -> io::Result<(u64, u64)> {
        let length = u32::try_from(payload.len())
            .map_err(|_| io::Error::other("request payload too large"))?;
        let follower = &mut self.followers[index];
        follower
            .stream
            .write_all(&request(kind, self.term, length))?;
        if !payload.is_empty() {
            follower.stream.write_all(payload)?;
        }
        let mut reply = [0_u8; ACK_LEN];
        follower.stream.read_exact(&mut reply)?;
        Ok((
            u64::from_le_bytes(reply[..8].try_into().unwrap_or_default()),
            u64::from_le_bytes(reply[8..].try_into().unwrap_or_default()),
        ))
    }

    /// Sends the pending bytes to every live follower and waits for a quorum.
    fn replicate(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        if self.deposed {
            return Err(io::Error::other(
                "this leader has been replaced by a newer term and cannot acknowledge",
            ));
        }
        // A dropped follower is offered a way back before the group goes out, so
        // it rejoins holding everything rather than everything since it woke up.
        self.since_readmit = self.since_readmit.saturating_add(1);
        if self.since_readmit >= READMIT_EVERY {
            self.since_readmit = 0;
            if self.followers.iter().any(|follower| !follower.live) {
                let settled = self.local_len()?.saturating_sub(self.pending.len() as u64);
                self.readmit(settled);
                if self.deposed {
                    return Err(io::Error::other(
                        "a follower reported a newer term; this leader has been replaced",
                    ));
                }
            }
        }

        let length = u32::try_from(self.pending.len())
            .map_err(|_| io::Error::other("replication group too large"))?;
        let header = request(APPEND, self.term, length);

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
            // A timeout lands here as an error, so a follower that has stopped
            // answering is treated exactly like one that has died: dropped from
            // the live set, and counted as a missing confirmation rather than
            // waited on.
            if follower.stream.read_exact(&mut ack).is_ok() {
                let seen = u64::from_le_bytes(ack[..8].try_into().unwrap_or_default());
                if seen > self.term {
                    // A follower has a newer leader. This one is finished, and
                    // saying so is the whole point of fencing.
                    self.deposed = true;
                    follower.live = false;
                } else {
                    confirmed += 1;
                }
            } else {
                follower.live = false;
            }
        }

        if self.deposed {
            return Err(io::Error::other(
                "a follower reported a newer term; this leader has been replaced",
            ));
        }

        if confirmed < self.quorum {
            // Refusing is the only safe answer. Reporting success here would
            // acknowledge a command to a client that one machine's failure
            // could still erase.
            // Names all three numbers. "0 of 1" alone reads as though the
            // leader forgot a node it was configured with, when what it means
            // is that one confirmation was needed and none arrived.
            return Err(io::Error::other(format!(
                "replication reached {confirmed} confirmations, needs {}; {} of {} \
                 followers still answering",
                self.quorum,
                self.followers.iter().filter(|f| f.live).count(),
                self.followers.len()
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

    /// Durable means "a quorum holds it", so this does not wait for the local
    /// device -- that is the entire advantage being bought.
    ///
    /// It does still push the bytes out of the process. A log that buffers until
    /// `sync` and a `sync` that only replicates meant the leader's own file was
    /// never written at all: ten thousand acknowledged orders left an eight-byte
    /// file containing nothing but the magic. The quorum held them, so nothing was
    /// lost, but the leader was contributing no copy of its own.
    fn sync(&mut self) -> io::Result<()> {
        self.local.flush()?;
        self.replicate()
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.local.read_at(offset, buf)
    }

    fn end(&self) -> io::Result<u64> {
        self.local.end()
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.local.truncate(len)
    }
}

/// The follower side: accept a leader, append what it sends, confirm.
#[derive(Debug)]
pub struct Replica<L: LogStorage> {
    pub(crate) log: L,
    /// Highest leader term this follower has accepted. Anything older is refused,
    /// which is what stops a replaced leader appending over a newer one's data.
    highest_term: u64,
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
            highest_term: 0,
            flush_before_ack,
            held: 0,
        }
    }

    /// Bytes accepted from the leader.
    #[must_use]
    pub const fn held(&self) -> u64 {
        self.held
    }

    /// Highest leader term accepted.
    #[must_use]
    pub const fn highest_term(&self) -> u64 {
        self.highest_term
    }

    /// The one reply shape: the term that won, and how much this node holds.
    fn reply(&self, stream: &mut TcpStream) -> io::Result<()> {
        let mut reply = [0_u8; ACK_LEN];
        reply[..8].copy_from_slice(&self.highest_term.to_le_bytes());
        reply[8..].copy_from_slice(&self.held.to_le_bytes());
        stream.write_all(&reply)
    }

    /// Serves one leader until it disconnects or goes quiet.
    ///
    /// `idle_timeout` bounds how long a silent leader may hold this follower. A
    /// follower serves one leader at a time, so without it a leader that hangs
    /// rather than dying keeps its replacement from ever being served -- the
    /// promotion cannot complete because the node it must ask is still waiting on
    /// a corpse. Longer than the leader's own acknowledgement timeout, or a busy
    /// leader looks silent.
    ///
    /// # Errors
    /// Fails on an I/O error that is not the leader simply going away.
    pub fn serve(&mut self, stream: &mut TcpStream, idle_timeout: Duration) -> io::Result<()> {
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(idle_timeout))?;
        let mut header = [0_u8; HEADER_LEN];
        let mut group = Vec::new();

        loop {
            match stream.read_exact(&mut header) {
                Ok(()) => {}
                // Gone, or gone quiet. Either way this follower is free again.
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                    ) =>
                {
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
            let kind = header[0];
            let term = u64::from_le_bytes(header[1..9].try_into().unwrap_or_default());
            let length = u32::from_le_bytes(header[9..].try_into().unwrap_or_default()) as usize;

            if term < self.highest_term {
                // A leader that has been replaced. Refusing loudly rather than
                // ignoring it: the connection ends, and the reply tells it which
                // term won so it learns why.
                let mut reply = [0_u8; ACK_LEN];
                reply[..8].copy_from_slice(&self.highest_term.to_le_bytes());
                reply[8..].copy_from_slice(&self.held.to_le_bytes());
                let _ = stream.write_all(&reply);
                return Err(io::Error::other(format!(
                    "leader term {term} is older than {}, refusing",
                    self.highest_term
                )));
            }
            self.highest_term = term;

            // A question, not a change. Answering it is how a newly promoted
            // leader discovers what it is missing.
            if kind == QUERY {
                self.reply(stream)?;
                continue;
            }
            if kind == FETCH {
                let mut range = [0_u8; size_of::<u64>() + size_of::<u32>()];
                stream.read_exact(&mut range)?;
                let from = u64::from_le_bytes(range[..8].try_into().unwrap_or_default());
                let wanted = u32::from_le_bytes(range[8..].try_into().unwrap_or_default()) as usize;
                self.reply(stream)?;
                // Served a record at a time, so a fetch never needs a buffer
                // proportional to the log.
                let mut record = [0_u8; RECORD_LEN];
                let mut sent = 0;
                while sent < wanted {
                    let read = self
                        .log
                        .read_at(HEADER_OFFSET + from + sent as u64, &mut record)?;
                    if read < RECORD_LEN {
                        break;
                    }
                    stream.write_all(&record)?;
                    sent += RECORD_LEN;
                }
                continue;
            }

            if length == 0 || !length.is_multiple_of(RECORD_LEN) {
                return Err(io::Error::other(
                    "leader sent a group that is not whole records",
                ));
            }
            group.resize(length, 0);
            stream.read_exact(&mut group)?;

            self.log.append(&group)?;
            // Always out of the process, so a follower's file is a real copy and
            // it survives its own process dying. Only onto the platter when asked,
            // because waiting for the device is the cost the leader is paying a
            // quorum to avoid. A log that buffers until `sync` and a follower that
            // only synced when configured to meant the file held nothing but its
            // magic while the follower confirmed ten thousand records from memory.
            self.log.flush()?;
            if self.flush_before_ack {
                self.log.sync()?;
            }
            self.held += length as u64;

            self.reply(stream)?;
        }
    }

    /// Accepts one leader connection and serves it.
    ///
    /// # Errors
    /// Fails if the listener cannot accept.
    pub fn serve_one(&mut self, listener: &TcpListener, idle_timeout: Duration) -> io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        self.serve(&mut stream, idle_timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Journal, MemoryLog};
    use bx_protocol::{Command, CommandKind, Side, TimeInForce};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::thread;

    /// Generous for loopback, short enough that the hung-follower test does not
    /// slow the suite down.
    const ACK_TIMEOUT: Duration = Duration::from_millis(250);
    /// Any term will do for the tests that are not about fencing.
    const TERM: u64 = 7;
    /// Generous, so a busy machine does not look like a silent leader.
    const IDLE: Duration = Duration::from_secs(5);

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
                let _ = replica.serve_one(&listener, IDLE);
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
        let log = ReplicatedLog::connect(
            MemoryLog::new(),
            std::slice::from_ref(&replica.address),
            ACK_TIMEOUT,
            TERM,
        )
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
        let log = ReplicatedLog::connect(
            MemoryLog::new(),
            std::slice::from_ref(&replica.address),
            ACK_TIMEOUT,
            TERM,
        )
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
            ACK_TIMEOUT,
            TERM,
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
        let log = ReplicatedLog::connect(MemoryLog::new(), &[], ACK_TIMEOUT, TERM).unwrap();
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
            let mut header = [0_u8; HEADER_LEN];
            stream.read_exact(&mut header).unwrap();
            let length = u32::from_le_bytes(header[9..].try_into().unwrap()) as usize;
            let mut group = vec![0_u8; length];
            stream.read_exact(&mut group).unwrap();
            let mut reply = [0_u8; ACK_LEN];
            reply[..8].copy_from_slice(&TERM.to_le_bytes());
            reply[8..].copy_from_slice(&(length as u64).to_le_bytes());
            stream.write_all(&reply).unwrap();
        });

        let log = ReplicatedLog::connect(MemoryLog::new(), &[address], ACK_TIMEOUT, TERM).unwrap();
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
    fn a_follower_that_hangs_does_not_stall_the_leader_forever() {
        // The dangerous failure is not a follower that dies -- that closes the
        // socket and is noticed at once -- but one that accepts the connection
        // and then goes silent. Without a deadline the leader waits inside sync
        // forever and the venue stops acknowledging anything at all.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let hung = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            // Accept, read nothing, answer nothing, and hold the socket open.
            thread::sleep(Duration::from_secs(2));
            drop(stream);
        });

        let log = ReplicatedLog::connect(MemoryLog::new(), &[address], ACK_TIMEOUT, TERM).unwrap();
        let mut journal = Journal::open(log).unwrap();
        journal.append(&mut command(1)).unwrap();

        let started = std::time::Instant::now();
        let outcome = journal.sync();
        let waited = started.elapsed();

        assert!(
            outcome.is_err(),
            "a silent follower was counted as a confirmation"
        );
        assert!(
            waited < Duration::from_secs(1),
            "the leader waited {waited:?} on a hung follower instead of giving up"
        );
        hung.join().unwrap();
    }

    #[test]
    fn a_replaced_leader_is_refused_and_learns_that_it_was() {
        // The failure fencing exists to prevent: a leader that has been replaced
        // keeps writing, and followers accept it, so two leaders acknowledge
        // orders into logs that diverge. Election is not built yet, but a
        // promotion performed by any means has to be safe.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let follower = thread::spawn(move || {
            let mut replica = Replica::new(MemoryLog::new(), false);
            // Two leaders in turn: term 9 takes over, then term 4 tries.
            let _ = replica.serve_one(&listener, IDLE);
            let result = replica.serve_one(&listener, IDLE);
            (result.is_err(), replica.highest_term(), replica.held())
        });

        // The newer leader writes first and is accepted.
        {
            let log = ReplicatedLog::connect(
                MemoryLog::new(),
                std::slice::from_ref(&address),
                ACK_TIMEOUT,
                9,
            )
            .unwrap();
            let mut journal = Journal::open(log).unwrap();
            journal.append(&mut command(1)).unwrap();
            journal.sync().unwrap();
        }

        // The stale one reconnects and must be turned away.
        let stale = ReplicatedLog::connect(MemoryLog::new(), &[address], ACK_TIMEOUT, 4).unwrap();
        let mut journal = Journal::open(stale).unwrap();
        journal.append(&mut command(2)).unwrap();
        let outcome = journal.sync();

        let (refused, highest, held) = follower.join().unwrap();
        assert!(refused, "the follower accepted a replaced leader");
        assert!(
            outcome.is_err(),
            "a replaced leader acknowledged a group anyway"
        );
        assert_eq!(highest, 9, "the follower forgot which term won");
        assert_eq!(
            held, RECORD_LEN as u64,
            "the stale leader's write reached the follower's log"
        );
    }

    #[test]
    fn a_newer_term_takes_over_from_an_older_one() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let follower = thread::spawn(move || {
            let mut replica = Replica::new(MemoryLog::new(), false);
            let _ = replica.serve_one(&listener, IDLE);
            let _ = replica.serve_one(&listener, IDLE);
            (replica.highest_term(), replica.held())
        });

        for term in [3_u64, 8] {
            let log = ReplicatedLog::connect(
                MemoryLog::new(),
                std::slice::from_ref(&address),
                ACK_TIMEOUT,
                term,
            )
            .unwrap();
            assert_eq!(log.term(), term);
            let mut journal = Journal::open(log).unwrap();
            journal.append(&mut command(term)).unwrap();
            journal.sync().unwrap();
        }

        let (highest, held) = follower.join().unwrap();
        assert_eq!(highest, 8, "the follower did not adopt the newer term");
        assert_eq!(held, 2 * RECORD_LEN as u64, "both leaders' groups are held");
    }

    #[test]
    fn a_promoted_leader_recovers_a_group_it_never_saw() {
        // The guarantee that makes automatic promotion safe. A group is
        // acknowledged once a majority holds it; a promotion contacts a majority;
        // two majorities intersect. So the longest log any majority holds contains
        // everything a client was ever told about, and taking it recovers exactly
        // that.
        //
        // Here the old leader wrote three groups, the new one has none, and the
        // follower still has all three.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let follower = thread::spawn(move || {
            let mut replica = Replica::new(MemoryLog::new(), false);
            // Serves the old leader, then the promoted one.
            let _ = replica.serve_one(&listener, IDLE);
            let _ = replica.serve_one(&listener, IDLE);
            replica.held()
        });

        // The old leader, term 4.
        {
            let log = ReplicatedLog::connect(
                MemoryLog::new(),
                std::slice::from_ref(&address),
                ACK_TIMEOUT,
                4,
            )
            .unwrap();
            let mut journal = Journal::open(log).unwrap();
            for id in 0..3 {
                journal.append(&mut command(id)).unwrap();
                journal.sync().unwrap();
            }
        }

        // A different node is promoted at term 5 with an empty log of its own.
        let mut promoted = ReplicatedLog::connect(
            MemoryLog::new(),
            std::slice::from_ref(&address),
            ACK_TIMEOUT,
            5,
        )
        .unwrap();
        let recovered = promoted.catch_up(Duration::from_secs(2)).unwrap();
        assert_eq!(
            recovered,
            3 * RECORD_LEN as u64,
            "the promoted leader did not recover the tail it was missing"
        );

        // And what it recovered is the real thing, in order.
        let journal = Journal::open(promoted).unwrap();
        let replayed = journal.replay().collect_all().unwrap();
        assert_eq!(
            replayed.iter().map(|c| c.order_id).collect::<Vec<_>>(),
            vec![0, 1, 2],
            "the recovered tail is not the log that was acknowledged"
        );

        drop(journal);
        assert_eq!(follower.join().unwrap(), 3 * RECORD_LEN as u64);
    }

    #[test]
    fn a_leader_that_cannot_reach_a_majority_refuses_to_serve() {
        // It cannot know what it is missing, so serving would risk losing an
        // order a client was told about. Refusing is the only safe answer.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let follower = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            // Accepts and says nothing, as a partitioned node looks.
            thread::sleep(Duration::from_millis(600));
            drop(stream);
        });

        let mut promoted = ReplicatedLog::connect(
            MemoryLog::new(),
            std::slice::from_ref(&address),
            ACK_TIMEOUT,
            9,
        )
        .unwrap();
        let outcome = promoted.catch_up(Duration::from_secs(2));
        assert!(
            outcome.is_err(),
            "a leader served without knowing whether it was behind"
        );
        follower.join().unwrap();
    }

    #[test]
    fn catching_up_when_already_current_copies_nothing() {
        let replica = RunningReplica::start();
        let log = ReplicatedLog::connect(
            MemoryLog::new(),
            std::slice::from_ref(&replica.address),
            ACK_TIMEOUT,
            TERM,
        )
        .unwrap();
        let mut journal = Journal::open(log).unwrap();
        journal.append(&mut command(1)).unwrap();
        journal.sync().unwrap();

        let mut log = journal.into_storage();
        assert_eq!(
            log.catch_up(Duration::from_secs(2)).unwrap(),
            0,
            "copied a tail it already had"
        );
        drop(log);
        replica.accepted();
    }

    #[test]
    fn the_replica_refuses_a_group_that_is_not_whole_records() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = thread::spawn(move || {
            let mut replica = Replica::new(MemoryLog::new(), false);
            replica.serve_one(&listener, IDLE)
        });

        let mut leader = TcpStream::connect(address).unwrap();
        // A length that is not a whole number of records: corruption, or a peer
        // speaking a different protocol version.
        leader.write_all(&request(APPEND, TERM, 30)).unwrap();
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
        let log = ReplicatedLog::connect(
            MemoryLog::new(),
            std::slice::from_ref(&replica.address),
            ACK_TIMEOUT,
            TERM,
        )
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

    /// A follower that survives its leader, the way the `replica` binary does.
    ///
    /// The one-shot helper above cannot express readmission at all: the whole
    /// property is that a follower keeps its log across a disconnection and is
    /// brought back holding everything, so it has to outlive the connection that
    /// dropped.
    struct StandingReplica {
        address: String,
        held: Arc<AtomicU64>,
        /// A fold over everything it holds. Byte counts alone would pass a
        /// backfill that shipped the *wrong* range, which is the failure worth
        /// catching: a follower holding the right number of wrong records looks
        /// healthy from every angle except this one.
        digest: Arc<AtomicU64>,
        serving: Arc<AtomicBool>,
    }

    /// FNV-1a over a whole log, header included, so two logs that agree produce
    /// the same number and two that do not almost certainly will not.
    fn digest_of<L: LogStorage>(log: &L) -> u64 {
        let end = usize::try_from(log.end().unwrap_or(0)).unwrap_or(0);
        let mut buffer = vec![0_u8; end];
        let read = log.read_at(0, &mut buffer).unwrap_or(0);
        buffer.truncate(read);
        buffer.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
    }

    impl StandingReplica {
        fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let held = Arc::new(AtomicU64::new(0));
            let reported = Arc::clone(&held);
            let digest = Arc::new(AtomicU64::new(0));
            let folded = Arc::clone(&digest);
            let serving = Arc::new(AtomicBool::new(true));
            let running = Arc::clone(&serving);

            // Detached: it ends with the test process. A stop flag would have to
            // interrupt a blocking accept, which is more machinery than a test
            // helper is worth.
            thread::spawn(move || {
                let mut replica = Replica::new(MemoryLog::new(), false);
                while running.load(Ordering::Relaxed) {
                    let _ = replica.serve_one(&listener, IDLE);
                    folded.store(digest_of(&replica.log), Ordering::Relaxed);
                    reported.store(replica.held(), Ordering::Relaxed);
                }
            });
            Self {
                address,
                held,
                digest,
                serving,
            }
        }

        /// Bytes it has accepted, as of the last leader that hung up.
        fn held(&self) -> u64 {
            self.held.load(Ordering::Relaxed)
        }

        fn digest(&self) -> u64 {
            self.digest.load(Ordering::Relaxed)
        }

        /// Stops it answering after the current leader goes.
        fn retire(&self) {
            self.serving.store(false, Ordering::Relaxed);
        }
    }

    /// Drives enough groups through a leader to pass the readmission interval.
    fn push_groups(journal: &mut Journal<ReplicatedLog<MemoryLog>>, from: u64, count: u64) {
        for id in from..from + count {
            journal.append(&mut command(id)).unwrap();
            journal.sync().unwrap();
        }
    }

    #[test]
    fn a_follower_that_drops_out_is_brought_back_holding_everything() {
        // The bug this pins cost a real cluster its leader. A follower that
        // failed once was never spoken to again, so one slow moment took a node
        // out for the life of the process and the next one stopped the venue --
        // with both replica processes alive and healthy the whole time.
        let steady = StandingReplica::start();
        let flaky = StandingReplica::start();
        let mut journal = Journal::open(
            ReplicatedLog::connect(
                MemoryLog::new(),
                &[steady.address.clone(), flaky.address.clone()],
                ACK_TIMEOUT,
                TERM,
            )
            .unwrap(),
        )
        .unwrap();

        push_groups(&mut journal, 0, 4);

        // Take it out from under the leader, which is what a hiccup looks like:
        // the socket the leader holds stops working.
        journal.storage_mut().drop_follower(1);
        assert_eq!(journal.storage().live_followers(), 1);

        // The venue keeps going on the remaining quorum, which is the point of
        // replicating to two.
        push_groups(&mut journal, 100, u64::from(READMIT_EVERY) + 4);

        assert!(
            journal.storage().readmitted() >= 1,
            "the follower was never offered a way back"
        );
        assert_eq!(
            journal.storage().live_followers(),
            2,
            "it reconnected but was not counted back into the quorum"
        );
        assert!(
            journal.storage().backfilled() > 0,
            "it rejoined without being sent what it missed"
        );

        // What matters: it holds everything, not merely everything since it
        // woke up. A follower with a hole in its log is worse than one that is
        // absent, because it looks complete.
        push_groups(&mut journal, 9_000, 1);
        let leader_bytes = journal.storage().local_len().unwrap();
        let leader_digest = digest_of(journal.storage());
        drop(journal);
        steady.retire();
        flaky.retire();

        let deadline = Instant::now() + Duration::from_secs(5);
        while flaky.held() < leader_bytes && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            flaky.held(),
            leader_bytes,
            "the readmitted follower holds {} of the leader's {leader_bytes} bytes",
            flaky.held()
        );
        // Byte for byte, not merely the right number of them. A backfill that
        // shipped the wrong range would satisfy the count above and leave a
        // follower that is confidently wrong.
        assert_eq!(
            flaky.digest(),
            leader_digest,
            "the readmitted follower holds the right number of the wrong records"
        );
        assert_eq!(
            steady.digest(),
            leader_digest,
            "the follower that never dropped out diverged"
        );
    }

    #[test]
    fn readmission_refuses_a_follower_that_followed_somebody_else() {
        // Three comparisons, and each is the difference between a consistent
        // cluster and a quietly corrupt one. Checked directly rather than
        // through a socket, because the states that reach them are ones a
        // healthy cluster cannot be talked into producing.
        const TERM: u64 = 5;

        // Behind: send it what it missed, starting where it stopped.
        assert_eq!(backfill_from(TERM, TERM, 640, 1_280), Ok(640));
        // Level: nothing to send, and it rejoins immediately.
        assert_eq!(backfill_from(TERM, TERM, 1_280, 1_280), Ok(1_280));
        // Never heard from anyone: an empty follower takes the whole log.
        assert_eq!(backfill_from(0, TERM, 0, 1_280), Ok(0));

        // Ahead. It can only be holding records this leader does not because it
        // followed a different one, so writing over them is the divergence
        // fencing exists to prevent.
        assert_eq!(backfill_from(TERM, TERM, 1_920, 1_280), Err(Refusal::Ahead));

        // A newer term outranks everything, including being behind: this leader
        // has been replaced and must stop, not catch anybody up.
        assert_eq!(
            backfill_from(TERM + 1, TERM, 0, 1_280),
            Err(Refusal::Deposed)
        );
        assert_eq!(
            backfill_from(TERM + 1, TERM, 9_999, 1_280),
            Err(Refusal::Deposed),
            "a deposed leader must be told so before anything else is considered"
        );
    }
}
