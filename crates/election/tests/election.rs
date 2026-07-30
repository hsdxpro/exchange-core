//! Electing a leader, and re-electing one when it dies.
//!
//! Three nodes in one process, each with its own store and its own socket, which
//! is the smallest arrangement where a majority means anything. What is being
//! checked is not that openraft works — it is that this integration of it does:
//! that exactly one node leads, that the term it leads under is usable as a
//! fencing token, and that killing the leader produces a new one **without
//! anybody editing a file**, which is the whole point of the exercise.

use bx_election::{Leadership, NodeId};
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Generous. A cluster settles in about a second by design — a heartbeat every
/// 250 ms against a 1–2 second patience — and a loaded test machine is slower
/// than a venue.
/// How long an election may take before the test gives up.
///
/// Sixty seconds is absurd for a Raft election, which settles in about a second
/// on an idle machine. It is not absurd on a shared runner, or on a desk running
/// two other test suites: the node has to miss heartbeats, time out, campaign and
/// win, and every one of those steps is a timer competing for a core. This ran
/// out at twenty while the machine was busy, reporting a working election as a
/// broken one. A generous limit costs nothing when the code is right and seconds
/// when it is wrong.
const SETTLE: Duration = Duration::from_secs(60);

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("bx-election-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Three addresses the OS is not using. Bound and released, which is racy in
/// principle and reliable in practice on a loopback interface — and the
/// alternative, fixed ports, makes two test binaries collide.
fn free_addresses(count: usize) -> Vec<String> {
    let held: Vec<TcpListener> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    held.iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect()
}

fn cluster(scratch: &Scratch, size: usize) -> Vec<(NodeId, String)> {
    let _ = scratch;
    free_addresses(size)
        .into_iter()
        .enumerate()
        .map(|(index, address)| (index as NodeId + 1, address))
        .collect()
}

/// Waits until exactly one of `nodes` says it leads, and returns which.
fn settled(nodes: &[Leadership], within: Duration) -> Option<usize> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let leaders: Vec<usize> = nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.is_leader())
            .map(|(index, _)| index)
            .collect();
        if leaders.len() == 1 {
            return Some(leaders[0]);
        }
        assert!(
            leaders.len() <= 1,
            "two nodes each believe they lead: {leaders:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

#[test]
fn three_nodes_elect_one_leader_without_being_told_to() {
    let scratch = Scratch::new("elect");
    let peers = cluster(&scratch, 3);
    let nodes: Vec<Leadership> = peers
        .iter()
        .map(|(id, _)| {
            Leadership::join(*id, &peers, &scratch.file(&format!("node-{id}.json")))
                .unwrap_or_else(|e| panic!("node {id} could not join: {e}"))
        })
        .collect();

    let leader = settled(&nodes, SETTLE).expect("the cluster never settled on a leader");
    let term = nodes[leader].term();
    assert!(term > 0, "a leader was elected under term zero");

    // Everybody agrees on the term, which is what makes it a fencing token
    // rather than one node's opinion.
    for node in &nodes {
        assert_eq!(
            node.term(),
            term,
            "node {} disagrees about the term",
            node.id()
        );
    }

    // And the leader can still reach a majority, which is what it must prove
    // before it is allowed to take an order.
    nodes[leader]
        .announce()
        .expect("the leader could not reach a majority to announce itself");
}

#[test]
fn killing_the_leader_elects_another_with_a_higher_term() {
    // The property the whole crate exists for. Before this, a promotion meant a
    // person editing a term into a configuration file and starting a process.
    let scratch = Scratch::new("failover");
    let peers = cluster(&scratch, 3);
    let mut nodes: Vec<Option<Leadership>> = peers
        .iter()
        .map(|(id, _)| {
            Some(
                Leadership::join(*id, &peers, &scratch.file(&format!("node-{id}.json")))
                    .unwrap_or_else(|e| panic!("node {id} could not join: {e}")),
            )
        })
        .collect();

    let live: Vec<Leadership> = nodes.iter_mut().filter_map(Option::take).collect();
    let mut nodes = live;
    let first = settled(&nodes, SETTLE).expect("the cluster never settled on a leader");
    let first_term = nodes[first].term();

    // Kill it, as a machine failure would. Dropping stops its Raft node, so it
    // stops answering heartbeats and stops standing for election.
    let dead = nodes.remove(first);
    drop(dead);

    let second = settled(&nodes, SETTLE).expect("no replacement was elected after the leader died");
    let second_term = nodes[second].term();

    assert!(
        second_term > first_term,
        "the replacement leads under term {second_term}, not above the dead leader's \
         {first_term} — a fencing token that does not increase fences nothing"
    );
    nodes[second]
        .announce()
        .expect("the replacement could not reach a majority");
}

#[test]
fn a_minority_elects_nobody() {
    // One node of three cannot lead, and must not decide it can. A node that
    // promoted itself without a majority would be serving orders the cluster has
    // no copy of.
    let scratch = Scratch::new("minority");
    let peers = cluster(&scratch, 3);
    let lonely = Leadership::join(peers[1].0, &peers, &scratch.file("lonely.json"))
        .expect("the node could not start");

    assert!(
        lonely.await_leadership(Duration::from_secs(6)).is_none(),
        "a node reached leadership with one vote out of three"
    );
}

#[test]
fn a_restarted_node_remembers_the_vote_it_cast() {
    // Raft's safety rests on never voting twice in one term. A vote that reached
    // memory and not the platter would let a restarted node vote again, and two
    // leaders in one term is the failure everything here exists to prevent.
    let scratch = Scratch::new("durable-vote");
    let peers = cluster(&scratch, 3);
    let state = scratch.file("node-1.json");

    {
        let nodes: Vec<Leadership> = peers
            .iter()
            .map(|(id, _)| {
                Leadership::join(*id, &peers, &scratch.file(&format!("node-{id}.json"))).unwrap()
            })
            .collect();
        settled(&nodes, SETTLE).expect("the cluster never settled");
    }

    // The file outlives the process that wrote it, and it holds a vote.
    let written = std::fs::read_to_string(&state).expect("no leadership state was written");
    assert!(
        written.contains("\"vote\""),
        "the leadership state holds no vote: {written}"
    );
    assert!(
        !written.contains("\"vote\":null"),
        "the leadership state was written before a vote was cast: {written}"
    );
}
