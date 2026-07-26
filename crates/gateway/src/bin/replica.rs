//! A follower, as a process.
//!
//! Accepts a leader, appends every group it sends, and confirms. A majority of
//! these holding a group is what lets the leader acknowledge an order without
//! waiting for its own disk — measured at 59× faster than an `fsync`.
//!
//! ```text
//! replica [address] [--file PATH] [--flush]
//! ```
//!
//! `--flush` makes the follower sync its own disk before confirming. Off by
//! default, and the reason is the whole point of replicating: what the leader is
//! buying is a copy in the memory of a machine that will not lose power at the
//! same moment. Making each follower wait on its own platter hands back exactly
//! the cost the design set out to avoid. Turn it on when the replicas share a
//! power domain, because then the quorum is not independent and memory copies can
//! all vanish together.
//!
//! A follower serves one leader at a time and waits for another when that one
//! goes. It refuses a leader whose term is older than the highest it has seen,
//! which is what stops a replaced leader writing over a newer one's data.

use bx_journal::{FileLog, LogStorage, MemoryLog, Replica};
use std::net::TcpListener;
use std::path::Path;

fn serve<L: LogStorage>(listener: &TcpListener, log: L, flush: bool) -> std::io::Result<()> {
    let mut replica = Replica::new(log, flush);
    println!(
        "replica listening {}, flush before confirming: {flush}",
        listener.local_addr()?
    );
    loop {
        match replica.serve_one(listener) {
            // A leader disconnected. Another may take over, and the highest term
            // seen is remembered so a replaced one cannot come back.
            Ok(()) => println!(
                "leader disconnected; holding {} bytes, highest term {}",
                replica.held(),
                replica.highest_term()
            ),
            Err(e) => {
                // A refused leader or a bad group is worth reporting and worth
                // surviving: the next leader may be the legitimate one.
                eprintln!("leader rejected: {e}");
            }
        }
    }
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let address = args
        .first()
        .filter(|a| !a.starts_with("--"))
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:7100".to_string());
    let flush = args.iter().any(|a| a == "--flush");
    let file = args
        .iter()
        .position(|a| a == "--file")
        .and_then(|i| args.get(i + 1));

    let listener = TcpListener::bind(&address)?;
    match file {
        Some(path) => {
            let log =
                FileLog::open(Path::new(path)).map_err(|e| std::io::Error::other(e.to_string()))?;
            serve(&listener, log, flush)
        }
        // Without a file the follower holds the group in memory only, which is
        // what a quorum actually buys and what the throughput figures measure.
        None => serve(&listener, MemoryLog::new(), flush),
    }
}
