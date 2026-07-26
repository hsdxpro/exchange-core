//! The venue, as a process.
//!
//! Run it, point clients at it. This is the same `Server` the integration tests
//! drive, with nothing added for production and nothing removed for testing —
//! which is the point of having it: a multi-process test exercises exactly the
//! binary a deployment would run.
//!
//! ```text
//! venue [address] [--file PATH]
//! ```
//!
//! With `--file` the journal is a real file and the venue survives a restart.
//! Without it the journal is in memory, which is what a throughput measurement
//! wants: an in-memory log measures the venue, a real file measures the disk.

use bx_gateway::tcp::Server;
use bx_journal::{FileLog, LogStorage, MemoryLog};
use bx_pipeline::instrument::{Instrument, Instruments};
use std::path::PathBuf;

const BTC: u32 = 1;
const USD: u32 = 2;
const SYMBOL: u32 = 1;
const FLOOR: i64 = 10_000;
const MAX_QUANTITY: u64 = 1_000_000;
const MAX_OPEN_ORDERS: u32 = 1_000_000;
const RETAINED_PER_CHANNEL: usize = 1 << 16;
const MAX_RECORDS_PER_SESSION: usize = 4_096;
/// Accounts credited at startup, so a client can trade without a funding API.
const ACCOUNTS: std::ops::RangeInclusive<u64> = 1..=16;
const STARTING_BALANCE: u64 = u64::MAX / 4;

fn instruments() -> Instruments {
    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(
        SYMBOL,
        BTC,
        USD,
        FLOOR,
        MAX_QUANTITY,
        MAX_OPEN_ORDERS,
    ));
    instruments
}

fn run<S: LogStorage>(address: &str, storage: S, fresh: bool) -> std::io::Result<()> {
    let mut server = Server::bind(
        address,
        storage,
        instruments(),
        RETAINED_PER_CHANNEL,
        MAX_RECORDS_PER_SESSION,
    )?;

    if fresh {
        for account in ACCOUNTS {
            for asset in [USD, BTC] {
                server
                    .venue_mut()
                    .deposit(account, asset, STARTING_BALANCE)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
            }
        }
    } else {
        let replayed = server
            .venue_mut()
            .recover()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        println!("recovered {replayed} commands from the journal");
    }

    // Ready only once the port is bound and state is restored, so a parent
    // process can wait for this line instead of sleeping and hoping.
    println!("listening {}", server.address()?);
    loop {
        if let Err(e) = server.poll() {
            eprintln!("venue stopped: {e}");
            return Err(std::io::Error::other(e.to_string()));
        }
    }
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let address = args
        .first()
        .filter(|a| !a.starts_with("--"))
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:7070".to_string());

    let file = args
        .iter()
        .position(|a| a == "--file")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from);

    match file {
        Some(path) => {
            let fresh = !path.exists();
            let log = FileLog::open(&path).map_err(|e| std::io::Error::other(e.to_string()))?;
            run(&address, log, fresh)
        }
        None => run(&address, MemoryLog::new(), true),
    }
}
