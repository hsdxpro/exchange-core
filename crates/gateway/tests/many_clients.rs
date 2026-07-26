//! Many clients trading at once.
//!
//! Every other socket test drives one to three connections, which never
//! exercises the part of the loop that merges readiness across sessions, shares
//! a pass between them, or keeps their private feeds apart. This one runs
//! sixty-four clients on their own accounts, all sending at the same time, and
//! checks the three things that can go wrong when a pass has to serve everybody:
//!
//! - **Nothing is lost.** Every order from every client is acknowledged.
//! - **Nothing crosses over.** A client's private feed carries only its own
//!   events, however many sessions the pass was juggling.
//! - **Nobody is starved.** Every client gets served, not just the ones whose
//!   sockets happened to be read first.

use bx_gateway::codec::encode;
use bx_gateway::tcp::{Server, read_events};
use bx_journal::MemoryLog;
use bx_pipeline::instrument::{Instrument, Instruments};
use bx_pipeline::{limit_order, subscribe};
use bx_protocol::{ChannelKind, Command, Event, EventKind, Side, Ticks};
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const BTC: u32 = 1;
const USD: u32 = 2;
const SYMBOL: u32 = 1;
const FLOOR: Ticks = 10_000;
const RETAINED: usize = 1 << 16;
const MAX_RECORDS: usize = 256;
const MAX_SESSIONS: usize = 1_024;

/// Accounts, and therefore clients. Each trades as itself.
const CLIENTS: u64 = 64;
const ORDERS_EACH: u64 = 200;

fn instruments() -> Instruments {
    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(SYMBOL, BTC, USD, FLOOR, 1_000_000, 1 << 20));
    instruments
}

struct Running {
    address: String,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Running {
    fn start() -> Self {
        let mut server = Server::bind(
            "127.0.0.1:0",
            MemoryLog::new(),
            instruments(),
            RETAINED,
            MAX_RECORDS,
            MAX_SESSIONS,
        )
        .unwrap();
        for account in 1..=CLIENTS {
            for asset in [USD, BTC] {
                server
                    .venue_mut()
                    .deposit(account, asset, u64::MAX / 4)
                    .unwrap();
            }
        }
        let address = server.address().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                server.poll().expect("the venue failed to commit");
            }
        });
        Self {
            address,
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// One client: connects, subscribes, sends its orders, counts what comes back.
fn client(address: String, account: u64) -> (u64, Vec<Event>) {
    let mut stream = TcpStream::connect(&address).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let mut reader = stream.try_clone().unwrap();

    // Read on another thread: a client that writes without reading deadlocks
    // against its own receive buffer, which would look like the venue stalling.
    let counter = std::thread::spawn(move || {
        let mut mine = Vec::new();
        let mut acknowledged = 0;
        let deadline = Instant::now() + Duration::from_secs(30);
        while acknowledged < ORDERS_EACH && Instant::now() < deadline {
            let mut batch = Vec::new();
            if read_events(&mut reader, 1, &mut batch).is_err() || batch.is_empty() {
                break;
            }
            acknowledged += batch
                .iter()
                .filter(|e| e.kind == EventKind::Received as u8)
                .count() as u64;
            mine.extend(batch);
        }
        (acknowledged, mine)
    });

    // Each account works its own slice of the ladder, so orders rest rather than
    // crossing and the test is about plumbing rather than matching.
    let base = FLOOR + 1_000 + (account as Ticks) * 500;
    let mut bytes = Vec::new();
    encode(&subscribe(account, SYMBOL, ChannelKind::Book), &mut bytes);
    let commands: Vec<Command> = (0..ORDERS_EACH)
        .map(|i| {
            limit_order(
                account,
                SYMBOL,
                account * 1_000_000 + i,
                Side::Bid,
                base + i as Ticks,
                1,
            )
        })
        .collect();
    for command in &commands {
        encode(command, &mut bytes);
    }
    stream.write_all(&bytes).unwrap();
    stream.flush().unwrap();

    let result = counter.join().unwrap();
    drop(stream);
    result
}

#[test]
fn sixty_four_clients_trading_at_once_all_get_served() {
    let venue = Running::start();

    let handles: Vec<_> = (1..=CLIENTS)
        .map(|account| {
            let address = venue.address.clone();
            std::thread::spawn(move || (account, client(address, account)))
        })
        .collect();

    let mut starved = Vec::new();
    let mut crossed = Vec::new();
    for handle in handles {
        let (account, (acknowledged, events)) = handle.join().expect("a client thread panicked");
        if acknowledged < ORDERS_EACH {
            starved.push((account, acknowledged));
        }
        // A private event carrying somebody else's account is the failure that
        // matters most: it is a data leak, not a dropped message.
        if events
            .iter()
            .any(|e| e.account != 0 && e.account != account)
        {
            crossed.push(account);
        }
    }

    assert!(
        crossed.is_empty(),
        "these clients received another account's private events: {crossed:?}"
    );
    assert!(
        starved.is_empty(),
        "these clients did not get every order acknowledged: {starved:?}"
    );
}
