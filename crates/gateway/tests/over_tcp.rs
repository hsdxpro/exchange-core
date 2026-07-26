//! End-to-end over real sockets.
//!
//! A server on a real TCP port, clients on real connections, orders encoded as
//! bytes and events decoded from bytes. Nothing is stubbed: the same framing,
//! the same group-commit loop, and the same fan-out a deployed venue would run.
//!
//! The server runs on its own thread and the clients on the test thread, so a
//! deadlock or a lost wakeup shows up as a test timeout rather than as a pass.

use bx_gateway::codec::encode;
use bx_gateway::tcp::{Server, read_events};
use bx_journal::MemoryLog;
use bx_pipeline::instrument::{Instrument, Instruments};
use bx_pipeline::{
    cancel_on_disconnect, limit_order, market_order, query_open_orders, subscribe, unsubscribe,
};
use bx_protocol::{ChannelKind, Command, Event, EventKind, Side, Ticks};
use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const BTC: u32 = 1;
const USD: u32 = 2;
const SYMBOL: u32 = 1;
const FLOOR: Ticks = 10_000;
/// Retention window. Sized so the burst test below stays inside it when a pass
/// is bounded (a few hundred events at a time) and would blow straight through
/// it if a pass were unbounded, which is the regression being guarded.
const RETAINED: usize = 32_768;
const MAX_SESSIONS: usize = 1_024;
const MAX_RECORDS: usize = 256;

fn instruments() -> Instruments {
    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(SYMBOL, BTC, USD, FLOOR, 1_000_000, 65_536));
    instruments
}

/// A running venue, and the handle that stops it.
struct Running {
    address: String,
    stop: Arc<AtomicBool>,
    /// Sessions the server held at its last pass. Only a hint: the test thread
    /// reads it while the server thread writes it.
    sessions: Arc<AtomicUsize>,
    /// Entries in the channel-to-session index, so a test can watch it for a
    /// leak that produces no wrong events and would otherwise be invisible.
    subscriptions: Arc<AtomicUsize>,
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
        // Funded well beyond anything these tests spend. An underfunded account
        // rejects for insufficient balance part way through a run, which looks
        // exactly like the venue dropping commands.
        for account in 1..=4 {
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
        let sessions = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&sessions);
        let subscriptions = Arc::new(AtomicUsize::new(0));
        let tracked = Arc::clone(&subscriptions);

        let thread = std::thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                server.poll().expect("the venue failed to commit");
                counter.store(server.sessions(), Ordering::Relaxed);
                tracked.store(server.tracked_subscriptions(), Ordering::Relaxed);
                // Nothing to do until a socket is readable. A deployed venue
                // busy-polls a pinned core here; a test does not need to.
                std::thread::sleep(Duration::from_micros(200));
            }
        });

        Self {
            address,
            stop,
            sessions,
            subscriptions,
            thread: Some(thread),
        }
    }

    fn sessions_hint(&self) -> usize {
        self.sessions.load(Ordering::Relaxed)
    }

    /// Entries in the venue's channel-to-session index. Same caveat as
    /// `sessions_hint`: read from this thread while the server writes it.
    fn tracked_subscriptions(&self) -> usize {
        self.subscriptions.load(Ordering::Relaxed)
    }

    fn connect(&self) -> TcpStream {
        let stream = TcpStream::connect(&self.address).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        stream
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

/// Collects events until `enough` is satisfied or the window closes.
///
/// Reading an exact count means guessing how many events a command produces,
/// and a wrong guess blocks until the socket times out and then reports a
/// failure that has nothing to do with the venue. Three tests were wrong that
/// way before this existed. A short read timeout means "nothing more for now"
/// rather than an error.
fn collect_until(
    stream: &mut TcpStream,
    window: Duration,
    enough: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .unwrap();
    let mut seen = Vec::new();
    let deadline = Instant::now() + window;
    while Instant::now() < deadline && !enough(&seen) {
        let mut batch = Vec::new();
        match read_events(stream, 1, &mut batch) {
            Ok(()) if batch.is_empty() => break, // peer closed
            Ok(()) => seen.extend(batch),
            Err(_) => {} // nothing waiting; keep trying until the deadline
        }
    }
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    seen
}

fn has_kind(events: &[Event], kind: EventKind) -> bool {
    events.iter().any(|e| e.kind == kind as u8)
}

/// Asks for the public feeds. Sessions get their own private feed for free but
/// have to ask for anything public, so every test that watches the book or the
/// tape starts here.
fn watch_public(stream: &mut TcpStream, account: u64) {
    send(
        stream,
        &[
            subscribe(account, SYMBOL, ChannelKind::Book),
            subscribe(account, SYMBOL, ChannelKind::Trades),
        ],
    );
}

fn send(stream: &mut TcpStream, commands: &[Command]) {
    let mut bytes = Vec::new();
    for command in commands {
        encode(command, &mut bytes);
    }
    stream.write_all(&bytes).unwrap();
    stream.flush().unwrap();
}

#[test]
fn orders_sent_over_a_socket_trade_and_come_back_as_events() {
    let venue = Running::start();
    let mut trader = venue.connect();
    watch_public(&mut trader, 1);

    send(
        &mut trader,
        &[
            limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5),
            limit_order(1, SYMBOL, 102, Side::Bid, 10_090, 3),
        ],
    );

    // Its own acknowledgements arrive too; the depth updates are what matter.
    let events = collect_until(&mut trader, Duration::from_secs(5), |seen| {
        seen.iter()
            .filter(|e| e.kind == EventKind::BookDelta as u8)
            .count()
            >= 2
    });
    let events: Vec<_> = events
        .into_iter()
        .filter(|e| e.kind == EventKind::BookDelta as u8)
        .collect();
    assert_eq!(events.len(), 2, "expected two depth updates");
    let prices: Vec<Ticks> = events.iter().map(|e| e.price).collect();
    assert!(prices.contains(&10_100) && prices.contains(&10_090));
}

#[test]
fn a_second_client_sees_the_public_feed_of_the_first_clients_trading() {
    let venue = Running::start();
    let mut maker = venue.connect();
    // The watcher subscribes before anything trades.
    let mut watcher = venue.connect();
    watch_public(&mut watcher, 4);

    send(
        &mut maker,
        &[limit_order(1, SYMBOL, 101, Side::Ask, 10_100, 5)],
    );
    // Give the maker's order time to rest before the taker crosses it.
    std::thread::sleep(Duration::from_millis(50));
    send(&mut maker, &[market_order(2, SYMBOL, 201, Side::Bid, 5)]);

    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let mut batch = Vec::new();
        read_events(&mut watcher, 1, &mut batch).unwrap();
        seen.extend(batch);
        if seen.iter().any(|e| e.kind == EventKind::Trade as u8) {
            break;
        }
    }

    let trades: Vec<_> = seen
        .iter()
        .filter(|e| e.kind == EventKind::Trade as u8)
        .collect();
    assert!(!trades.is_empty(), "the watcher never saw the print");
    assert_eq!((trades[0].price, trades[0].quantity), (10_100, 5));
    assert_eq!(trades[0].account, 0, "the public tape leaked an account");
}

#[test]
fn a_record_split_across_two_writes_is_still_understood() {
    let venue = Running::start();
    let mut trader = venue.connect();
    watch_public(&mut trader, 1);

    let mut bytes = Vec::new();
    encode(
        &limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5),
        &mut bytes,
    );
    // Deliberately tear the record in half, with a pause between the pieces.
    trader.write_all(&bytes[..30]).unwrap();
    trader.flush().unwrap();
    std::thread::sleep(Duration::from_millis(30));
    trader.write_all(&bytes[30..]).unwrap();
    trader.flush().unwrap();

    let events = collect_until(&mut trader, Duration::from_secs(5), |seen| {
        has_kind(seen, EventKind::BookDelta)
    });
    let delta = events
        .iter()
        .find(|e| e.kind == EventKind::BookDelta as u8)
        .expect("the torn record never reached the book");
    assert_eq!(delta.price, 10_100);
}

#[test]
fn many_orders_pushed_at_once_are_applied_as_one_group_and_all_arrive() {
    const ORDERS: usize = 200;
    let venue = Running::start();
    let mut trader = venue.connect();
    watch_public(&mut trader, 1);

    let commands: Vec<Command> = (0..ORDERS)
        .map(|i| limit_order(1, SYMBOL, 100 + i as u64, Side::Bid, 10_000 + i as Ticks, 1))
        .collect();
    send(&mut trader, &commands);

    let events = collect_until(&mut trader, Duration::from_secs(20), |seen| {
        seen.iter()
            .filter(|e| e.kind == EventKind::BookDelta as u8)
            .count()
            >= ORDERS
    });
    let deltas = events
        .iter()
        .filter(|e| e.kind == EventKind::BookDelta as u8)
        .count();
    assert_eq!(
        deltas, ORDERS,
        "a command was lost between the socket and the book"
    );
}

#[test]
fn a_burst_larger_than_the_retention_window_does_not_drop_the_client() {
    // The venue used to read a socket until it blocked, so one client pushing a
    // large write handed it a group big enough to wrap the subscription rings
    // inside a single pass: 8,192 orders produce about 24,000 events against a
    // 32,768 window, which one unbounded pass overruns and bounded passes of a
    // few hundred never approach. Everyone was then dropped for lagging, including
    // clients that had not been sent anything yet. A pass is now bounded, so
    // the feed keeps up with the book however hard one client writes.
    const ORDERS: usize = 8_192;
    let venue = Running::start();
    let mut writer = venue.connect();
    let mut reader = writer.try_clone().unwrap();

    // Read on another thread. A client that writes half a megabyte without
    // reading deadlocks against its own receive buffer, which is a bug in the
    // client and would hide the one being tested for.
    let counter = std::thread::spawn(move || {
        let mut acknowledged = 0;
        let deadline = Instant::now() + Duration::from_secs(30);
        while acknowledged < ORDERS && Instant::now() < deadline {
            let mut batch = Vec::new();
            if read_events(&mut reader, 1, &mut batch).is_err() || batch.is_empty() {
                break;
            }
            acknowledged += batch
                .iter()
                .filter(|e| e.kind == EventKind::Received as u8)
                .count();
        }
        acknowledged
    });

    let commands: Vec<Command> = (0..ORDERS)
        .map(|i| {
            limit_order(
                1,
                SYMBOL,
                100 + i as u64,
                Side::Bid,
                FLOOR + 1_000 + (i % 4_000) as Ticks,
                1,
            )
        })
        .collect();
    send(&mut writer, &commands);

    assert_eq!(
        counter.join().unwrap(),
        ORDERS,
        "the venue dropped a client that was reading as fast as it could"
    );
}

#[test]
fn a_client_that_never_reads_is_dropped_instead_of_growing_the_venue() {
    // The venue queues what a socket will not yet take. A client that connects,
    // gets subscribed, and then never reads would otherwise make that queue grow
    // without limit -- one session able to exhaust the whole process. The bound
    // is a full retention window, past which the client could not be caught up
    // anyway, so it is shed for the same reason a lagging one is.
    let venue = Running::start();

    // A silent client: connected, attributed an account, then never reading.
    let mut silent = venue.connect();
    send(
        &mut silent,
        &[limit_order(3, SYMBOL, 9_001, Side::Bid, 10_050, 1)],
    );
    // Subscribed to the public feed, so a backlog builds for it.
    watch_public(&mut silent, 3);

    // A well-behaved client generates far more feed than the window holds, and
    // reads the whole time. It must not be shed.
    let mut busy = venue.connect();
    watch_public(&mut busy, 1);
    let mut reader = busy.try_clone().unwrap();
    let writing = Arc::new(AtomicBool::new(true));
    let still_writing = Arc::clone(&writing);
    let drain = std::thread::spawn(move || {
        let mut seen = 0_usize;
        while still_writing.load(Ordering::Relaxed) {
            let mut batch = Vec::new();
            if read_events(&mut reader, 1, &mut batch).is_err() || batch.is_empty() {
                break;
            }
            seen += batch.len();
        }
        seen
    });

    // Enough backlog to pass the bound comfortably: the cap is one retention
    // window in bytes, and each order produces about three events.
    const ROUNDS: u64 = 60;
    for round in 0..ROUNDS {
        let commands: Vec<Command> = (0..1_000)
            .map(|i| {
                limit_order(
                    1,
                    SYMBOL,
                    round * 1_000 + i + 1,
                    Side::Bid,
                    FLOOR + 1_000 + ((round * 1_000 + i) % 4_000) as Ticks,
                    1,
                )
            })
            .collect();
        send(&mut busy, &commands);
    }
    writing.store(false, Ordering::Relaxed);
    let received = drain.join().unwrap();
    assert!(
        received > RETAINED,
        "the reading client only got {received} events, so the venue never \
         produced enough backlog to shed anyone"
    );

    // The silent session is gone; the reading one is not.
    let deadline = Instant::now() + Duration::from_secs(10);
    while venue.sessions_hint() > 1 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        venue.sessions_hint(),
        1,
        "the silent client was not shed, so its queue was unbounded"
    );

    // And the venue still trades.
    let mut fresh = venue.connect();
    send(
        &mut fresh,
        &[limit_order(2, SYMBOL, 8_000_001, Side::Ask, 10_050, 1)],
    );
    let events = collect_until(&mut fresh, Duration::from_secs(5), |seen| !seen.is_empty());
    assert!(
        !events.is_empty(),
        "the venue stopped serving after shedding a silent client"
    );
    drop(silent);
}

#[test]
fn a_client_joining_a_book_that_is_already_trading_can_rebuild_it() {
    // Increments alone cannot build a book: a subscriber has no idea what was
    // resting before it arrived. The venue states the current levels first.
    // Every end-to-end test before this one happened to subscribe while the book
    // was empty, where an empty book plus increments is accidentally correct.
    let venue = Running::start();

    // One client builds a book.
    let mut maker = venue.connect();
    send(
        &mut maker,
        &[
            limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5),
            limit_order(1, SYMBOL, 102, Side::Bid, 10_090, 3),
            limit_order(1, SYMBOL, 103, Side::Ask, 10_200, 7),
        ],
    );
    std::thread::sleep(Duration::from_millis(80));

    // A second client arrives afterwards and asks for the book.
    let mut latecomer = venue.connect();
    watch_public(&mut latecomer, 4);

    let events = collect_until(&mut latecomer, Duration::from_secs(5), |seen| {
        seen.iter()
            .filter(|e| e.kind == EventKind::BookSnapshot as u8)
            .count()
            >= 3
    });
    let state: Vec<(u8, Ticks, u64)> = events
        .iter()
        .filter(|e| e.kind == EventKind::BookSnapshot as u8)
        .map(|e| (e.side, e.price, e.quantity))
        .collect();

    assert_eq!(
        state.len(),
        3,
        "the venue did not state the book it already had: {events:?}"
    );
    assert!(state.contains(&(Side::Bid as u8, 10_100, 5)));
    assert!(state.contains(&(Side::Bid as u8, 10_090, 3)));
    assert!(state.contains(&(Side::Ask as u8, 10_200, 7)));

    // And increments follow from the same position, so the two compose.
    send(
        &mut maker,
        &[limit_order(1, SYMBOL, 104, Side::Bid, 10_095, 2)],
    );
    let after = collect_until(&mut latecomer, Duration::from_secs(5), |seen| {
        has_kind(seen, EventKind::BookDelta)
    });
    let delta = after
        .iter()
        .find(|e| e.kind == EventKind::BookDelta as u8)
        .expect("no increment arrived after the snapshot");
    assert_eq!((delta.price, delta.quantity), (10_095, 2));
}

#[test]
fn a_client_following_only_the_top_of_book_gets_it_stated_then_changed() {
    // The cheap feed, over a real socket. Two things have to hold: a subscriber
    // is told where the market is before it is told how it moved, and an order
    // that lands behind the touch does not reach it at all.
    let venue = Running::start();

    let mut maker = venue.connect();
    send(
        &mut maker,
        &[
            limit_order(1, SYMBOL, 201, Side::Bid, 10_100, 5),
            limit_order(1, SYMBOL, 202, Side::Ask, 10_200, 7),
        ],
    );
    std::thread::sleep(Duration::from_millis(80));

    let mut watcher = venue.connect();
    send(
        &mut watcher,
        &[
            limit_order(4, SYMBOL, 299, Side::Bid, 10_000, 1),
            subscribe(4, SYMBOL, ChannelKind::Bbo),
        ],
    );
    let stated = collect_until(&mut watcher, Duration::from_secs(5), |seen| {
        seen.iter()
            .filter(|e| e.kind == EventKind::Bbo as u8)
            .count()
            >= 2
    });
    let tops: Vec<(u8, Ticks, u64)> = stated
        .iter()
        .filter(|e| e.kind == EventKind::Bbo as u8)
        .map(|e| (e.side, e.price, e.quantity))
        .collect();
    assert!(
        tops.contains(&(Side::Bid as u8, 10_100, 5))
            && tops.contains(&(Side::Ask as u8, 10_200, 7)),
        "the venue did not state the market it already had: {tops:?}"
    );

    // Deep behind the touch: the depth feed would carry this, and this one must
    // not.
    send(
        &mut maker,
        &[limit_order(1, SYMBOL, 203, Side::Bid, 10_050, 9)],
    );
    std::thread::sleep(Duration::from_millis(150));
    // Then something that does move it, so the absence above is a real silence
    // rather than a feed that had simply not caught up.
    send(
        &mut maker,
        &[limit_order(1, SYMBOL, 204, Side::Bid, 10_150, 2)],
    );

    let after = collect_until(&mut watcher, Duration::from_secs(5), |seen| {
        seen.iter()
            .any(|e| e.kind == EventKind::Bbo as u8 && e.price == 10_150)
    });
    let moves: Vec<(u8, Ticks, u64)> = after
        .iter()
        .filter(|e| e.kind == EventKind::Bbo as u8)
        .map(|e| (e.side, e.price, e.quantity))
        .collect();
    assert_eq!(
        moves,
        vec![(Side::Bid as u8, 10_150, 2)],
        "an order behind the touch reached the top-of-book feed"
    );
}

#[test]
fn a_client_shed_for_being_slow_can_reconnect_and_rebuild_the_book() {
    // This is the recovery story end to end. A client too slow to read is shed,
    // because the alternative is queueing for it without limit. What makes that
    // survivable is that reconnecting restates the book, so the client comes back
    // with a correct picture rather than a stream of increments against a book it
    // no longer knows.
    let venue = Running::start();

    // A silent client: subscribed, never reading.
    let mut silent = venue.connect();
    send(
        &mut silent,
        &[limit_order(3, SYMBOL, 7_001, Side::Bid, 10_050, 1)],
    );
    watch_public(&mut silent, 3);

    // A maker that reads, generating far more feed than the silent client's
    // queue is allowed to hold.
    let mut maker = venue.connect();
    let mut maker_reader = maker.try_clone().unwrap();
    let sending = Arc::new(AtomicBool::new(true));
    let still_sending = Arc::clone(&sending);
    let drain = std::thread::spawn(move || {
        let mut scratch = Vec::new();
        while still_sending.load(Ordering::Relaxed) {
            scratch.clear();
            if read_events(&mut maker_reader, 1, &mut scratch).is_err() || scratch.is_empty() {
                break;
            }
        }
    });

    for round in 0..80_u64 {
        let commands: Vec<Command> = (0..500)
            .map(|i| {
                limit_order(
                    1,
                    SYMBOL,
                    round * 500 + i + 1,
                    Side::Bid,
                    FLOOR + 1_000 + ((round * 500 + i) % 4_000) as Ticks,
                    1,
                )
            })
            .collect();
        send(&mut maker, &commands);
        std::thread::sleep(Duration::from_millis(2));
    }
    sending.store(false, Ordering::Relaxed);
    let _ = drain.join();

    // The silent one is gone; the venue did not queue for it forever.
    let deadline = Instant::now() + Duration::from_secs(10);
    while venue.sessions_hint() > 1 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        venue.sessions_hint() <= 1,
        "the slow client was never shed, so its queue was unbounded"
    );
    drop(silent);

    // It comes back, and is told the book as it now stands.
    let mut returning = venue.connect();
    watch_public(&mut returning, 3);
    let events = collect_until(&mut returning, Duration::from_secs(10), |seen| {
        seen.iter()
            .filter(|e| e.kind == EventKind::BookSnapshot as u8)
            .count()
            > 100
    });
    let levels = events
        .iter()
        .filter(|e| e.kind == EventKind::BookSnapshot as u8)
        .count();
    assert!(
        levels > 100,
        "a reconnecting client got {levels} levels of book state, so it cannot          rebuild what it missed"
    );
    assert!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::BookSnapshot as u8)
            .all(|e| e.quantity > 0),
        "an empty level was sent as state"
    );
}

/// Asks the venue what `account` still has working, on a fresh connection.
///
/// Drained rather than stopped at the first reply: the count is the answer, and a
/// predicate that stops early truncates a reply of several events to one.
fn working_orders(venue: &Running, account: u64) -> Vec<u64> {
    let mut client = venue.connect();
    send(&mut client, &[query_open_orders(account, SYMBOL)]);
    collect_until(&mut client, Duration::from_millis(600), |_| false)
        .iter()
        .filter(|e| e.kind == EventKind::OrderState as u8)
        .map(|e| e.order_id)
        .collect()
}

#[test]
fn a_reconnecting_client_can_recover_the_orders_it_still_has_working() {
    // A book can be rebuilt from a snapshot; a client's own orders cannot, and
    // that is the one thing a trader must know before acting again.
    let venue = Running::start();
    {
        let mut before = venue.connect();
        send(
            &mut before,
            &[
                limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5),
                limit_order(1, SYMBOL, 102, Side::Bid, 10_090, 7),
                limit_order(1, SYMBOL, 103, Side::Ask, 10_400, 2),
            ],
        );
        collect_until(&mut before, Duration::from_secs(5), |seen| {
            seen.iter()
                .filter(|e| e.kind == EventKind::Resting as u8)
                .count()
                >= 3
        });
    } // gone, three orders still working

    // Someone else takes one of them while it is away.
    let mut other = venue.connect();
    send(
        &mut other,
        &[limit_order(2, SYMBOL, 201, Side::Ask, 10_100, 5)],
    );
    std::thread::sleep(Duration::from_millis(150));

    let working = working_orders(&venue, 1);
    assert!(
        !working.contains(&101),
        "an order that was filled while away was reported as working: {working:?}"
    );
    assert!(working.contains(&102), "{working:?}");
    assert!(working.contains(&103), "{working:?}");
}

#[test]
fn a_departed_session_is_forgotten_rather_than_held_forever() {
    // Sessions that are never freed are a leak per disconnect, and nothing
    // notices unless something counts them.
    let venue = Running::start();
    for _ in 0..4 {
        let mut client = venue.connect();
        send(
            &mut client,
            &[limit_order(5, SYMBOL, 1, Side::Bid, 10_010, 1)],
        );
        collect_until(&mut client, Duration::from_secs(5), |seen| !seen.is_empty());
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    while venue.sessions_hint() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        venue.sessions_hint(),
        0,
        "connections that closed are still being held"
    );
}

#[test]
fn without_cancel_on_disconnect_orders_survive_the_connection() {
    // The default. A retail client that loses its connection expects to come back
    // to the order it left.
    let venue = Running::start();
    {
        let mut client = venue.connect();
        send(
            &mut client,
            &[limit_order(3, SYMBOL, 601, Side::Bid, 10_120, 4)],
        );
        collect_until(&mut client, Duration::from_secs(5), |seen| {
            seen.iter().any(|e| e.kind == EventKind::Resting as u8)
        });
    }
    std::thread::sleep(Duration::from_millis(150));

    assert!(
        working_orders(&venue, 3).contains(&601),
        "an order was cancelled without being asked for"
    );
}

#[test]
fn turning_cancel_on_disconnect_back_off_takes_effect() {
    let venue = Running::start();
    {
        let mut client = venue.connect();
        send(
            &mut client,
            &[
                cancel_on_disconnect(4, true),
                limit_order(4, SYMBOL, 701, Side::Bid, 10_140, 3),
                cancel_on_disconnect(4, false),
            ],
        );
        collect_until(&mut client, Duration::from_secs(5), |seen| {
            seen.iter().any(|e| e.kind == EventKind::Resting as u8)
        });
    }
    std::thread::sleep(Duration::from_millis(150));

    assert!(
        working_orders(&venue, 4).contains(&701),
        "the order was cancelled after the setting was turned off"
    );
}

#[test]
fn cancel_on_disconnect_withdraws_every_quote_not_just_one() {
    let venue = Running::start();
    {
        let mut maker = venue.connect();
        send(
            &mut maker,
            &[
                cancel_on_disconnect(1, true),
                limit_order(1, SYMBOL, 501, Side::Bid, 10_100, 5),
                limit_order(1, SYMBOL, 502, Side::Ask, 10_300, 5),
                limit_order(1, SYMBOL, 503, Side::Bid, 10_050, 5),
            ],
        );
        collect_until(&mut maker, Duration::from_secs(5), |seen| {
            seen.iter()
                .filter(|e| e.kind == EventKind::Resting as u8)
                .count()
                >= 3
        });
    }
    std::thread::sleep(Duration::from_millis(200));

    let mut asking = venue.connect();
    send(&mut asking, &[query_open_orders(1, SYMBOL)]);
    // Drained rather than stopped at the first event: the count is the answer,
    // and a predicate that stops early would truncate a reply of several to one.
    let events = collect_until(&mut asking, Duration::from_millis(600), |_| false);
    let working: Vec<u64> = events
        .iter()
        .filter(|e| e.kind == EventKind::OrderState as u8)
        .map(|e| e.order_id)
        .collect();
    assert!(
        working.is_empty(),
        "quotes outlived the connection managing them: {working:?}"
    );
}

#[test]
fn a_channel_nobody_asked_for_is_never_delivered() {
    // A session gets its own acknowledgements and nothing else until it asks.
    // At a thousand instruments, handing every session every book would
    // multiply outbound traffic by the number of instruments nobody wanted.
    let venue = Running::start();
    let mut quiet = venue.connect();
    send(
        &mut quiet,
        &[limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5)],
    );

    let events = collect_until(&mut quiet, Duration::from_secs(2), |seen| seen.len() >= 2);
    assert!(
        !events.is_empty(),
        "the session did not even get its own acknowledgements"
    );
    assert!(
        events.iter().all(|e| e.account == 1),
        "an unsubscribed public feed was delivered: {events:?}"
    );
    assert!(
        events.iter().all(|e| e.kind != EventKind::BookDelta as u8),
        "depth arrived without being asked for"
    );
}

#[test]
fn unsubscribing_stops_a_feed_the_session_was_receiving() {
    let venue = Running::start();
    let mut trader = venue.connect();
    watch_public(&mut trader, 1);
    send(
        &mut trader,
        &[limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5)],
    );

    // Depth arrives while subscribed.
    let events = collect_until(&mut trader, Duration::from_secs(5), |seen| {
        has_kind(seen, EventKind::BookDelta)
    });
    assert!(
        events.iter().any(|e| e.kind == EventKind::BookDelta as u8),
        "never received the feed in the first place"
    );

    send(
        &mut trader,
        &[
            unsubscribe(1, SYMBOL, ChannelKind::Book),
            unsubscribe(1, SYMBOL, ChannelKind::Trades),
        ],
    );
    // Drain whatever was already queued before the unsubscribe landed.
    collect_until(&mut trader, Duration::from_millis(300), |_| false);

    send(
        &mut trader,
        &[limit_order(1, SYMBOL, 102, Side::Bid, 10_080, 5)],
    );
    let after = collect_until(&mut trader, Duration::from_secs(2), |seen| {
        has_kind(seen, EventKind::Resting)
    });
    assert!(
        !after.is_empty(),
        "the session stopped receiving its own events too"
    );
    assert!(
        !has_kind(&after, EventKind::BookDelta),
        "depth kept arriving after unsubscribing: {after:?}"
    );
}

#[test]
fn a_client_that_disconnects_does_not_disturb_the_venue() {
    let venue = Running::start();
    {
        let mut leaving = venue.connect();
        watch_public(&mut leaving, 1);
        send(
            &mut leaving,
            &[limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5)],
        );
        let mut events = Vec::new();
        read_events(&mut leaving, 1, &mut events).unwrap();
    } // dropped: the socket closes mid-session

    std::thread::sleep(Duration::from_millis(50));

    // The venue is still trading, and still knows about the departed client's
    // resting order.
    let mut arriving = venue.connect();
    watch_public(&mut arriving, 2);
    send(
        &mut arriving,
        &[limit_order(2, SYMBOL, 201, Side::Ask, 10_100, 5)],
    );

    let events = collect_until(&mut arriving, Duration::from_secs(5), |seen| {
        has_kind(seen, EventKind::Trade) || has_kind(seen, EventKind::BookDelta)
    });
    assert!(
        events
            .iter()
            .any(|e| e.kind == EventKind::BookDelta as u8 || e.kind == EventKind::Trade as u8),
        "the venue stopped working after a disconnect: {events:?}"
    );
}

#[test]
fn unsubscribing_stops_the_feed_and_leaves_nothing_behind() {
    // The venue writes to the sessions following the channels a group touched,
    // which means an index from channel to session. An entry left behind after
    // an unsubscribe is a session written to for a feed it asked to stop, and a
    // slot that is later reused inherits it.
    let venue = Running::start();
    let mut watcher = venue.connect();
    send(
        &mut watcher,
        &[limit_order(4, SYMBOL, 900, Side::Bid, 10_000, 1)],
    );
    watch_public(&mut watcher, 4);

    let mut maker = venue.connect();
    send(
        &mut maker,
        &[limit_order(1, SYMBOL, 301, Side::Bid, 10_100, 1)],
    );
    let seen = collect_until(&mut watcher, Duration::from_secs(5), |seen| {
        has_kind(seen, EventKind::BookDelta)
    });
    assert!(
        has_kind(&seen, EventKind::BookDelta),
        "the watcher never saw the book while subscribed"
    );

    send(&mut watcher, &[unsubscribe(4, SYMBOL, ChannelKind::Book)]);
    std::thread::sleep(Duration::from_millis(100));
    // Drain whatever was already in flight before the unsubscribe landed.
    let _ = collect_until(&mut watcher, Duration::from_millis(300), |_| false);

    send(
        &mut maker,
        &[limit_order(1, SYMBOL, 302, Side::Bid, 10_090, 1)],
    );
    let after = collect_until(&mut watcher, Duration::from_millis(800), |seen| {
        has_kind(seen, EventKind::BookDelta)
    });
    assert!(
        !has_kind(&after, EventKind::BookDelta),
        "a depth update arrived after unsubscribing: {after:?}"
    );
}

#[test]
fn a_session_does_not_inherit_the_feeds_of_the_one_before_it() {
    // Session slots are reused, so a fresh client lands on an index a departed
    // one used. It must hear about its own orders and nothing else.
    //
    // This holds even if the subscriber index still names the old session,
    // because the write pass checks the session actually follows the channel
    // before sending -- which is why the *leak* needs its own test below rather
    // than showing up as a wrong event here.
    let venue = Running::start();
    {
        let mut departing = venue.connect();
        send(
            &mut departing,
            &[limit_order(4, SYMBOL, 910, Side::Bid, 10_000, 1)],
        );
        watch_public(&mut departing, 4);
        let _ = collect_until(&mut departing, Duration::from_secs(2), |seen| {
            !seen.is_empty()
        });
    }
    // Give the venue a pass to notice the socket has gone and free the slot.
    std::thread::sleep(Duration::from_millis(200));

    // A fresh client that asks for nothing public. It should hear about its own
    // orders and not a word about anybody's book.
    let mut arriving = venue.connect();
    send(
        &mut arriving,
        &[limit_order(3, SYMBOL, 920, Side::Bid, 10_000, 1)],
    );
    let mut maker = venue.connect();
    send(
        &mut maker,
        &[limit_order(1, SYMBOL, 303, Side::Bid, 10_150, 5)],
    );

    let seen = collect_until(&mut arriving, Duration::from_secs(1), |seen| {
        has_kind(seen, EventKind::BookDelta)
    });
    assert!(
        !has_kind(&seen, EventKind::BookDelta),
        "a fresh session inherited the departed session's book feed: {seen:?}"
    );
    assert!(
        seen.iter().any(|e| e.order_id == 920),
        "the fresh session did not even get its own acknowledgement"
    );
}

#[test]
fn connection_churn_does_not_grow_the_subscriber_index() {
    // The index from channel to session is what lets the write pass touch only
    // the sessions a group concerns, and it is the thing that has to be given
    // back when a session goes.
    //
    // Getting this wrong produces no wrong events -- the write pass checks the
    // session follows the channel before sending -- so it would never fail a
    // correctness test. It would simply grow, for the life of the process, one
    // entry per subscription per departed client. At a million clients
    // connecting and disconnecting that is a leak found in production.
    let venue = Running::start();
    let mut settled = 0;

    for round in 0..12 {
        {
            let mut client = venue.connect();
            send(
                &mut client,
                &[limit_order(4, SYMBOL, 2_000 + round, Side::Bid, 10_000, 1)],
            );
            watch_public(&mut client, 4);
            let _ = collect_until(&mut client, Duration::from_secs(2), |seen| !seen.is_empty());
        }
        // Long enough for the venue to notice the socket closed and free the slot.
        let deadline = Instant::now() + Duration::from_secs(5);
        while venue.sessions_hint() > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }

        let held = venue.tracked_subscriptions();
        // After the second round the figure must stop moving. It is not asserted
        // to be zero: a churn test races the venue's own bookkeeping, and what
        // matters is that it does not accumulate.
        if round == 1 {
            settled = held;
        } else if round > 1 {
            assert!(
                held <= settled,
                "the subscriber index held {held} entries after {} rounds of \
                 connect-and-leave, against {settled} after two, so a departed \
                 session's subscriptions are never given back",
                round + 1
            );
        }
    }
}
