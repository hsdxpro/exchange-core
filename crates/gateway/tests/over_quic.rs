//! End-to-end over real QUIC.
//!
//! The same venue and the same 64-byte records, reached over UDP with TLS 1.3
//! rather than a TCP socket. The property worth testing is the one that justified
//! the transport: order acknowledgements and market data are on separate streams,
//! so a client that stops reading its depth feed still gets its fills. On one TCP
//! connection carrying both, it would not.

use bx_gateway::codec::encode;
use bx_gateway::quic::{ALPN, QuicVenue, read_events, self_signed};
use bx_journal::MemoryLog;
use bx_pipeline::instrument::{Instrument, Instruments};
use bx_pipeline::{cancel_on_disconnect, limit_order, query_open_orders, subscribe};
use bx_protocol::{ChannelKind, Command, Event, EventKind, Side, Ticks};
use quinn::{ClientConfig, Endpoint};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const BTC: u32 = 1;
const USD: u32 = 2;
const SYMBOL: u32 = 1;
const FLOOR: Ticks = 10_000;
const RETAINED: usize = 1 << 14;
const QUEUED: usize = 4_096;

fn instruments() -> Instruments {
    let mut instruments = Instruments::new();
    instruments.insert(Instrument::new(SYMBOL, BTC, USD, FLOOR, 1_000_000, 1 << 20));
    instruments
}

/// A venue on its own thread, and what a client needs to reach it.
struct Running {
    address: SocketAddr,
    certificate: Vec<u8>,
    stop: Arc<AtomicBool>,
    /// Sessions the venue held at its last pass. A hint: the test thread reads it
    /// while the venue thread writes it.
    sessions: Arc<std::sync::atomic::AtomicUsize>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Running {
    fn start() -> Self {
        let (config, certificate) = self_signed("localhost").unwrap();
        let mut venue = QuicVenue::bind(
            "127.0.0.1:0".parse().unwrap(),
            config,
            MemoryLog::new(),
            instruments(),
            RETAINED,
            QUEUED,
        )
        .unwrap();
        for account in 1..=8 {
            for asset in [USD, BTC] {
                venue
                    .venue_mut()
                    .deposit(account, asset, u64::MAX / 4)
                    .unwrap();
            }
        }
        let address = venue.address().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let sessions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&sessions);
        let thread = std::thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                venue
                    .poll(Duration::from_millis(2))
                    .expect("the venue failed to commit");
                counter.store(venue.sessions(), Ordering::Relaxed);
            }
        });
        Self {
            address,
            certificate,
            stop,
            sessions,
            thread: Some(thread),
        }
    }

    fn sessions_hint(&self) -> usize {
        self.sessions.load(Ordering::Relaxed)
    }

    /// A client that trusts exactly this venue's certificate, rather than
    /// disabling verification.
    fn client(&self) -> ClientConfig {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls::pki_types::CertificateDer::from(
                self.certificate.clone(),
            ))
            .unwrap();
        let mut crypto = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        crypto.alpn_protocols = vec![ALPN.to_vec()];
        ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
        ))
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

/// A connected client: the order-entry stream, plus the connection for feeds.
struct Client {
    connection: quinn::Connection,
    orders: quinn::SendStream,
    acks: quinn::RecvStream,
}

async fn connect(venue: &Running) -> Client {
    let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    endpoint.set_default_client_config(venue.client());
    let connection = endpoint
        .connect(venue.address, "localhost")
        .unwrap()
        .await
        .expect("the handshake failed");
    // The client opens order entry; the venue opens a stream per feed.
    let (mut orders, acks) = connection.open_bi().await.unwrap();
    // A stream is not visible to the peer until something is written on it.
    orders.write_all(&[]).await.unwrap();
    Client {
        connection,
        orders,
        acks,
    }
}

async fn send(client: &mut Client, commands: &[Command]) {
    let mut bytes = Vec::new();
    for command in commands {
        encode(command, &mut bytes);
    }
    client.orders.write_all(&bytes).await.unwrap();
}

/// Collects from a stream until `enough` or the window closes.
async fn collect_until(
    stream: &mut quinn::RecvStream,
    window: Duration,
    enough: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    let mut seen = Vec::new();
    let deadline = Instant::now() + window;
    while Instant::now() < deadline && !enough(&seen) {
        let mut batch = Vec::new();
        match tokio::time::timeout(
            Duration::from_millis(100),
            read_events(stream, 1, &mut batch),
        )
        .await
        {
            Ok(Ok(())) if batch.is_empty() => break,
            Ok(Ok(())) => seen.extend(batch),
            Ok(Err(_)) => break,
            Err(_) => {}
        }
    }
    seen
}

/// Collects everything that arrives within `window`, with no early exit.
///
/// For answers whose *length* is what is being measured. `collect_until` stops
/// at the first event satisfying its predicate, which silently truncates a reply
/// of several events to one -- a mistake made three times in these tests before
/// this existed.
async fn drain_for(stream: &mut quinn::RecvStream, window: Duration) -> Vec<Event> {
    collect_until(stream, window, |_| false).await
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn orders_over_quic_trade_and_are_acknowledged() {
    let venue = Running::start();
    runtime().block_on(async {
        let mut client = connect(&venue).await;
        send(
            &mut client,
            &[
                limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5),
                limit_order(2, SYMBOL, 201, Side::Ask, 10_100, 5),
            ],
        )
        .await;

        let events = collect_until(&mut client.acks, Duration::from_secs(10), |seen| {
            seen.iter()
                .filter(|e| e.kind == EventKind::Received as u8)
                .count()
                >= 1
        })
        .await;
        assert!(
            events.iter().any(|e| e.kind == EventKind::Received as u8),
            "no acknowledgement arrived over QUIC: {events:?}"
        );
        // The session's own feed carries its own account and nobody else's.
        assert!(events.iter().all(|e| e.account == 1 || e.account == 0));
    });
}

#[test]
fn a_book_is_stated_before_its_increments() {
    let venue = Running::start();
    runtime().block_on(async {
        let mut maker = connect(&venue).await;
        send(
            &mut maker,
            &[
                limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5),
                limit_order(1, SYMBOL, 102, Side::Ask, 10_200, 3),
            ],
        )
        .await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // A latecomer asks for the book and must be told what is already there.
        let mut watcher = connect(&venue).await;
        send(
            &mut watcher,
            &[
                limit_order(3, SYMBOL, 301, Side::Bid, 10_000, 1),
                subscribe(3, SYMBOL, ChannelKind::Book),
            ],
        )
        .await;

        let mut feed = watcher
            .connection
            .accept_uni()
            .await
            .expect("the venue never opened a feed stream");
        let events = collect_until(&mut feed, Duration::from_secs(10), |seen| {
            seen.iter()
                .filter(|e| e.kind == EventKind::BookSnapshot as u8)
                .count()
                >= 2
        })
        .await;

        let state: Vec<(u8, Ticks, u64)> = events
            .iter()
            .filter(|e| e.kind == EventKind::BookSnapshot as u8)
            .map(|e| (e.side, e.price, e.quantity))
            .collect();
        assert!(
            state.contains(&(Side::Bid as u8, 10_100, 5)),
            "the resting bid was not stated: {state:?}"
        );
        assert!(
            state.contains(&(Side::Ask as u8, 10_200, 3)),
            "the resting ask was not stated: {state:?}"
        );
    });
}

#[test]
fn a_client_that_disconnects_reconnects_and_rebuilds_what_it_missed() {
    // The disconnect/reconnect path end to end. A client trades, drops entirely,
    // the venue keeps trading without it, and it comes back to a book it can
    // reconstruct -- which needs the state, not just the changes since it left.
    let venue = Running::start();
    runtime().block_on(async {
        {
            let mut early = connect(&venue).await;
            send(
                &mut early,
                &[limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5)],
            )
            .await;
            collect_until(&mut early.acks, Duration::from_secs(5), |seen| {
                !seen.is_empty()
            })
            .await;
            // Connection dropped: no close handshake, just gone.
        }

        // The venue carries on, and the book moves while nobody is watching.
        let mut other = connect(&venue).await;
        send(
            &mut other,
            &[
                limit_order(2, SYMBOL, 201, Side::Bid, 10_090, 4),
                limit_order(2, SYMBOL, 202, Side::Ask, 10_300, 6),
            ],
        )
        .await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Back again, on a fresh connection, asking for the book.
        let mut returning = connect(&venue).await;
        send(
            &mut returning,
            &[
                limit_order(1, SYMBOL, 999, Side::Bid, 10_000, 1),
                subscribe(1, SYMBOL, ChannelKind::Book),
            ],
        )
        .await;
        let mut feed = returning
            .connection
            .accept_uni()
            .await
            .expect("no feed stream after reconnecting");
        // Collected once, until the first increment arrives. The snapshot is sent
        // before any increment, so a BookDelta means the state is complete --
        // and collecting twice would have the first pass drain the increment the
        // second is waiting for.
        let events = collect_until(&mut feed, Duration::from_secs(10), |seen| {
            seen.iter().any(|e| e.kind == EventKind::BookDelta as u8)
        })
        .await;
        let state: Vec<(u8, Ticks, u64)> = events
            .iter()
            .filter(|e| e.kind == EventKind::BookSnapshot as u8)
            .map(|e| (e.side, e.price, e.quantity))
            .collect();

        // Everything that was resting when it subscribed, including what moved
        // while it was away and its own order from before the disconnect.
        assert!(state.contains(&(Side::Bid as u8, 10_100, 5)), "{state:?}");
        assert!(state.contains(&(Side::Bid as u8, 10_090, 4)), "{state:?}");
        assert!(state.contains(&(Side::Ask as u8, 10_300, 6)), "{state:?}");

        // Its own new order was in the same batch as the subscription, so it
        // belongs *after* the snapshot: state is taken at the sequence the
        // increments resume from, and anything later arrives as an increment.
        // The split between state and change is exact rather than approximate.
        assert!(
            !state.contains(&(Side::Bid as u8, 10_000, 1)),
            "an order placed after the snapshot point appeared inside it: {state:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| e.kind == EventKind::BookDelta as u8 && e.price == 10_000),
            "the order placed after the snapshot never arrived as an increment"
        );
    });
}

#[test]
fn a_reconnecting_client_can_recover_the_orders_it_still_has_working() {
    // A book can be rebuilt from a snapshot; a client's own orders cannot. Until
    // it can ask, a trader that has just reconnected does not know what it still
    // has in the market, which is the one thing it must know before acting.
    let venue = Running::start();
    runtime().block_on(async {
        {
            let mut before = connect(&venue).await;
            send(
                &mut before,
                &[
                    limit_order(1, SYMBOL, 101, Side::Bid, 10_100, 5),
                    limit_order(1, SYMBOL, 102, Side::Bid, 10_090, 7),
                    limit_order(1, SYMBOL, 103, Side::Ask, 10_400, 2),
                ],
            )
            .await;
            collect_until(&mut before.acks, Duration::from_secs(5), |seen| {
                seen.iter()
                    .filter(|e| e.kind == EventKind::Resting as u8)
                    .count()
                    >= 3
            })
            .await;
        } // gone, with three orders still working

        // Someone else trades one of them away while it is disconnected.
        let mut other = connect(&venue).await;
        send(
            &mut other,
            &[limit_order(2, SYMBOL, 201, Side::Ask, 10_100, 5)],
        )
        .await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Back, and asking.
        let mut returning = connect(&venue).await;
        send(
            &mut returning,
            &[
                limit_order(1, SYMBOL, 104, Side::Bid, 10_000, 1),
                query_open_orders(1, SYMBOL),
            ],
        )
        .await;
        let events = collect_until(&mut returning.acks, Duration::from_secs(10), |seen| {
            seen.iter()
                .filter(|e| e.kind == EventKind::OrderState as u8)
                .count()
                >= 3
        })
        .await;

        let working: Vec<(u64, Ticks, u64)> = events
            .iter()
            .filter(|e| e.kind == EventKind::OrderState as u8)
            .map(|e| (e.order_id, e.price, e.quantity))
            .collect();

        // 101 was fully taken while it was away and must not be reported.
        assert!(
            !working.iter().any(|(order, _, _)| *order == 101),
            "an order that was already filled was reported as working: {working:?}"
        );
        // The two that survived, with what is still working rather than what was
        // originally sent.
        assert!(working.contains(&(102, 10_090, 7)), "{working:?}");
        assert!(working.contains(&(103, 10_400, 2)), "{working:?}");
        // Not the one it placed in the same batch as the query. Session control is
        // answered where it sits in the stream, so 104 is applied after and
        // arrives as a Resting event -- the same composition as a book snapshot
        // and its increments. The split is exact rather than approximate.
        assert!(
            !working.iter().any(|(order, _, _)| *order == 104),
            "an order placed after the query appeared in its answer: {working:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| e.kind == EventKind::Resting as u8 && e.order_id == 104),
            "the order placed after the query never arrived as an event"
        );
        assert!(
            events
                .iter()
                .filter(|e| e.kind == EventKind::OrderState as u8)
                .all(|e| e.account == 1),
            "another account's orders were reported"
        );
    });
}

/// Asks the venue what `account` still has working, on a fresh connection.
///
/// The query alone, with no order alongside it: the account is attributed from
/// the query itself, and anything else in the batch would land after the answer.
async fn working_orders(venue: &Running, account: u64) -> Vec<u64> {
    let mut client = connect(venue).await;
    send(&mut client, &[query_open_orders(account, SYMBOL)]).await;
    // Drained rather than stopped at the first: the count is the answer.
    let events = drain_for(&mut client.acks, Duration::from_millis(600)).await;
    events
        .iter()
        .filter(|e| e.kind == EventKind::OrderState as u8)
        .map(|e| e.order_id)
        .collect()
}

#[test]
fn a_departed_session_is_forgotten_rather_than_held_forever() {
    // A session is freed only when the venue is told the peer has gone, and the
    // stream writer used to be parked waiting for the venue to free it -- each
    // waiting on the other, so a disconnected client stayed in the map for good.
    // Nothing else noticed, because nothing else counted.
    let venue = Running::start();
    runtime().block_on(async {
        for _ in 0..4 {
            let mut client = connect(&venue).await;
            send(
                &mut client,
                &[limit_order(5, SYMBOL, 1, Side::Bid, 10_010, 1)],
            )
            .await;
            collect_until(&mut client.acks, Duration::from_secs(5), |seen| {
                !seen.is_empty()
            })
            .await;
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        while venue.sessions_hint() > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            venue.sessions_hint(),
            0,
            "connections that closed are still being held"
        );
    });
}

#[test]
fn cancel_on_disconnect_withdraws_a_market_makers_quotes() {
    // A market maker cannot manage risk it can no longer see, so its quotes must
    // not outlive its connection. Opt-in, because a client holding a limit order
    // for a week wants the opposite.
    let venue = Running::start();
    runtime().block_on(async {
        {
            let mut maker = connect(&venue).await;
            send(
                &mut maker,
                &[
                    cancel_on_disconnect(1, true),
                    limit_order(1, SYMBOL, 501, Side::Bid, 10_100, 5),
                    limit_order(1, SYMBOL, 502, Side::Ask, 10_300, 5),
                ],
            )
            .await;
            collect_until(&mut maker.acks, Duration::from_secs(5), |seen| {
                seen.iter()
                    .filter(|e| e.kind == EventKind::Resting as u8)
                    .count()
                    >= 2
            })
            .await;
        } // connection dies
        tokio::time::sleep(Duration::from_millis(300)).await;

        let working = working_orders(&venue, 1).await;
        assert!(
            !working.contains(&501) && !working.contains(&502),
            "quotes outlived the connection that was managing them: {working:?}"
        );
    });
}

#[test]
fn without_it_orders_survive_the_connection_that_placed_them() {
    // The default. A retail client that loses its connection expects to come back
    // to the order it left.
    let venue = Running::start();
    runtime().block_on(async {
        {
            let mut client = connect(&venue).await;
            send(
                &mut client,
                &[limit_order(3, SYMBOL, 601, Side::Bid, 10_120, 4)],
            )
            .await;
            collect_until(&mut client.acks, Duration::from_secs(5), |seen| {
                seen.iter().any(|e| e.kind == EventKind::Resting as u8)
            })
            .await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        let working = working_orders(&venue, 3).await;
        assert!(
            working.contains(&601),
            "an order was cancelled without being asked for: {working:?}"
        );
    });
}

#[test]
fn turning_cancel_on_disconnect_back_off_takes_effect() {
    let venue = Running::start();
    runtime().block_on(async {
        {
            let mut client = connect(&venue).await;
            send(
                &mut client,
                &[
                    cancel_on_disconnect(4, true),
                    limit_order(4, SYMBOL, 701, Side::Bid, 10_140, 3),
                    cancel_on_disconnect(4, false),
                ],
            )
            .await;
            collect_until(&mut client.acks, Duration::from_secs(5), |seen| {
                seen.iter().any(|e| e.kind == EventKind::Resting as u8)
            })
            .await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        let working = working_orders(&venue, 4).await;
        assert!(
            working.contains(&701),
            "the order was cancelled after the setting was turned off: {working:?}"
        );
    });
}

#[test]
fn a_venue_serves_many_connections_at_once_without_crossing_their_feeds() {
    const CLIENTS: u64 = 32;
    let venue = Running::start();
    runtime().block_on(async {
        let mut clients = Vec::new();
        for account in 1..=CLIENTS.min(8) {
            let mut client = connect(&venue).await;
            // Own price band, so orders rest rather than crossing.
            let base = FLOOR + 1_000 + (account as Ticks) * 300;
            let commands: Vec<Command> = (0..50)
                .map(|i| {
                    limit_order(
                        account,
                        SYMBOL,
                        account * 10_000 + i,
                        Side::Bid,
                        base + i as Ticks,
                        1,
                    )
                })
                .collect();
            send(&mut client, &commands).await;
            clients.push((account, client));
        }

        for (account, client) in &mut clients {
            let events = collect_until(&mut client.acks, Duration::from_secs(15), |seen| {
                seen.iter()
                    .filter(|e| e.kind == EventKind::Received as u8)
                    .count()
                    >= 50
            })
            .await;
            let acknowledged = events
                .iter()
                .filter(|e| e.kind == EventKind::Received as u8)
                .count();
            assert_eq!(acknowledged, 50, "account {account} lost orders");
            assert!(
                events
                    .iter()
                    .all(|e| e.account == 0 || e.account == *account),
                "account {account} received another account's private events"
            );
        }
    });
}

#[test]
fn a_stalled_market_data_stream_does_not_block_order_acknowledgements() {
    // The reason this transport was chosen. The client subscribes to depth and
    // then never reads that stream, while continuing to trade. On one TCP socket
    // carrying both, its own fills would queue behind the feed it is ignoring.
    let venue = Running::start();
    runtime().block_on(async {
        let mut client = connect(&venue).await;
        send(
            &mut client,
            &[
                limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1),
                subscribe(1, SYMBOL, ChannelKind::Book),
            ],
        )
        .await;
        // Accept the feed stream and then deliberately ignore it.
        let _feed = client.connection.accept_uni().await.ok();

        // Generate far more depth than the feed's flow-control window holds.
        let mut flood = connect(&venue).await;
        for round in 0..40_u64 {
            let commands: Vec<Command> = (0..500)
                .map(|i| {
                    limit_order(
                        2,
                        SYMBOL,
                        round * 500 + i + 1,
                        Side::Bid,
                        FLOOR + 1_000 + ((round * 500 + i) % 4_000) as Ticks,
                        1,
                    )
                })
                .collect();
            send(&mut flood, &commands).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        // The ignoring client's own order is still acknowledged.
        send(
            &mut client,
            &[limit_order(1, SYMBOL, 9_999, Side::Bid, 10_060, 2)],
        )
        .await;
        let events = collect_until(&mut client.acks, Duration::from_secs(15), |seen| {
            seen.iter().any(|e| e.order_id == 9_999)
        })
        .await;

        assert!(
            events.iter().any(|e| e.order_id == 9_999),
            "an order acknowledgement was blocked by an unread market-data stream, \
             which is the exact failure this transport exists to avoid"
        );
    });
}
