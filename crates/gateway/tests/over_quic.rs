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
use bx_pipeline::{limit_order, subscribe};
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
        let thread = std::thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                venue
                    .poll(Duration::from_millis(2))
                    .expect("the venue failed to commit");
            }
        });
        Self {
            address,
            certificate,
            stop,
            thread: Some(thread),
        }
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
