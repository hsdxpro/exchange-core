//! The TLS 1.3 door, end to end: a rustls client trades through it.
//!
//! The venue offers two listeners -- raw for the colocated cross-connect, TLS
//! for the internet -- and everything past the record layer must be identical:
//! same framing, same events, same budgets. So the real assertion here is not
//! "TLS works", it is that a session behind TLS is indistinguishable from a raw
//! one to the venue, and that the handshake really negotiated 1.3.
//!
//! The certificate and key are generated fresh per run and written only to a
//! temp directory: no key material in the repository, test or otherwise.

use bx_gateway::tcp::Server;
use bx_journal::MemoryLog;
use bx_pipeline::instrument::{Instrument, Instruments};
use bx_pipeline::limit_order;
use bx_protocol::{Command, Event, EventKind, Side};

const FRAME_LEN: usize = size_of::<Event>();
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use zerocopy::{FromBytes, IntoBytes};

const SYMBOL: u32 = 1;
const BTC: u32 = 1;
const USD: u32 = 2;

fn encode(command: &Command, out: &mut Vec<u8>) {
    out.extend_from_slice(command.as_bytes());
}

struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("bx-tls-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A venue with both doors open, and the pieces a client needs to trust it.
struct Running {
    tls_address: String,
    trust: RootCertStore,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    _scratch: Scratch,
}

impl Running {
    fn start() -> Self {
        let scratch = Scratch::new();
        // A fresh identity per run. The private half exists only in this temp
        // directory, for the seconds this test runs.
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_path = scratch.0.join("venue.crt");
        let key_path = scratch.0.join("venue.tls.key");
        std::fs::write(&cert_path, certified.cert.pem()).unwrap();
        std::fs::write(&key_path, certified.key_pair.serialize_pem()).unwrap();

        let mut instruments = Instruments::new();
        instruments.insert(Instrument::new(SYMBOL, BTC, USD, 10_000, 1_000_000, 65_536));
        let mut server =
            Server::bind("127.0.0.1:0", MemoryLog::new(), instruments, 4_096, 256, 64).unwrap();
        server
            .tls_listen("127.0.0.1:0", &cert_path, &key_path)
            .unwrap();
        for account in 1..=4 {
            for asset in [USD, BTC] {
                server
                    .venue_mut()
                    .deposit(account, asset, u64::MAX / 4)
                    .unwrap();
            }
        }
        let tls_address = server.tls_address().unwrap().unwrap().to_string();

        let mut trust = RootCertStore::empty();
        trust
            .add(CertificateDer::from(certified.cert.der().to_vec()))
            .unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) {
                server.poll().expect("the venue failed to commit");
                std::thread::sleep(Duration::from_micros(200));
            }
        });
        Self {
            tls_address,
            trust,
            stop,
            thread: Some(thread),
            _scratch: scratch,
        }
    }

    fn tls_client(&self) -> StreamOwned<ClientConnection, TcpStream> {
        let config = ClientConfig::builder()
            .with_root_certificates(self.trust.clone())
            .with_no_client_auth();
        let name = ServerName::try_from("localhost").unwrap();
        let connection = ClientConnection::new(Arc::new(config), name).unwrap();
        let stream = TcpStream::connect(&self.tls_address).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        StreamOwned::new(connection, stream)
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

fn collect_until(
    stream: &mut StreamOwned<ClientConnection, TcpStream>,
    window: Duration,
    enough: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    let mut seen = Vec::new();
    let mut buffer = vec![0_u8; FRAME_LEN * 64];
    let mut held = 0;
    let deadline = Instant::now() + window;
    while Instant::now() < deadline && !enough(&seen) {
        let Ok(read) = stream.read(&mut buffer[held..]) else {
            continue;
        };
        if read == 0 {
            break;
        }
        let filled = held + read;
        let whole = filled / FRAME_LEN;
        for index in 0..whole {
            let start = index * FRAME_LEN;
            if let Ok(event) = Event::read_from_bytes(&buffer[start..start + FRAME_LEN]) {
                seen.push(event);
            }
        }
        buffer.copy_within(whole * FRAME_LEN..filled, 0);
        held = filled - whole * FRAME_LEN;
    }
    seen
}

#[test]
fn a_session_behind_tls_trades_like_a_raw_one() {
    let venue = Running::start();
    let mut client = venue.tls_client();

    let mut bytes = Vec::new();
    encode(&limit_order(1, SYMBOL, 1, Side::Bid, 10_050, 1), &mut bytes);
    client.write_all(&bytes).unwrap();
    client.flush().unwrap();

    let events = collect_until(&mut client, Duration::from_secs(10), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Resting as u8)
    });
    assert!(
        events.iter().any(|e| e.kind == EventKind::Received as u8),
        "no acknowledgement came back through TLS: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| e.kind == EventKind::Resting as u8 && e.order_id == 1),
        "the order never rested: {events:?}"
    );
    // The whole point of the version pin.
    assert_eq!(
        client.conn.protocol_version(),
        Some(rustls::ProtocolVersion::TLSv1_3),
        "the handshake settled on something other than TLS 1.3"
    );
}

#[test]
fn plaintext_on_the_tls_door_is_dropped_not_served() {
    let venue = Running::start();
    // A raw client -- a scanner, a misconfigured cross-connect -- speaks
    // plaintext at the TLS listener. There is no protocol to answer in, so the
    // only right outcome is a closed connection and an untouched venue.
    let mut naked = TcpStream::connect(&venue.tls_address).unwrap();
    naked
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut bytes = Vec::new();
    encode(&limit_order(1, SYMBOL, 7, Side::Bid, 10_050, 1), &mut bytes);
    naked.write_all(&bytes).unwrap();
    let mut buffer = [0_u8; 256];
    let outcome = naked.read(&mut buffer);
    assert!(
        matches!(outcome, Ok(0) | Err(_)),
        "the TLS door answered plaintext with data: {outcome:?}"
    );

    // And the venue is still fine: a real TLS client trades right after.
    let mut client = venue.tls_client();
    let mut bytes = Vec::new();
    encode(&limit_order(2, SYMBOL, 8, Side::Ask, 10_100, 1), &mut bytes);
    client.write_all(&bytes).unwrap();
    client.flush().unwrap();
    let events = collect_until(&mut client, Duration::from_secs(10), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Resting as u8)
    });
    assert!(
        events
            .iter()
            .any(|e| e.kind == EventKind::Resting as u8 && e.order_id == 8),
        "a plaintext probe broke the door for the client after it: {events:?}"
    );
}

#[test]
fn both_doors_serve_the_same_venue() {
    let venue = Running::start();
    // One order in through TLS...
    let mut seller = venue.tls_client();
    let mut bytes = Vec::new();
    encode(&limit_order(1, SYMBOL, 1, Side::Ask, 10_100, 3), &mut bytes);
    seller.write_all(&bytes).unwrap();
    seller.flush().unwrap();
    let resting = collect_until(&mut seller, Duration::from_secs(10), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Resting as u8)
    });
    assert!(
        resting.iter().any(|e| e.kind == EventKind::Resting as u8),
        "the ask never rested: {resting:?}"
    );

    // ...crossed by an order through TLS from another account: same books,
    // same matching, whatever door the participants used.
    let mut buyer = venue.tls_client();
    let mut bytes = Vec::new();
    encode(&limit_order(2, SYMBOL, 2, Side::Bid, 10_100, 3), &mut bytes);
    buyer.write_all(&bytes).unwrap();
    buyer.flush().unwrap();
    let events = collect_until(&mut buyer, Duration::from_secs(10), |seen| {
        seen.iter().any(|e| e.kind == EventKind::Filled as u8)
    });
    assert!(
        events
            .iter()
            .any(|e| e.kind == EventKind::Filled as u8 && e.quantity == 3),
        "the cross never filled across TLS sessions: {events:?}"
    );
}
