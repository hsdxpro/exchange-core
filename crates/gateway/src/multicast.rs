//! The feed as one packet, however many are listening.
//!
//! TCP fan-out costs a copy and a write per subscriber. Multicast costs one
//! send: the switch replicates, and a group of ten and a group of ten thousand
//! are the same work for the venue. That is the whole reason professional
//! market data is distributed this way, and it is the only fan-out that does
//! not get more expensive as it succeeds.
//!
//! ## The shape, and where it comes from
//!
//! MoldUDP64, which is what Nasdaq's ITCH is carried over, and CME's feeds are
//! the same idea: a header naming the run and the position, several messages
//! per packet, and a sequence a receiver checks by arithmetic. Ours is that
//! with one simplification -- a message here is a fixed 64-byte event, so no
//! per-message length prefix is needed and a packet is a header and an array.
//!
//! ## A and B
//!
//! Two groups carry identical packets. A receiver takes whichever copy of a
//! sequence arrives first and discards the second, so a packet lost on one path
//! is covered by the other without anybody asking for a retransmission. Line
//! redundancy, not bandwidth: the feeds are the same feed.
//!
//! ## What this deliberately does not do
//!
//! Nothing here waits for anyone. There is no acknowledgement, no flow control
//! and no retransmission on this path -- a receiver that misses a packet sees a
//! gap in the sequence and asks the recovery service, which is a separate thing
//! on a separate socket precisely so that a slow receiver cannot reach back
//! into the fast one.

use bx_pipeline::hub::Channel;
use bx_protocol::Event;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use zerocopy::IntoBytes;

/// Bytes of event in one packet.
///
/// Sized so a full packet fits inside a 1,500-byte Ethernet MTU with room for
/// IP and UDP headers and the feed header: fragmenting a market-data packet
/// would mean a single lost fragment costing the whole packet, which is the one
/// thing sequence numbers cannot repair cheaply.
pub const MOST_PER_PACKET: usize = 21;

/// Header on every packet. Fixed layout, little-endian, like everything else on
/// this venue's wire.
///
/// `session` distinguishes one run of the venue from the next. Sequence numbers
/// restart when a venue does, so a receiver that reconnects across a promotion
/// can tell "sequence 5 again" from "sequence 5 still" -- without it, a stale
/// cursor would look valid and a client would silently skip a market.
#[derive(Clone, Copy, Debug)]
pub struct Header {
    pub session: u64,
    /// Which feed this packet belongs to, encoded as kind and symbol.
    pub channel_kind: u8,
    pub symbol: u32,
    /// Position of the first event in the packet, on that channel.
    pub sequence: u64,
    pub count: u16,
}

/// Bytes a header occupies. Kept a multiple of eight so the events after it
/// land aligned, which is what lets a receiver read them without copying.
pub const HEADER_LEN: usize = 32;

impl Header {
    /// Writes the header into the front of a packet.
    pub fn write(&self, out: &mut [u8; HEADER_LEN]) {
        out[..8].copy_from_slice(&self.session.to_le_bytes());
        out[8..16].copy_from_slice(&self.sequence.to_le_bytes());
        out[16..20].copy_from_slice(&self.symbol.to_le_bytes());
        out[20..22].copy_from_slice(&self.count.to_le_bytes());
        out[22] = self.channel_kind;
        out[23..].fill(0);
    }

    /// Reads a header back. `None` if the slice is too short to hold one.
    #[must_use]
    pub fn read(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_LEN {
            return None;
        }
        let word =
            |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().ok().unwrap_or([0; 8]));
        Some(Self {
            session: word(0),
            sequence: word(8),
            symbol: u32::from_le_bytes(bytes[16..20].try_into().ok()?),
            count: u16::from_le_bytes(bytes[20..22].try_into().ok()?),
            channel_kind: bytes[22],
        })
    }
}

/// How a channel is named on the wire. Matches the subscription encoding, so a
/// client that can ask for a channel can recognise one.
#[must_use]
pub const fn wire_channel(channel: Channel) -> Option<(u8, u32)> {
    match channel {
        Channel::Book(symbol) => Some((0, symbol)),
        Channel::Trades(symbol) => Some((1, symbol)),
        Channel::Bbo(symbol) => Some((3, symbol)),
        Channel::Checkpoint => Some((4, 0)),
        // Never multicast. A private feed on a group anybody may join is not a
        // private feed.
        Channel::Account(_) => None,
    }
}

/// Sends the feed to one or two groups.
#[derive(Debug)]
pub struct Multicast {
    sockets: Vec<UdpSocket>,
    destinations: Vec<SocketAddr>,
    session: u64,
    /// Built once and refilled. A market-data send path that allocates is one
    /// that pauses for the allocator while a market moves.
    packet: Vec<u8>,
    sent: u64,
    failed: u64,
}

impl Multicast {
    /// Opens the sending sockets. One destination is a feed; two are A and B.
    ///
    /// # Errors
    /// Fails if a socket cannot be opened or an address cannot be parsed.
    pub fn open(destinations: &[String], session: u64) -> io::Result<Self> {
        let mut sockets = Vec::with_capacity(destinations.len());
        let mut parsed = Vec::with_capacity(destinations.len());
        for destination in destinations {
            let address: SocketAddr = destination.parse().map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("{destination}: {e}"))
            })?;
            let socket = UdpSocket::bind("0.0.0.0:0")?;
            // One hop by default: a market-data group belongs on the venue's own
            // network, and a feed that escapes it is a leak rather than a
            // feature. An operator who wants it routed says so at the switch.
            let _ = socket.set_multicast_ttl_v4(1);
            // The venue does not consume its own feed, and looping it back
            // costs a copy through the kernel for nobody.
            let _ = socket.set_multicast_loop_v4(false);
            socket.set_nonblocking(true)?;
            sockets.push(socket);
            parsed.push(address);
        }
        Ok(Self {
            sockets,
            destinations: parsed,
            session,
            packet: vec![0; HEADER_LEN + MOST_PER_PACKET * size_of::<Event>()],
            sent: 0,
            failed: 0,
        })
    }

    /// Sends `events` for one channel, batched into as few packets as fit.
    ///
    /// `from` is the channel position of the first event. Every packet names
    /// its own first position, so a receiver reading packets out of order still
    /// knows where each belongs.
    pub fn send(&mut self, channel: Channel, from: u64, events: &[Event]) {
        let Some((kind, symbol)) = wire_channel(channel) else {
            return;
        };
        for (batch, chunk) in events.chunks(MOST_PER_PACKET).enumerate() {
            let header = Header {
                session: self.session,
                channel_kind: kind,
                symbol,
                sequence: from + (batch * MOST_PER_PACKET) as u64,
                count: chunk.len() as u16,
            };
            let mut front = [0_u8; HEADER_LEN];
            header.write(&mut front);
            self.packet[..HEADER_LEN].copy_from_slice(&front);
            let mut at = HEADER_LEN;
            for event in chunk {
                let bytes = event.as_bytes();
                self.packet[at..at + bytes.len()].copy_from_slice(bytes);
                at += bytes.len();
            }
            // Both groups carry the same bytes. A failure on one is counted and
            // not retried: the other path is the redundancy, and blocking here
            // would put a switch in charge of the venue's pace.
            for (socket, destination) in self.sockets.iter().zip(&self.destinations) {
                match socket.send_to(&self.packet[..at], destination) {
                    Ok(_) => self.sent += 1,
                    Err(_) => self.failed += 1,
                }
            }
        }
    }

    /// Packets sent, and sends that failed.
    #[must_use]
    pub const fn counts(&self) -> (u64, u64) {
        (self.sent, self.failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events(count: usize, first: u64) -> Vec<Event> {
        (0..count)
            .map(|i| Event {
                sequence: first + i as u64,
                ..Event::default()
            })
            .collect()
    }

    #[test]
    fn a_header_survives_the_wire() {
        let header = Header {
            session: 0x0102_0304_0506_0708,
            channel_kind: 1,
            symbol: 42,
            sequence: 9_999,
            count: 7,
        };
        let mut bytes = [0_u8; HEADER_LEN];
        header.write(&mut bytes);
        let back = Header::read(&bytes).expect("a full header did not read back");
        assert_eq!(back.session, header.session);
        assert_eq!(back.sequence, header.sequence);
        assert_eq!(back.symbol, header.symbol);
        assert_eq!(back.count, header.count);
        assert_eq!(back.channel_kind, header.channel_kind);
    }

    #[test]
    fn a_truncated_header_is_refused_rather_than_guessed() {
        assert!(Header::read(&[0_u8; HEADER_LEN - 1]).is_none());
    }

    #[test]
    fn a_full_packet_fits_inside_an_ethernet_frame() {
        // The reason MOST_PER_PACKET is what it is. A fragmented market-data
        // packet turns one lost fragment into a lost packet.
        let full = HEADER_LEN + MOST_PER_PACKET * size_of::<Event>();
        assert!(
            full + 28 <= 1_500,
            "a full packet plus IP and UDP headers is {} bytes, past the MTU",
            full + 28
        );
    }

    #[test]
    fn a_private_channel_is_never_given_a_wire_name() {
        // A group anybody may join must not be able to carry one account's
        // fills.
        assert!(wire_channel(Channel::Account(7)).is_none());
        assert!(wire_channel(Channel::Trades(1)).is_some());
    }

    #[test]
    fn events_arrive_batched_numbered_and_whole() {
        // Bound to a real socket and read back: the packet layout is checked as
        // a receiver sees it, not as the sender believes it wrote it.
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        receiver
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let address = receiver.local_addr().unwrap().to_string();
        let mut feed = Multicast::open(&[address], 0xABCD).unwrap();

        // One more than fits, so batching is exercised rather than assumed.
        let count = MOST_PER_PACKET + 3;
        feed.send(Channel::Trades(9), 100, &events(count, 0));

        let mut seen = 0;
        let mut buffer = vec![0_u8; 2_048];
        let mut expected = 100_u64;
        while seen < count {
            let read = receiver.recv(&mut buffer).expect("a packet never arrived");
            let header = Header::read(&buffer[..read]).expect("packet had no header");
            assert_eq!(header.session, 0xABCD, "the run identifier was lost");
            assert_eq!(header.symbol, 9);
            assert_eq!(
                header.channel_kind, 1,
                "the tape arrived named as something else"
            );
            assert_eq!(
                header.sequence, expected,
                "a packet did not say where it belongs"
            );
            assert_eq!(
                read,
                HEADER_LEN + header.count as usize * size_of::<Event>(),
                "the packet length disagrees with its own count"
            );
            assert!(
                header.count as usize <= MOST_PER_PACKET,
                "a packet carried more than an MTU allows"
            );
            expected += u64::from(header.count);
            seen += header.count as usize;
        }
        assert_eq!(seen, count, "the batches did not add up to what was sent");
        assert_eq!(
            feed.counts(),
            (2, 0),
            "the send count disagrees with the wire"
        );
    }

    #[test]
    fn both_paths_carry_the_same_bytes() {
        // A and B are line redundancy: identical content, so a receiver can
        // take whichever copy arrives first.
        let a = UdpSocket::bind("127.0.0.1:0").unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").unwrap();
        for socket in [&a, &b] {
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
        }
        let mut feed = Multicast::open(
            &[
                a.local_addr().unwrap().to_string(),
                b.local_addr().unwrap().to_string(),
            ],
            1,
        )
        .unwrap();
        feed.send(Channel::Bbo(3), 7, &events(2, 0));

        let mut first = vec![0_u8; 2_048];
        let mut second = vec![0_u8; 2_048];
        let read_a = a.recv(&mut first).expect("A never arrived");
        let read_b = b.recv(&mut second).expect("B never arrived");
        assert_eq!(
            &first[..read_a],
            &second[..read_b],
            "A and B disagreed, so a receiver could not treat them as one feed"
        );
    }
}
