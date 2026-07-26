//! Wire framing.
//!
//! Both records are a fixed 64 bytes, so a frame needs no length prefix and no
//! parsing step: a whole number of records is a whole number of frames, and a
//! partial record is simply a buffer that has not filled yet. Decoding is a
//! bounds check and a cast.
//!
//! The buffer keeps whatever trailing bytes did not make a complete record and
//! prepends them to the next read, which is the only thing a stream transport
//! requires of a decoder: TCP will split a record across two segments sooner or
//! later, and a decoder that assumes otherwise works in testing and corrupts
//! the venue under load.

use bx_protocol::Command;
use zerocopy::{FromBytes, IntoBytes};

/// Bytes in one framed record. Both directions use the same size.
pub const FRAME_LEN: usize = size_of::<Command>();

const _: () = assert!(FRAME_LEN == 64);
const _: () = assert!(size_of::<bx_protocol::Event>() == FRAME_LEN);

/// Accumulates bytes off a stream and yields whole records.
///
/// Fixed capacity: a client cannot make the venue allocate by sending a partial
/// record and stopping, because there is nowhere for the buffer to grow to.
#[derive(Debug)]
pub struct Decoder {
    buffer: Vec<u8>,
    /// Bytes currently held.
    filled: usize,
}

impl Decoder {
    /// `max_records` is how many whole records one read may deliver, which
    /// bounds both the buffer and the work one client can queue in a turn.
    #[must_use]
    pub fn new(max_records: usize) -> Self {
        Self {
            buffer: vec![0; max_records * FRAME_LEN],
            filled: 0,
        }
    }

    /// Space a reader may fill on this pass. Empty when the buffer is full,
    /// which is the signal to drain before reading more.
    pub fn writable(&mut self) -> &mut [u8] {
        &mut self.buffer[self.filled..]
    }

    /// Records the bytes a reader placed in [`Self::writable`].
    pub fn advance(&mut self, bytes: usize) {
        self.filled += bytes;
        debug_assert!(self.filled <= self.buffer.len());
    }

    /// Decodes every whole record held, and keeps any partial tail for the next
    /// read. Returns how many were appended to `out`.
    ///
    /// A record whose discriminants do not decode is dropped rather than
    /// guessed at: it is either corruption or a newer protocol version, and in
    /// both cases acting on it is worse than ignoring it.
    pub fn drain(&mut self, out: &mut Vec<Command>) -> usize {
        let whole = self.filled / FRAME_LEN;
        let mut decoded = 0;
        for index in 0..whole {
            let start = index * FRAME_LEN;
            let Ok(command) = Command::read_from_bytes(&self.buffer[start..start + FRAME_LEN])
            else {
                continue;
            };
            if command.is_well_formed() {
                out.push(command);
                decoded += 1;
            }
        }
        let consumed = whole * FRAME_LEN;
        self.buffer.copy_within(consumed..self.filled, 0);
        self.filled -= consumed;
        decoded
    }

    #[must_use]
    pub const fn is_full(&self) -> bool {
        self.filled == self.buffer.len()
    }

    /// Bytes held that do not yet form a record.
    ///
    /// Only the framing tests read this, and a private method used solely from
    /// test code counts as dead in a normal build, so it is gated rather than
    /// made public for a caller that does not exist.
    #[cfg(test)]
    const fn partial(&self) -> usize {
        self.filled % FRAME_LEN
    }
}

/// Appends a record's bytes to an output buffer.
pub fn encode<T: IntoBytes + zerocopy::Immutable>(record: &T, out: &mut Vec<u8>) {
    out.extend_from_slice(record.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use bx_protocol::{CommandKind, Event, Side, TimeInForce};

    fn command(order_id: u64) -> Command {
        Command::new(
            CommandKind::NewOrder,
            1,
            1,
            order_id,
            Side::Bid,
            10_100,
            5,
            TimeInForce::GoodTillCancel,
        )
    }

    /// Feeds `bytes` through the decoder in chunks of `chunk`, as a stream
    /// would deliver them.
    fn stream(decoder: &mut Decoder, bytes: &[u8], chunk: usize) -> Vec<Command> {
        let mut out = Vec::new();
        for piece in bytes.chunks(chunk) {
            let room = decoder.writable();
            let take = piece.len().min(room.len());
            room[..take].copy_from_slice(&piece[..take]);
            decoder.advance(take);
            decoder.drain(&mut out);
        }
        out
    }

    #[test]
    fn a_whole_record_decodes() {
        let mut decoder = Decoder::new(4);
        let mut bytes = Vec::new();
        encode(&command(7), &mut bytes);
        assert_eq!(stream(&mut decoder, &bytes, FRAME_LEN)[0].order_id, 7);
    }

    #[test]
    fn a_record_split_across_reads_is_reassembled() {
        let mut bytes = Vec::new();
        for id in 1..=3 {
            encode(&command(id), &mut bytes);
        }
        // Every chunk size, including ones that never align to a record.
        for chunk in [1, 5, 7, 63, 64, 65, 100, 192] {
            let mut decoder = Decoder::new(8);
            let decoded = stream(&mut decoder, &bytes, chunk);
            assert_eq!(
                decoded.iter().map(|c| c.order_id).collect::<Vec<_>>(),
                vec![1, 2, 3],
                "chunk size {chunk} lost or reordered a record"
            );
            assert_eq!(decoder.partial(), 0, "chunk size {chunk} left a tail");
        }
    }

    #[test]
    fn a_partial_record_yields_nothing_until_it_completes() {
        let mut decoder = Decoder::new(4);
        let mut bytes = Vec::new();
        encode(&command(9), &mut bytes);

        let mut out = Vec::new();
        decoder.writable()[..40].copy_from_slice(&bytes[..40]);
        decoder.advance(40);
        assert_eq!(decoder.drain(&mut out), 0, "decoded an incomplete record");
        assert_eq!(decoder.partial(), 40);

        decoder.writable()[..24].copy_from_slice(&bytes[40..]);
        decoder.advance(24);
        assert_eq!(decoder.drain(&mut out), 1);
        assert_eq!(out[0], command(9));
    }

    #[test]
    fn a_record_with_an_undecodable_field_is_dropped_not_guessed() {
        let mut broken = command(1);
        broken.kind = 200;
        let mut bytes = Vec::new();
        encode(&broken, &mut bytes);
        encode(&command(2), &mut bytes);

        let mut decoder = Decoder::new(4);
        let decoded = stream(&mut decoder, &bytes, FRAME_LEN * 2);
        assert_eq!(
            decoded.iter().map(|c| c.order_id).collect::<Vec<_>>(),
            vec![2],
            "a malformed record was acted on, or a good one was lost with it"
        );
    }

    #[test]
    fn the_buffer_never_grows_however_much_is_pushed_at_it() {
        let mut decoder = Decoder::new(4);
        let capacity = decoder.writable().len();
        for _ in 0..100 {
            let room = decoder.writable().len();
            if room == 0 {
                let mut out = Vec::new();
                decoder.drain(&mut out);
                continue;
            }
            decoder.advance(room);
            let mut out = Vec::new();
            decoder.drain(&mut out);
        }
        assert!(decoder.is_full() || decoder.partial() == 0);
        assert_eq!(
            decoder.writable().len() + decoder.filled,
            capacity,
            "the buffer changed size"
        );
    }

    #[test]
    fn events_frame_to_the_same_width_as_commands() {
        let mut bytes = Vec::new();
        encode(&Event::default(), &mut bytes);
        assert_eq!(bytes.len(), FRAME_LEN);
    }
}
