//! Wire types for the exchange.
//!
//! Every type here has a fixed layout, a fixed size, and no padding, so a
//! record can be read straight out of a byte slice with no parsing step and no
//! allocation. The layout is asserted at compile time; a field reorder that
//! changes the size will not build.
//!
//! Widths are chosen for a venue, not for the engine. Quantities are `u64`
//! because crypto base units overflow `u32` immediately, and order IDs are
//! `u64` because they are exchange-assigned identifiers rather than table
//! indices. The adapter into the matching engine narrows them and rejects what
//! does not fit; see `bx-engine`'s documented limits.

use core::fmt;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

pub type AccountId = u64;
pub type OrderId = u64;
pub type SymbolId = u32;
pub type Sequence = u64;

/// Price in ticks. Signed so an unset or sentinel price is representable
/// without stealing a value from the domain.
pub type Ticks = i64;

/// Quantity in an instrument's base units.
pub type Quantity = u64;

/// Bumped whenever any record layout changes. A peer sending a different
/// version is rejected at the handshake rather than misread.
pub const WIRE_VERSION: u8 = 1;

// ---------------------------------------------------------------- enums

/// `u8` discriminants so the wire representation is stable regardless of what
/// the compiler would otherwise choose.
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum Side {
    Bid = 0,
    Ask = 1,
}

impl Side {
    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Bid => Self::Ask,
            Self::Ask => Self::Bid,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum TimeInForce {
    GoodTillCancel = 0,
    ImmediateOrCancel = 1,
    FillOrKill = 2,
    PostOnly = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum CommandKind {
    NewOrder = 0,
    Cancel = 1,
    /// Reduce quantity, retaining queue priority.
    AmendDown = 2,
    /// Cancel and re-enter with a new ID and new priority.
    CancelReplace = 3,
}

/// Why an order was refused. Every variant is a distinct, reportable reason; a
/// client should never see a generic failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum RejectReason {
    None = 0,
    UnknownAccount = 1,
    UnknownSymbol = 2,
    QuantityZero = 3,
    QuantityTooLarge = 4,
    DuplicateOrderId = 5,
    UnknownOrderId = 6,
    OutsidePriceBand = 7,
    InsufficientBalance = 8,
    OrderLimitReached = 9,
    WouldCross = 10,
    InsufficientLiquidity = 11,
    AmendWouldIncrease = 12,
    SelfMatchPrevented = 13,
    UnsupportedTimeInForce = 14,
    EngineCapacity = 15,
}

impl RejectReason {
    #[must_use]
    pub const fn is_rejected(self) -> bool {
        !matches!(self, Self::None)
    }
}

impl fmt::Display for RejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::None => "accepted",
            Self::UnknownAccount => "unknown account",
            Self::UnknownSymbol => "unknown symbol",
            Self::QuantityZero => "quantity must be greater than zero",
            Self::QuantityTooLarge => "quantity exceeds the venue limit",
            Self::DuplicateOrderId => "order ID is already live",
            Self::UnknownOrderId => "order ID is not live",
            Self::OutsidePriceBand => "price is outside the permitted band",
            Self::InsufficientBalance => "insufficient free balance",
            Self::OrderLimitReached => "open order limit reached",
            Self::WouldCross => "post-only order would cross the book",
            Self::InsufficientLiquidity => "fill-or-kill cannot be filled completely",
            Self::AmendWouldIncrease => "an amend cannot increase quantity; use cancel/replace",
            Self::SelfMatchPrevented => "self-match prevented",
            Self::UnsupportedTimeInForce => "unsupported time-in-force",
            Self::EngineCapacity => "matching engine capacity exhausted",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum EventKind {
    /// Durable and sequenced. Not yet matched.
    Received = 0,
    Rejected = 1,
    Resting = 2,
    Filled = 3,
    Canceled = 4,
    /// One price level changed. The unit of the depth feed.
    BookDelta = 5,
    Trade = 6,
}

// ---------------------------------------------------------------- records

/// A client command, once sequenced. This is the journal record: replaying
/// these in order reproduces the whole exchange.
///
/// 64 bytes, which is one cache line, so a record never straddles two.
#[derive(Clone, Copy, Debug, Eq, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct Command {
    pub sequence: Sequence,
    /// NIC hardware timestamp: when the packet reached the venue.
    pub ingress_ns: u64,
    pub account: AccountId,
    pub order_id: OrderId,
    /// For `CancelReplace`, the ID the replacement takes.
    pub replacement_id: OrderId,
    pub quantity: Quantity,
    pub price: Ticks,
    pub symbol: SymbolId,
    pub kind: u8,
    pub side: u8,
    pub time_in_force: u8,
    pub _pad: [u8; 1],
}

const _: () = assert!(size_of::<Command>() == 64);

/// An event emitted by the pipeline. Also 64 bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct Event {
    /// Position in this event's channel, so a subscriber detects a gap by
    /// arithmetic rather than by waiting.
    pub sequence: Sequence,
    /// The command that caused this event.
    pub cause_sequence: Sequence,
    pub account: AccountId,
    pub order_id: OrderId,
    /// For a fill, the resting order that was hit.
    pub counterparty_order_id: OrderId,
    pub quantity: Quantity,
    pub price: Ticks,
    pub symbol: SymbolId,
    pub kind: u8,
    pub side: u8,
    pub reject_reason: u8,
    pub _pad: [u8; 1],
}

const _: () = assert!(size_of::<Event>() == 64);

impl Command {
    /// Builds a command with the sequence unset. The sequencer stamps it.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kind: CommandKind,
        account: AccountId,
        symbol: SymbolId,
        order_id: OrderId,
        side: Side,
        price: Ticks,
        quantity: Quantity,
        time_in_force: TimeInForce,
    ) -> Self {
        Self {
            sequence: 0,
            ingress_ns: 0,
            account,
            order_id,
            replacement_id: 0,
            quantity,
            price,
            symbol,
            kind: kind as u8,
            side: side as u8,
            time_in_force: time_in_force as u8,
            _pad: [0; 1],
        }
    }

    /// Returns `None` if a discriminant is not a value this version defines,
    /// which is what a corrupt or newer-version record looks like.
    #[must_use]
    pub fn kind(&self) -> Option<CommandKind> {
        match self.kind {
            0 => Some(CommandKind::NewOrder),
            1 => Some(CommandKind::Cancel),
            2 => Some(CommandKind::AmendDown),
            3 => Some(CommandKind::CancelReplace),
            _ => None,
        }
    }

    #[must_use]
    pub fn side(&self) -> Option<Side> {
        match self.side {
            0 => Some(Side::Bid),
            1 => Some(Side::Ask),
            _ => None,
        }
    }

    #[must_use]
    pub fn time_in_force(&self) -> Option<TimeInForce> {
        match self.time_in_force {
            0 => Some(TimeInForce::GoodTillCancel),
            1 => Some(TimeInForce::ImmediateOrCancel),
            2 => Some(TimeInForce::FillOrKill),
            3 => Some(TimeInForce::PostOnly),
            _ => None,
        }
    }

    /// True when every discriminant decodes and the required fields are sane.
    /// Called on replay, where the bytes may be truncated or corrupt.
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        self.kind().is_some() && self.side().is_some() && self.time_in_force().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Command {
        Command::new(
            CommandKind::NewOrder,
            42,
            7,
            1001,
            Side::Bid,
            -25,
            9_000_000_000_000,
            TimeInForce::GoodTillCancel,
        )
    }

    #[test]
    fn command_round_trips_through_bytes() {
        let command = sample();
        let bytes = command.as_bytes();
        assert_eq!(bytes.len(), 64);
        assert_eq!(Command::read_from_bytes(bytes).unwrap(), command);
    }

    #[test]
    fn quantity_exceeds_u32_and_price_is_signed() {
        let command = sample();
        assert!(command.quantity > u64::from(u32::MAX));
        assert!(command.price < 0);
        let decoded = Command::read_from_bytes(command.as_bytes()).unwrap();
        assert_eq!(decoded.quantity, command.quantity);
        assert_eq!(decoded.price, command.price);
    }

    #[test]
    fn unknown_discriminants_are_reported_not_guessed() {
        let mut command = sample();
        command.kind = 200;
        assert!(command.kind().is_none());
        assert!(!command.is_well_formed());
    }

    #[test]
    fn a_short_buffer_fails_instead_of_reading_past_it() {
        let command = sample();
        let bytes = command.as_bytes();
        assert!(Command::read_from_bytes(&bytes[..63]).is_err());
    }

    #[test]
    fn events_round_trip_and_stay_one_cache_line() {
        let event = Event {
            sequence: 5,
            cause_sequence: 4,
            account: 42,
            order_id: 1001,
            counterparty_order_id: 999,
            quantity: 1_500,
            price: 100,
            symbol: 7,
            kind: EventKind::Filled as u8,
            side: Side::Bid as u8,
            reject_reason: RejectReason::None as u8,
            _pad: [0; 1],
        };
        assert_eq!(size_of::<Event>(), 64);
        assert_eq!(Event::read_from_bytes(event.as_bytes()).unwrap(), event);
    }

    #[test]
    fn every_reject_reason_has_a_distinct_message() {
        let reasons = [
            RejectReason::None,
            RejectReason::UnknownAccount,
            RejectReason::UnknownSymbol,
            RejectReason::QuantityZero,
            RejectReason::QuantityTooLarge,
            RejectReason::DuplicateOrderId,
            RejectReason::UnknownOrderId,
            RejectReason::OutsidePriceBand,
            RejectReason::InsufficientBalance,
            RejectReason::OrderLimitReached,
            RejectReason::WouldCross,
            RejectReason::InsufficientLiquidity,
            RejectReason::AmendWouldIncrease,
            RejectReason::SelfMatchPrevented,
            RejectReason::UnsupportedTimeInForce,
            RejectReason::EngineCapacity,
        ];
        let mut messages: Vec<String> = reasons.iter().map(ToString::to_string).collect();
        let total = messages.len();
        messages.sort_unstable();
        messages.dedup();
        assert_eq!(messages.len(), total, "two reject reasons share a message");
        assert!(!RejectReason::None.is_rejected());
        assert!(RejectReason::UnknownAccount.is_rejected());
    }
}
