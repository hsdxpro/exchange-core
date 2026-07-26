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
    /// Decodes a wire byte, returning `None` if it is not a side this version
    /// defines.
    #[must_use]
    pub const fn from_wire(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Bid),
            1 => Some(Self::Ask),
            _ => None,
        }
    }

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

/// Whether an order names a price or takes whatever the book offers.
///
/// Carried explicitly rather than inferred from a sentinel price. A market
/// order used to be signalled by `Ticks::MIN`, which meant every reader had to
/// know the trick, and a price field that is sometimes a price and sometimes a
/// flag is a bug waiting to be written.
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum OrderType {
    /// Rests or trades at `price`, never worse.
    Limit = 0,
    /// Takes the best available. `price` is unused.
    Market = 1,
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
    /// Credits an account. Journalled like everything else, so balances are
    /// reproduced by replay rather than restored from somewhere else.
    ///
    /// A deposit names no instrument, so it reuses two fields: `symbol` carries
    /// the asset credited and `quantity` the amount. See
    /// [`Command::deposit_asset`].
    Deposit = 4,
    /// Starts a feed on one channel. Session control, not venue state: it never
    /// reaches the exchange and is never journalled, because a subscription
    /// belongs to a connection and connections do not survive a restart.
    ///
    /// Layout: `symbol` names the instrument and `quantity` the channel, via
    /// [`Command::channel_kind`].
    Subscribe = 5,
    /// Stops a feed. Same layout as [`Self::Subscribe`].
    Unsubscribe = 6,
    /// Asks for the session's own resting orders on one symbol.
    ///
    /// Session control, like a subscription: it changes nothing and is never
    /// journalled. It exists because a client can rebuild a *book* from a
    /// snapshot but not its own orders, and a trader that has just reconnected
    /// needs to know what it still has working before it can act.
    ///
    /// Layout: `symbol` names the instrument. Answered with
    /// [`EventKind::OrderState`], one per resting order.
    QueryOpenOrders = 7,
    /// Asks the venue to cancel this account's resting orders if the connection
    /// drops. `quantity` of 1 turns it on, 0 off.
    ///
    /// Opt-in because the right answer differs by client. A market maker cannot
    /// manage risk it can no longer see, so leaving its quotes in the book after
    /// its connection dies is dangerous. Someone holding a limit order for a week
    /// wants exactly the opposite. A venue that picks one for everybody is wrong
    /// for half its clients.
    ///
    /// Session control: it changes nothing itself and is never journalled. The
    /// cancels it later causes are ordinary commands and are.
    CancelOnDisconnect = 8,
}

/// Which feed a [`CommandKind::Subscribe`] names.
///
/// The record is a union discriminated by `kind`: an order uses `quantity` for
/// size, and a subscription has no size, so it carries the channel there. Every
/// fixed-layout wire protocol does this; what matters is that the message type
/// determines the layout and that the accessor says so.
#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum ChannelKind {
    /// Price-level changes for one symbol.
    Book = 0,
    /// Prints for one symbol.
    Trades = 1,
    /// The session's own order lifecycle.
    Account = 2,
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
    /// One of the session's own orders as it stands right now.
    ///
    /// Sent in answer to [`CommandKind::QueryOpenOrders`]. `quantity` is what is
    /// still working, not what was originally sent.
    OrderState = 8,
    /// One price level as it stands right now, not a change to it.
    ///
    /// Sent when a client starts following a book, and again if it falls outside
    /// the retention window. A subscriber cannot build a book out of increments
    /// alone -- it has no idea what was there before it arrived -- so the venue
    /// states the current levels first and increments follow. Every one carries
    /// the channel sequence the snapshot is taken at, so a client knows exactly
    /// where the increments resume.
    BookSnapshot = 7,
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
    pub order_type: u8,
}

const _: () = assert!(size_of::<Command>() == 64);

/// An event emitted by the pipeline. Also 64 bytes.
///
/// `Default` is all zeros, which is what an unwritten slot in a subscription
/// ring holds before anything is published into it.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout,
)]
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

impl Event {
    /// Returns `None` if the discriminant is not a value this version defines.
    #[must_use]
    pub fn kind(&self) -> Option<EventKind> {
        match self.kind {
            0 => Some(EventKind::Received),
            1 => Some(EventKind::Rejected),
            2 => Some(EventKind::Resting),
            3 => Some(EventKind::Filled),
            4 => Some(EventKind::Canceled),
            5 => Some(EventKind::BookDelta),
            6 => Some(EventKind::Trade),
            7 => Some(EventKind::BookSnapshot),
            8 => Some(EventKind::OrderState),
            _ => None,
        }
    }

    #[must_use]
    pub fn side(&self) -> Option<Side> {
        Side::from_wire(self.side)
    }
}

impl Command {
    #[must_use]
    pub const fn order_type(&self) -> Option<OrderType> {
        match self.order_type {
            0 => Some(OrderType::Limit),
            1 => Some(OrderType::Market),
            _ => None,
        }
    }

    /// True when this order takes liquidity at any price.
    #[must_use]
    pub const fn is_market(&self) -> bool {
        matches!(self.order_type(), Some(OrderType::Market))
    }

    /// Channel named by a [`CommandKind::Subscribe`] or
    /// [`CommandKind::Unsubscribe`]. Meaningless for any other kind.
    #[must_use]
    pub const fn channel_kind(&self) -> Option<ChannelKind> {
        match self.quantity {
            0 => Some(ChannelKind::Book),
            1 => Some(ChannelKind::Trades),
            2 => Some(ChannelKind::Account),
            _ => None,
        }
    }

    /// True for a message that configures the connection rather than changing
    /// the venue. These are handled by the gateway and never journalled.
    #[must_use]
    pub const fn is_session_control(&self) -> bool {
        self.kind == CommandKind::Subscribe as u8
            || self.kind == CommandKind::Unsubscribe as u8
            || self.kind == CommandKind::QueryOpenOrders as u8
            || self.kind == CommandKind::CancelOnDisconnect as u8
    }

    /// Asset credited by a [`CommandKind::Deposit`]. Meaningless for any other
    /// kind: a deposit names no instrument, so it reuses the symbol field.
    #[must_use]
    pub const fn deposit_asset(&self) -> u32 {
        self.symbol
    }

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
            order_type: OrderType::Limit as u8,
        }
    }

    /// The same command as a market order: takes the best available price.
    #[must_use]
    pub const fn taking(mut self) -> Self {
        self.order_type = OrderType::Market as u8;
        self.price = 0;
        self
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
            4 => Some(CommandKind::Deposit),
            5 => Some(CommandKind::Subscribe),
            6 => Some(CommandKind::Unsubscribe),
            7 => Some(CommandKind::QueryOpenOrders),
            8 => Some(CommandKind::CancelOnDisconnect),
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
        self.kind().is_some()
            && self.side().is_some()
            && self.time_in_force().is_some()
            && self.order_type().is_some()
    }
}

// ------------------------------------------------------- snapshot records

/// Identifies a snapshot file and its layout version.
///
/// Printable ASCII on purpose: a magic carrying raw control bytes makes the
/// source file read as binary to tools that diff and search it, and is easy for
/// an editor to mangle silently.
pub const SNAPSHOT_MAGIC: [u8; 8] = *b"BXSNAPv1";

/// Header of a snapshot: what it contains and where in the journal it applies.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout,
)]
#[repr(C)]
pub struct SnapshotHeader {
    pub magic: [u8; 8],
    /// The first journal sequence **not** included. Recovery replays from here.
    pub sequence: Sequence,
    pub orders: u64,
    pub balances: u64,
}

const _: () = assert!(size_of::<SnapshotHeader>() == 32);

/// One resting order, written in the exact price-then-time order it rests in,
/// so restoring in file order reproduces queue priority.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout,
)]
#[repr(C)]
pub struct SnapshotOrder {
    /// Balance still held for this order. Carried explicitly because a
    /// partially filled order's hold is not recomputable from price and
    /// quantity alone.
    pub reserved: u128,
    pub order_id: OrderId,
    pub account: AccountId,
    pub quantity: Quantity,
    pub price: Ticks,
    pub symbol: SymbolId,
    pub side: u8,
    pub _pad: [u8; 11],
}

const _: () = assert!(size_of::<SnapshotOrder>() == 64);

/// One account's holding of one asset.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout,
)]
#[repr(C)]
pub struct SnapshotBalance {
    pub free: u128,
    pub reserved: u128,
    pub account: AccountId,
    pub asset: u32,
    pub _pad: [u8; 4],
}

const _: () = assert!(size_of::<SnapshotBalance>() == 48);

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
    fn every_command_kind_decodes_and_unknown_ones_are_refused() {
        for (byte, kind) in [
            (0, CommandKind::NewOrder),
            (1, CommandKind::Cancel),
            (2, CommandKind::AmendDown),
            (3, CommandKind::CancelReplace),
            (4, CommandKind::Deposit),
            (5, CommandKind::Subscribe),
            (6, CommandKind::Unsubscribe),
            (7, CommandKind::QueryOpenOrders),
            (8, CommandKind::CancelOnDisconnect),
        ] {
            let mut command = sample();
            command.kind = byte;
            assert_eq!(command.kind(), Some(kind));
            assert!(command.is_well_formed());
        }
    }

    #[test]
    fn a_subscription_names_its_channel_and_is_session_control() {
        for (quantity, kind) in [
            (0, ChannelKind::Book),
            (1, ChannelKind::Trades),
            (2, ChannelKind::Account),
        ] {
            let command = Command {
                kind: CommandKind::Subscribe as u8,
                quantity,
                ..sample()
            };
            assert_eq!(command.channel_kind(), Some(kind));
            assert!(command.is_session_control());
        }

        let unknown = Command {
            kind: CommandKind::Subscribe as u8,
            quantity: 99,
            ..sample()
        };
        assert!(unknown.channel_kind().is_none());

        // An order is not session control, whatever its quantity.
        assert!(!sample().is_session_control());
    }

    #[test]
    fn a_market_order_is_flagged_not_inferred_from_its_price() {
        let limit = sample();
        assert!(!limit.is_market());
        assert_eq!(limit.order_type(), Some(OrderType::Limit));

        let market = sample().taking();
        assert!(market.is_market());
        // The price field carries no hidden meaning for a market order.
        assert_eq!(market.price, 0);
        assert_eq!(
            Command::read_from_bytes(market.as_bytes()).unwrap(),
            market,
            "the flag did not survive the wire"
        );
    }

    #[test]
    fn an_unknown_order_type_is_refused_rather_than_treated_as_a_limit() {
        let mut command = sample();
        command.order_type = 200;
        assert!(command.order_type().is_none());
        assert!(!command.is_well_formed());
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
    fn event_discriminants_decode_and_unknown_ones_are_refused() {
        for (byte, kind) in [
            (0, EventKind::Received),
            (1, EventKind::Rejected),
            (2, EventKind::Resting),
            (3, EventKind::Filled),
            (4, EventKind::Canceled),
            (5, EventKind::BookDelta),
            (6, EventKind::Trade),
            (7, EventKind::BookSnapshot),
            (8, EventKind::OrderState),
        ] {
            let event = Event {
                kind: byte,
                ..Event::default()
            };
            assert_eq!(event.kind(), Some(kind));
        }
        assert!(
            Event {
                kind: 9,
                ..Event::default()
            }
            .kind()
            .is_none()
        );
    }

    #[test]
    fn snapshot_records_round_trip_and_have_no_padding_holes() {
        let order = SnapshotOrder {
            reserved: u128::from(u64::MAX) * 3,
            order_id: 7,
            account: 42,
            quantity: 900,
            price: -12,
            symbol: 3,
            side: Side::Ask as u8,
            _pad: [0; 11],
        };
        assert_eq!(SnapshotOrder::read_from_bytes(order.as_bytes()), Ok(order));

        let balance = SnapshotBalance {
            free: u128::MAX / 2,
            reserved: 17,
            account: 5,
            asset: 2,
            _pad: [0; 4],
        };
        assert_eq!(
            SnapshotBalance::read_from_bytes(balance.as_bytes()),
            Ok(balance)
        );

        let header = SnapshotHeader {
            magic: SNAPSHOT_MAGIC,
            sequence: 1_000,
            orders: 2,
            balances: 3,
        };
        assert_eq!(
            SnapshotHeader::read_from_bytes(header.as_bytes()),
            Ok(header)
        );
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
    }
}
