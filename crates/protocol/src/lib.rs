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

/// Bytes of nonce in an [`EventKind::Challenge`]. 128 bits, so a nonce does not
/// repeat within a venue's lifetime and a proof cannot be replayed onto a later
/// connection.
pub const CHALLENGE_LEN: usize = 16;

/// Bytes of proof in a [`CommandKind::Authenticate`]: a full HMAC-SHA256 tag,
/// untruncated.
pub const PROOF_LEN: usize = 32;

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
    /// Proves the session may act for `account`, answering the challenge the
    /// venue sent on connect.
    ///
    /// Layout: `account` is the account claimed, and the four fields after it
    /// carry a 32-byte proof, via [`Command::proof`].
    ///
    /// Session control, and the one message accepted before authentication. The
    /// secret is never sent — see [`EventKind::Challenge`] for why that matters
    /// on this transport.
    Authenticate = 9,
    /// Sets whether a symbol accepts new orders. `symbol` names it and
    /// `quantity` carries a [`TradingState`].
    ///
    /// Journalled, because it *is* venue state: a replay that resumed trading a
    /// symbol an operator had halted would rebuild a venue nobody asked for.
    /// Permitted only from the configured admin account.
    SetSymbolState = 10,
    /// Stops or resumes an account's ability to open new risk. `account` names
    /// it and `quantity` is 0 to stop and 1 to resume.
    ///
    /// The kill switch. Journalled for the same reason as
    /// [`Self::SetSymbolState`], and it never blocks a cancel: an account that
    /// cannot be flattened is more dangerous than one that can still trade.
    SetAccountTrading = 11,
    /// Cancels everything an account has resting. `account` names it, and
    /// `symbol` narrows it to one instrument or is zero for all of them.
    ///
    /// What an operator reaches for when a client has gone wrong and what a
    /// client reaches for when its own state is uncertain. Expanded into
    /// ordinary cancels, so each one journals, publishes and replays like any
    /// other.
    CancelAll = 12,
}

/// Whether a symbol is open, closing, or shut.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, TryFromBytes, IntoBytes, Immutable, KnownLayout,
)]
#[repr(u8)]
pub enum TradingState {
    /// Normal.
    #[default]
    Trading = 0,
    /// Existing orders may be cancelled or amended down; no new orders rest.
    ///
    /// The state that matters most and the one venues actually use. Pulling a
    /// symbol straight to [`Self::Halted`] traps everyone in their positions;
    /// cancel-only lets the book drain in an orderly way, which is the whole
    /// point of having a state between open and shut.
    CancelOnly = 1,
    /// Nothing is accepted for this symbol, not even a cancel.
    ///
    /// For a venue that has decided the book itself is wrong -- a bad print, a
    /// corrupt feed -- and wants it frozen exactly as it stands while somebody
    /// looks at it.
    Halted = 2,
}

impl TradingState {
    /// Decodes a wire value, returning `None` for one this version does not
    /// define.
    #[must_use]
    pub const fn from_wire(value: u64) -> Option<Self> {
        match value {
            0 => Some(Self::Trading),
            1 => Some(Self::CancelOnly),
            2 => Some(Self::Halted),
            _ => None,
        }
    }
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
    /// Top of book only, for one symbol.
    ///
    /// The cheapest public feed and what most clients actually want. An order
    /// resting deep in the book moves the depth feed and not this one, so a
    /// client that only needs a price pays for price changes rather than for
    /// every order the venue receives.
    Bbo = 3,
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
    /// Sent before the session proved who it is, or the proof was wrong.
    NotAuthenticated = 16,
    /// The account is sending faster than its allowance. The command was
    /// discarded; the session stays open.
    RateLimited = 17,
    /// The order ID is not above the highest this account has had accepted.
    ///
    /// Separate from [`Self::DuplicateOrderId`], which means the ID is still
    /// live. This one means it was used at some point and is now gone, and the
    /// distinction is the whole point: a client retrying an order it never got
    /// an answer for needs to know that its earlier attempt *did* land, not
    /// that some unrelated order happens to hold the ID.
    ///
    /// Appended rather than inserted. These discriminants are on the wire, so
    /// renumbering an existing one silently changes what every deployed client
    /// reads from a message it has already received.
    OrderIdNotIncreasing = 18,
    /// The symbol is not accepting new orders.
    SymbolNotTrading = 19,
    /// The account has been stopped from opening new risk.
    AccountNotTrading = 20,
    /// The session's account may not send this command.
    NotPermitted = 21,
}

impl RejectReason {
    /// How many reasons exist, so a table indexed by one can be sized from here
    /// rather than from a number somebody keeps in step by hand.
    ///
    /// Two separate arrays elsewhere were sized to a literal and both were
    /// forgotten when a reason was appended, which panicked on the first
    /// refusal for the newest reason -- a crash reachable from the wire. The
    /// test below pins this against the decoder, so a variant added without
    /// updating it fails there instead.
    pub const COUNT: usize = Self::NotPermitted as usize + 1;
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
            Self::NotAuthenticated => "not authenticated",
            Self::RateLimited => "rate limit exceeded",
            Self::OrderIdNotIncreasing => "order ID must be above the highest already used",
            Self::SymbolNotTrading => "symbol is not accepting new orders",
            Self::AccountNotTrading => "account is not permitted to open new orders",
            Self::NotPermitted => "this account may not send that command",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(u8)]
pub enum EventKind {
    /// Durable and sequenced. Not yet matched.
    ///
    /// Carries the two timestamps, because it is the one event every command
    /// produces and its `quantity` and `price` would otherwise be zero: see
    /// [`Event::ingress_ns`] and [`Event::match_ns`]. Putting them in a field of
    /// their own would mean a 72-byte event, and an event that no longer fits a
    /// cache line costs every subscriber on every message to carry a number most
    /// of them never read.
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
    /// A nonce the session must sign to prove who it is. Sent on connect, before
    /// the client has said anything.
    ///
    /// The venue takes no TLS, deliberately: a market maker on a cross-connect
    /// wants nothing between it and the book. That decision is what makes a
    /// bearer token useless here — anyone able to read the wire could replay it —
    /// so the secret never crosses the wire at all. The client signs this nonce
    /// instead, and the venue checks the signature against the secret it holds.
    /// A fresh nonce per connection is what stops the answer being replayed.
    ///
    /// Layout: 16 bytes of nonce, via [`Event::challenge`].
    Challenge = 9,
    /// The proof was accepted. The session may trade as the account it claimed.
    Authenticated = 10,
    /// Best price on one side, and the quantity there.
    ///
    /// One side per event, discriminated by `side`, exactly like
    /// [`Self::BookDelta`] — so a command that moves only the bid publishes only
    /// the bid, which is what makes this feed cheap. A `quantity` of zero means
    /// that side is now empty; a real level never rests at zero, because a level
    /// that reaches zero is removed.
    Bbo = 11,
    /// A symbol's trading state changed. `quantity` carries the
    /// [`TradingState`].
    ///
    /// Published, because a halt a client cannot see is a halt it will keep
    /// sending orders into. `symbol` names the instrument.
    SymbolState = 12,
    /// An account's ability to open new risk changed. `quantity` is 0 for
    /// stopped and 1 for permitted.
    ///
    /// Reaches the account's own private channel, so a client learns it has been
    /// stopped rather than inferring it from rejections.
    AccountTrading = 13,
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
    ///
    /// Typed, so a client reporting why it was refused can print the reason's
    /// message rather than a number an operator has to look up in this file.
    #[must_use]
    pub fn reject_reason(&self) -> Option<RejectReason> {
        match self.reject_reason {
            0 => Some(RejectReason::None),
            1 => Some(RejectReason::UnknownAccount),
            2 => Some(RejectReason::UnknownSymbol),
            3 => Some(RejectReason::QuantityZero),
            4 => Some(RejectReason::QuantityTooLarge),
            5 => Some(RejectReason::DuplicateOrderId),
            6 => Some(RejectReason::UnknownOrderId),
            7 => Some(RejectReason::OutsidePriceBand),
            8 => Some(RejectReason::InsufficientBalance),
            9 => Some(RejectReason::OrderLimitReached),
            10 => Some(RejectReason::WouldCross),
            11 => Some(RejectReason::InsufficientLiquidity),
            12 => Some(RejectReason::AmendWouldIncrease),
            13 => Some(RejectReason::SelfMatchPrevented),
            14 => Some(RejectReason::UnsupportedTimeInForce),
            15 => Some(RejectReason::EngineCapacity),
            16 => Some(RejectReason::NotAuthenticated),
            17 => Some(RejectReason::RateLimited),
            18 => Some(RejectReason::OrderIdNotIncreasing),
            19 => Some(RejectReason::SymbolNotTrading),
            20 => Some(RejectReason::AccountNotTrading),
            21 => Some(RejectReason::NotPermitted),
            _ => None,
        }
    }

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
            9 => Some(EventKind::Challenge),
            10 => Some(EventKind::Authenticated),
            11 => Some(EventKind::Bbo),
            12 => Some(EventKind::SymbolState),
            13 => Some(EventKind::AccountTrading),
            _ => None,
        }
    }

    /// When the venue read this command off the wire, in nanoseconds since the
    /// Unix epoch. Meaningful only on [`EventKind::Received`].
    ///
    /// Journalled inside the command, so a replay reproduces it rather than
    /// re-reading a clock — which is what keeps recovery deterministic. Zero
    /// when the venue was configured without timestamps.
    ///
    /// Taken once per pass rather than once per command: every command in a
    /// group was read from its socket in the same pass, so the resolution is the
    /// pass and not the packet. True per-packet arrival has to come from the NIC
    /// (`SO_TIMESTAMPING`); a reading taken in the gateway measures our own
    /// scheduling as well as the network, and pretending otherwise is the lie
    /// this field spent its first life telling by being always zero.
    #[must_use]
    pub const fn ingress_ns(&self) -> u64 {
        self.quantity
    }

    /// When the group containing this command began matching. Meaningful only on
    /// [`EventKind::Received`].
    ///
    /// **Not journalled**, and so not reproduced by replay: a recovered venue
    /// re-emits these as zero. It is a measurement of the run, not a fact about
    /// the order, and journalling it would need a field the 64-byte command
    /// record does not have.
    #[must_use]
    pub const fn match_ns(&self) -> u64 {
        self.price as u64
    }

    /// The nonce carried by an [`EventKind::Challenge`]. Meaningless for any
    /// other kind.
    #[must_use]
    pub fn challenge(&self) -> [u8; CHALLENGE_LEN] {
        let mut nonce = [0_u8; CHALLENGE_LEN];
        nonce[..8].copy_from_slice(&self.order_id.to_le_bytes());
        nonce[8..].copy_from_slice(&self.counterparty_order_id.to_le_bytes());
        nonce
    }

    /// Builds the challenge sent to a session on connect.
    #[must_use]
    pub fn challenging(nonce: [u8; CHALLENGE_LEN]) -> Self {
        let half = |bytes: &[u8]| u64::from_le_bytes(bytes.try_into().expect("eight bytes"));
        Self {
            kind: EventKind::Challenge as u8,
            order_id: half(&nonce[..8]),
            counterparty_order_id: half(&nonce[8..]),
            ..Self::default()
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
            3 => Some(ChannelKind::Bbo),
            _ => None,
        }
    }

    /// True for a message that configures the connection rather than changing
    /// the venue. These are handled by the gateway and never journalled.
    ///
    /// `Authenticate` is listed even though it is taken out of the stream
    /// earlier, so that a second one from an already-authenticated session
    /// cannot reach the journal by the ordinary path.
    #[must_use]
    pub const fn is_session_control(&self) -> bool {
        self.kind == CommandKind::Subscribe as u8
            || self.kind == CommandKind::Unsubscribe as u8
            || self.kind == CommandKind::QueryOpenOrders as u8
            || self.kind == CommandKind::CancelOnDisconnect as u8
            || self.kind == CommandKind::Authenticate as u8
    }

    /// Whether this command may only come from the venue's admin account.
    ///
    /// These are *not* session control: they change venue state and are
    /// journalled like an order, because a replay that did not reapply a halt
    /// would rebuild a venue an operator never asked for. What makes them
    /// administrative is who may send them, and that is checked in the gateway
    /// before sequencing -- so an unauthorised one never reaches the journal.
    ///
    /// `CancelAll` is deliberately absent. An account cancelling its own orders
    /// needs no privilege, and requiring one would mean a client that has lost
    /// track of its own state cannot flatten itself.
    #[must_use]
    pub const fn is_administrative(&self) -> bool {
        self.kind == CommandKind::SetSymbolState as u8
            || self.kind == CommandKind::SetAccountTrading as u8
    }

    /// The 32 bytes proving a [`CommandKind::Authenticate`]. Meaningless for any
    /// other kind: authentication names no order and no price, so it reuses the
    /// four fields that would carry them.
    #[must_use]
    pub fn proof(&self) -> [u8; PROOF_LEN] {
        let mut proof = [0_u8; PROOF_LEN];
        proof[..8].copy_from_slice(&self.order_id.to_le_bytes());
        proof[8..16].copy_from_slice(&self.replacement_id.to_le_bytes());
        proof[16..24].copy_from_slice(&self.quantity.to_le_bytes());
        proof[24..].copy_from_slice(&self.price.to_le_bytes());
        proof
    }

    /// Builds the answer to a challenge. `proof` comes from signing the nonce
    /// with the account's secret; this only carries it.
    #[must_use]
    pub fn authenticating(account: AccountId, proof: [u8; PROOF_LEN]) -> Self {
        let word = |bytes: &[u8]| u64::from_le_bytes(bytes.try_into().expect("eight bytes"));
        Self {
            sequence: 0,
            ingress_ns: 0,
            account,
            order_id: word(&proof[..8]),
            replacement_id: word(&proof[8..16]),
            quantity: word(&proof[16..24]),
            price: i64::from_le_bytes(proof[24..].try_into().expect("eight bytes")),
            symbol: 0,
            kind: CommandKind::Authenticate as u8,
            side: 0,
            time_in_force: 0,
            order_type: 0,
        }
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
            9 => Some(CommandKind::Authenticate),
            10 => Some(CommandKind::SetSymbolState),
            11 => Some(CommandKind::SetAccountTrading),
            12 => Some(CommandKind::CancelAll),
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
/// `v3` adds symbol trading states and stopped accounts; `v2` added the
/// per-account highest order ID. A `v1` file is refused rather
/// than read: the journal is always authoritative, so rejecting an old snapshot
/// costs replay time, while reading one would restore a venue with no record of
/// which IDs had been used and quietly accept a replayed order.
pub const SNAPSHOT_MAGIC: [u8; 8] = *b"BXSNAPv3";

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
    /// Accounts with a highest-order-ID mark. One per account that has ever had
    /// an order accepted, so it is bounded by traders rather than by orders.
    pub order_id_marks: u64,
    /// Symbols whose trading state is not the default.
    pub symbol_states: u64,
    /// Accounts stopped from opening new risk.
    pub stopped_accounts: u64,
    /// Padding, and load-bearing.
    ///
    /// The records after this header are read straight out of the file with no
    /// copy, so each section has to land on its own alignment.
    /// [`SnapshotBalance`] holds `u128` and therefore wants 16, and it begins at
    /// the header's length plus a whole number of 64-byte orders -- so the
    /// header's own size decides whether that read is possible at all. At 40
    /// bytes it was not, and the zero-copy read failed in a way that reported
    /// itself as a truncated file.
    pub _pad: [u8; 8],
}

/// A multiple of 16, so every section that follows stays aligned. Asserted
/// rather than left to whoever edits the struct next.
const _: () = assert!(size_of::<SnapshotHeader>() == 64);
const _: () = assert!(size_of::<SnapshotHeader>().is_multiple_of(align_of::<SnapshotBalance>()));
const _: () = assert!(size_of::<SnapshotOrder>().is_multiple_of(align_of::<SnapshotBalance>()));

/// The highest order ID one account has had accepted.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout,
)]
#[repr(C)]
pub struct SnapshotOrderIdMark {
    pub account: AccountId,
    pub highest: OrderId,
}

const _: () = assert!(size_of::<SnapshotOrderIdMark>() == 16);

/// One symbol's trading state, for symbols not in the default state.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout,
)]
#[repr(C)]
pub struct SnapshotSymbolState {
    pub symbol: SymbolId,
    pub state: u8,
    pub _pad: [u8; 3],
}

const _: () = assert!(size_of::<SnapshotSymbolState>() == 8);

/// One account stopped from opening new risk.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, FromBytes, IntoBytes, Immutable, KnownLayout,
)]
#[repr(C)]
pub struct SnapshotStoppedAccount {
    pub account: AccountId,
}

const _: () = assert!(size_of::<SnapshotStoppedAccount>() == 8);

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
            (9, CommandKind::Authenticate),
            (10, CommandKind::SetSymbolState),
            (11, CommandKind::SetAccountTrading),
            (12, CommandKind::CancelAll),
        ] {
            let mut command = sample();
            command.kind = byte;
            assert_eq!(command.kind(), Some(kind));
            assert!(command.is_well_formed());
        }
        let mut unknown = sample();
        unknown.kind = 13;
        assert!(unknown.kind().is_none());
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
            (9, EventKind::Challenge),
            (10, EventKind::Authenticated),
            (11, EventKind::Bbo),
            (12, EventKind::SymbolState),
            (13, EventKind::AccountTrading),
        ] {
            let event = Event {
                kind: byte,
                ..Event::default()
            };
            assert_eq!(event.kind(), Some(kind));
        }
        assert!(
            Event {
                kind: 14,
                ..Event::default()
            }
            .kind()
            .is_none()
        );
    }

    #[test]
    fn a_challenge_round_trips_through_the_record() {
        let nonce = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3];
        let event = Event::challenging(nonce);
        assert_eq!(event.kind(), Some(EventKind::Challenge));
        assert_eq!(event.challenge(), nonce);
    }

    #[test]
    fn a_proof_round_trips_through_the_record() {
        let mut proof = [0_u8; PROOF_LEN];
        for (index, byte) in proof.iter_mut().enumerate() {
            // Distinct per byte, so a field packed in the wrong order shows up
            // rather than passing on a symmetric pattern.
            *byte = u8::try_from(index).expect("32 fits in a byte") ^ 0xa5;
        }
        let command = Command::authenticating(77, proof);
        assert_eq!(command.kind(), Some(CommandKind::Authenticate));
        assert_eq!(command.account, 77);
        assert_eq!(command.proof(), proof);
        assert!(
            command.is_session_control(),
            "an authentication must never reach the journal"
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
            order_id_marks: 4,
            symbol_states: 5,
            stopped_accounts: 6,
            _pad: [0; 8],
        };
        assert_eq!(
            SnapshotHeader::read_from_bytes(header.as_bytes()),
            Ok(header)
        );
    }

    #[test]
    fn every_reject_reason_has_a_distinct_message() {
        let mut messages: Vec<String> = ALL_REASONS.iter().map(ToString::to_string).collect();
        let total = messages.len();
        messages.sort_unstable();
        messages.dedup();
        assert_eq!(messages.len(), total, "two reject reasons share a message");
    }

    /// Every reason this version defines. The old inline list had quietly
    /// fallen two variants behind the enum, so the distinct-message test was
    /// not checking the reasons most likely to be new.
    const ALL_REASONS: [RejectReason; 22] = [
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
        RejectReason::NotAuthenticated,
        RejectReason::RateLimited,
        RejectReason::OrderIdNotIncreasing,
        RejectReason::SymbolNotTrading,
        RejectReason::AccountNotTrading,
        RejectReason::NotPermitted,
    ];

    /// `Event::reject_reason` is a hand-written match, so a variant added to
    /// the enum and forgotten there decodes to `None` -- and a client that
    /// reports why it was refused goes silent for exactly the newest reason.
    #[test]
    fn every_reject_reason_survives_the_wire_and_back() {
        for reason in ALL_REASONS {
            let event = Event {
                reject_reason: reason as u8,
                ..Event::default()
            };
            assert_eq!(
                event.reject_reason(),
                Some(reason),
                "discriminant {} does not decode back to {reason:?}; \
                 Event::reject_reason has fallen behind the enum",
                reason as u8
            );
        }
        // One past the last known discriminant is not a reason. When adding a
        // variant, extend ALL_REASONS and move this boundary up.
        let unknown = Event {
            reject_reason: ALL_REASONS.len() as u8,
            ..Event::default()
        };
        assert_eq!(unknown.reject_reason(), None);
        // And the count everything else sizes its tables from agrees.
        assert_eq!(
            RejectReason::COUNT,
            ALL_REASONS.len(),
            "RejectReason::COUNT is behind the enum, so a table sized from it              will be indexed out of bounds by the newest reason"
        );
    }
}
