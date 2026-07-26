//! Session handling, wire framing, and the loop that drives the exchange.
//!
//! The pipeline crate knows how to apply a command and when a group is durable.
//! This crate is what turns a stream of bytes from a socket into those groups
//! and the resulting events back into bytes, without the exchange ever learning
//! what a socket is.

pub mod codec;
pub mod config;
pub mod tcp;
pub mod venue;

pub use codec::{Decoder, FRAME_LEN, encode};
pub use config::Config;
pub use tcp::Server;
pub use venue::Venue;
