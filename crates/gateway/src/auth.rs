//! Proving which account a session may act for.
//!
//! Before this existed, a session stated its account on its first command and
//! was believed. Every other risk check was real; that one was a placeholder.
//!
//! The shape is decided by the transport. The venue takes no TLS deliberately —
//! a market maker on a cross-connect wants nothing between it and the book — so
//! anything the client *sends* can be read and replayed by anyone on the path. A
//! bearer token, an API key, a password: all worthless here, because capturing
//! one is the same as knowing it.
//!
//! So the secret is never sent. The venue puts a fresh 16-byte nonce on the wire
//! the moment a connection is accepted, the client returns
//! `HMAC-SHA256(secret, nonce)`, and the venue checks that against the secret it
//! already holds. An eavesdropper learns a nonce and a tag, and neither is worth
//! anything: the nonce will never be issued again, so the tag answers a question
//! nobody will be asked twice.
//!
//! What this does *not* do is protect the rest of the session. Authentication
//! establishes identity at connect; it does not encrypt or authenticate the
//! orders that follow, so an attacker who can write to the wire can still inject
//! them. That is the accepted cost of an unencrypted transport, and the answer
//! is a private link rather than a protocol change — which is exactly the
//! deployment this transport was chosen for. Say it plainly rather than let a
//! reader assume `Authenticated` means more than it does.
//!
//! It happens in the gateway, before the sequencer, so no key lookup and no
//! nonce can reach the deterministic path.

use bx_protocol::{AccountId, CHALLENGE_LEN, PROOF_LEN};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;

/// Bytes in an account's shared secret. 32, because a secret shorter than the
/// hash it feeds is the weakest link, and one longer buys nothing.
pub const SECRET_LEN: usize = 32;

/// Whether the venue demands proof of identity.
///
/// Stated rather than defaulted. A venue that is open because nobody set a key
/// looks exactly like one that is open on purpose, and the difference matters
/// enough that the configuration has to say which it is.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    /// Every session proves who it is before it can do anything else.
    Required,
    /// No proof asked for; a session declares its account on its first command.
    /// For measurement and local runs, never for a venue holding real balances.
    Open,
}

/// The accounts that may connect, and the secret each one signs with.
///
/// A `HashMap` rather than the venue's `FastMap`, and deliberately: the account
/// in an `Authenticate` is whatever an unauthenticated stranger chose to send,
/// which is exactly the hash-flooding input SipHash exists to resist. Everywhere
/// else the key is a number the venue itself validated, which is why the faster
/// hasher is right there and wrong here. This lookup happens once per
/// connection, so the difference costs nothing.
#[derive(Clone, Debug, Default)]
pub struct Credentials {
    keys: HashMap<AccountId, [u8; SECRET_LEN]>,
}

impl Credentials {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, account: AccountId, secret: [u8; SECRET_LEN]) {
        self.keys.insert(account, secret);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// True if `proof` is what this account's secret produces over `nonce`.
    ///
    /// An unknown account and a wrong proof return the same thing on purpose: a
    /// caller that could tell them apart would let anyone enumerate which
    /// accounts exist.
    #[must_use]
    pub fn verifies(
        &self,
        account: AccountId,
        nonce: &[u8; CHALLENGE_LEN],
        proof: &[u8; PROOF_LEN],
    ) -> bool {
        let Some(secret) = self.keys.get(&account) else {
            return false;
        };
        // Constant-time inside `hmac`: comparing tags with `==` leaks how many
        // leading bytes were right, which is enough to forge one byte at a time.
        expected(secret, nonce).verify_slice(proof).is_ok()
    }
}

/// The proof a client returns for a challenge. The client side of
/// [`Credentials::verifies`], and the only thing a client needs to implement.
#[must_use]
pub fn prove(secret: &[u8; SECRET_LEN], nonce: &[u8; CHALLENGE_LEN]) -> [u8; PROOF_LEN] {
    expected(secret, nonce).finalize().into_bytes().into()
}

fn expected(secret: &[u8; SECRET_LEN], nonce: &[u8; CHALLENGE_LEN]) -> Hmac<Sha256> {
    // Infallible for HMAC: it accepts a key of any length.
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC takes any key length");
    mac.update(nonce);
    mac
}

/// A nonce no client can predict.
///
/// From the OS, not from a counter or a clock. A predictable nonce lets an
/// attacker who once saw a valid proof pre-compute the answer to a challenge
/// that has not been issued yet, which gives back the replay this whole design
/// exists to prevent.
///
/// # Panics
/// If the OS cannot supply randomness. Continuing with a nonce that is not
/// random would mean authenticating sessions that had proved nothing, which is
/// worse than refusing to run.
#[must_use]
pub fn nonce() -> [u8; CHALLENGE_LEN] {
    let mut bytes = [0_u8; CHALLENGE_LEN];
    getrandom::fill(&mut bytes).expect("the OS must be able to supply randomness");
    bytes
}

/// Parses a 32-byte secret written as 64 hex characters.
///
/// # Errors
/// Returns a description of what is wrong with the text.
pub fn secret_from_hex(text: &str) -> Result<[u8; SECRET_LEN], String> {
    let text = text.trim();
    if text.len() != SECRET_LEN * 2 {
        return Err(format!(
            "a secret is {} hex characters, not {}",
            SECRET_LEN * 2,
            text.len()
        ));
    }
    let mut secret = [0_u8; SECRET_LEN];
    for (index, byte) in secret.iter_mut().enumerate() {
        let pair = &text[index * 2..index * 2 + 2];
        *byte = u8::from_str_radix(pair, 16).map_err(|_| format!("`{pair}` is not hex"))?;
    }
    Ok(secret)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: [u8; SECRET_LEN] = [7; SECRET_LEN];

    fn credentials() -> Credentials {
        let mut credentials = Credentials::new();
        credentials.insert(1, SECRET);
        credentials
    }

    #[test]
    fn a_correct_proof_is_accepted() {
        let nonce = nonce();
        assert!(credentials().verifies(1, &nonce, &prove(&SECRET, &nonce)));
    }

    #[test]
    fn a_proof_for_a_different_nonce_is_refused() {
        // The property the whole design rests on: a captured proof is worthless
        // against the next connection, because the nonce will never repeat.
        let captured = prove(&SECRET, &nonce());
        assert!(!credentials().verifies(1, &nonce(), &captured));
    }

    #[test]
    fn another_accounts_secret_does_not_open_this_account() {
        let mut credentials = credentials();
        credentials.insert(2, [9; SECRET_LEN]);
        let nonce = nonce();
        assert!(!credentials.verifies(1, &nonce, &prove(&[9; SECRET_LEN], &nonce)));
    }

    #[test]
    fn an_unknown_account_is_refused_like_a_bad_proof() {
        let nonce = nonce();
        assert!(!credentials().verifies(404, &nonce, &prove(&SECRET, &nonce)));
    }

    #[test]
    fn nonces_do_not_repeat() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1_000 {
            assert!(seen.insert(nonce()), "a nonce repeated");
        }
    }

    #[test]
    fn a_secret_round_trips_through_hex() {
        let text = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let secret = secret_from_hex(text).unwrap();
        assert_eq!(secret[0], 0x00);
        assert_eq!(secret[1], 0x11);
        assert_eq!(secret[31], 0xff);
    }

    #[test]
    fn a_malformed_secret_is_reported_rather_than_padded() {
        assert!(secret_from_hex("abcd").is_err(), "short secret accepted");
        assert!(
            secret_from_hex(&"zz".repeat(32)).is_err(),
            "non-hex accepted"
        );
    }
}
