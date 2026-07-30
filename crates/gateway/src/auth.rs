//! Proving which account a session may act for.
//!
//! Before this existed, a session stated its account on its first command and
//! was believed. Every other risk check was real; that one was a placeholder.
//!
//! The venue puts a fresh 16-byte nonce on the wire the moment a connection is
//! accepted, and the client returns an Ed25519 signature over it. The venue
//! holds only the account's **public** key, so a breach of the venue's own
//! configuration gives an attacker nothing they can sign with -- which is the
//! reason this is not an HMAC. It used to be one, and an HMAC secret is
//! symmetric: whoever reads the venue's key file can forge every account's
//! logon. The industry has moved the same way, Binance having deprecated its
//! HMAC keys in favour of Ed25519.
//!
//! ## What is signed, and why it is not just the nonce
//!
//! The message is [`AUTH_DOMAIN`] followed by the nonce, never the bare nonce.
//! The key here may well be one the account already uses elsewhere -- on chain,
//! say -- and a wallet asked to sign an opaque 16-byte string in some other
//! context would produce exactly what a bare-nonce challenge accepts. That
//! signature would then be a valid logon. Prefixing a domain string makes a
//! signature for this venue unmistakably for this venue. Ethereum's EIP-191
//! prefix exists for precisely this reason.
//!
//! Cross-*venue* replay is covered by the nonce being random per connection: a
//! signature captured against one venue answers a challenge no other venue will
//! ever issue.
//!
//! ## Verification is strict
//!
//! [`VerifyingKey::verify_strict`] rather than `verify`. The permissive form
//! accepts non-canonical encodings and small-order public keys, which means one
//! signature can be re-encoded into a different byte string that still verifies.
//! For a venue that is not academic: any place a signature is treated as an
//! identifier -- a log, an audit trail, a replay-protection cache -- can then be
//! given two spellings of the same authorisation.
//!
//! ## What this does *not* do
//!
//! Authentication establishes identity at connect. On a transport without TLS it
//! does not encrypt or authenticate the orders that follow, so an attacker who
//! can write to the wire can still inject them. That is the accepted cost of an
//! unencrypted cross-connect, and the answer is TLS or a private link rather
//! than a protocol change. Say it plainly rather than let a reader assume
//! `Authenticated` means more than it does.
//!
//! It happens in the gateway, before the sequencer, so no key lookup and no
//! nonce can reach the deterministic path.

use bx_protocol::{AUTH_DOMAIN, AccountId, CHALLENGE_LEN, PUBLIC_KEY_LEN, SIGNATURE_LEN};
use ed25519_dalek::{Signature, VerifyingKey};
use std::collections::HashMap;

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

/// The accounts that may connect, and the public key each one signs with.
///
/// A `HashMap` rather than the venue's `FastMap`, and deliberately: the account
/// in an `Authenticate` is whatever an unauthenticated stranger chose to send,
/// which is exactly the hash-flooding input SipHash exists to resist. Everywhere
/// else the key is a number the venue itself validated, which is why the faster
/// hasher is right there and wrong here. This lookup happens once per
/// connection, so the difference costs nothing.
#[derive(Clone, Debug, Default)]
pub struct Credentials {
    keys: HashMap<AccountId, VerifyingKey>,
}

impl Credentials {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an account's public key.
    ///
    /// # Errors
    /// If the bytes are not a valid Ed25519 public key.
    pub fn insert(
        &mut self,
        account: AccountId,
        public_key: [u8; PUBLIC_KEY_LEN],
    ) -> Result<(), String> {
        let key = VerifyingKey::from_bytes(&public_key)
            .map_err(|e| format!("not a valid Ed25519 public key: {e}"))?;
        self.keys.insert(account, key);
        Ok(())
    }

    /// Drops an account's key, so it can no longer authenticate.
    ///
    /// The immediate half of revoking a compromised key. The lasting half is
    /// removing it from the configuration, because that is where it comes back
    /// from on a restart.
    ///
    /// Returns whether there was a key to drop.
    pub fn revoke(&mut self, account: AccountId) -> bool {
        self.keys.remove(&account).is_some()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Whether a key is held for this account.
    ///
    /// For validating configuration, not for admitting sessions: answering this
    /// on the wire would let anyone enumerate the venue's accounts, which is why
    /// [`Self::verifies`] treats an unknown account and a bad signature alike.
    #[must_use]
    pub fn knows(&self, account: AccountId) -> bool {
        self.keys.contains_key(&account)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// True if `signature` is this account's, over this nonce, for this venue.
    ///
    /// An unknown account and a bad signature return the same thing on purpose:
    /// a caller that could tell them apart would let anyone enumerate which
    /// accounts exist.
    #[must_use]
    pub fn verifies(
        &self,
        account: AccountId,
        nonce: &[u8; CHALLENGE_LEN],
        signature: &[u8; SIGNATURE_LEN],
    ) -> bool {
        let Some(key) = self.keys.get(&account) else {
            return false;
        };
        // Strict: see the module note. `verify` would accept a re-encoding of
        // the same signature and a small-order key.
        key.verify_strict(&signed_message(nonce), &Signature::from_bytes(signature))
            .is_ok()
    }
}

/// Exactly the bytes a client signs: the domain, then the nonce.
///
/// One function, used by the venue to verify and by a client to sign, so the two
/// cannot drift apart. A mismatch here does not fail loudly -- it fails as every
/// signature being rejected, which reads like a client bug.
#[must_use]
pub fn signed_message(nonce: &[u8; CHALLENGE_LEN]) -> Vec<u8> {
    let mut message = Vec::with_capacity(AUTH_DOMAIN.len() + CHALLENGE_LEN);
    message.extend_from_slice(AUTH_DOMAIN);
    message.extend_from_slice(nonce);
    message
}

/// A nonce no client can predict.
///
/// From the OS, not from a counter or a clock. A predictable nonce lets an
/// attacker who once saw a valid signature pre-compute the answer to a challenge
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

/// Parses 32 key bytes written as 64 hex characters.
///
/// Used for both halves of a keypair -- an account's public key from the
/// configuration file, and the venue's own signing seed from the file that holds
/// it. Named for the bytes rather than for either role, because the parsing is
/// the same and having two of these would mean the next fix landed in one.
///
/// # Errors
/// Returns a description of what is wrong with the text.
pub fn key_bytes_from_hex(text: &str) -> Result<[u8; PUBLIC_KEY_LEN], String> {
    let text = text.trim();
    if text.len() != PUBLIC_KEY_LEN * 2 {
        return Err(format!(
            "a key is {} hex characters, not {}",
            PUBLIC_KEY_LEN * 2,
            text.len()
        ));
    }
    let mut key = [0_u8; PUBLIC_KEY_LEN];
    for (index, byte) in key.iter_mut().enumerate() {
        let pair = &text[index * 2..index * 2 + 2];
        *byte = u8::from_str_radix(pair, 16).map_err(|_| format!("`{pair}` is not hex"))?;
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    fn keypair(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn credentials(account: AccountId, key: &SigningKey) -> Credentials {
        let mut credentials = Credentials::new();
        credentials
            .insert(account, key.verifying_key().to_bytes())
            .unwrap();
        credentials
    }

    fn sign(key: &SigningKey, nonce: &[u8; CHALLENGE_LEN]) -> [u8; SIGNATURE_LEN] {
        key.sign(&signed_message(nonce)).to_bytes()
    }

    #[test]
    fn a_correct_signature_is_accepted() {
        let key = keypair(7);
        let nonce = nonce();
        assert!(credentials(1, &key).verifies(1, &nonce, &sign(&key, &nonce)));
    }

    #[test]
    fn another_accounts_key_does_not_authenticate() {
        let mine = keypair(7);
        let theirs = keypair(9);
        let nonce = nonce();
        let credentials = credentials(1, &mine);
        assert!(!credentials.verifies(1, &nonce, &sign(&theirs, &nonce)));
    }

    #[test]
    fn an_unknown_account_is_refused_like_a_bad_signature() {
        let key = keypair(7);
        let nonce = nonce();
        assert!(!credentials(1, &key).verifies(2, &nonce, &sign(&key, &nonce)));
    }

    /// A signature answers one challenge and no other.
    #[test]
    fn a_signature_for_one_nonce_does_not_answer_another() {
        let key = keypair(7);
        let credentials = credentials(1, &key);
        let first = nonce();
        let second = nonce();
        assert_ne!(first, second, "two nonces collided, which is the real bug");
        assert!(!credentials.verifies(1, &second, &sign(&key, &first)));
    }

    /// The domain prefix is what it is for: a signature over the bare nonce --
    /// which is what a wallet signing an opaque string elsewhere would produce
    /// -- must not authenticate here.
    #[test]
    fn a_signature_over_the_bare_nonce_is_refused() {
        let key = keypair(7);
        let nonce = nonce();
        let bare = key.sign(&nonce).to_bytes();
        assert!(
            !credentials(1, &key).verifies(1, &nonce, &bare),
            "a signature made without this venue's domain prefix authenticated"
        );
    }

    /// And a different domain does not work either, so bumping the string is a
    /// real break rather than a decoration.
    #[test]
    fn a_signature_under_a_different_domain_is_refused() {
        let key = keypair(7);
        let nonce = nonce();
        let mut other = b"bx-venue-auth-v0".to_vec();
        other.extend_from_slice(&nonce);
        let signature = key.sign(&other).to_bytes();
        assert!(!credentials(1, &key).verifies(1, &nonce, &signature));
    }

    #[test]
    fn a_flipped_bit_anywhere_in_the_signature_is_refused() {
        let key = keypair(7);
        let nonce = nonce();
        let good = sign(&key, &nonce);
        let credentials = credentials(1, &key);
        for bit in 0..SIGNATURE_LEN * 8 {
            let mut bad = good;
            bad[bit / 8] ^= 1 << (bit % 8);
            assert!(
                !credentials.verifies(1, &nonce, &bad),
                "flipping bit {bit} still verified"
            );
        }
    }

    #[test]
    fn an_all_zero_signature_is_refused() {
        let key = keypair(7);
        assert!(!credentials(1, &key).verifies(1, &nonce(), &[0; SIGNATURE_LEN]));
    }

    /// A hostile key encoding either fails to register or can never authenticate.
    ///
    /// Which of the two depends on whether the bytes happen to decompress to a
    /// curve point, and that is the library's business rather than something to
    /// assert. What matters is that neither outcome lets anybody in, so the test
    /// says exactly that and stays true across dalek versions.
    #[test]
    fn a_hostile_public_key_never_authenticates() {
        for bytes in [
            [0xFF; PUBLIC_KEY_LEN],
            [0; PUBLIC_KEY_LEN],
            [1; PUBLIC_KEY_LEN],
        ] {
            let mut credentials = Credentials::new();
            if credentials.insert(1, bytes).is_err() {
                continue;
            }
            let nonce = nonce();
            for signature in [
                [0_u8; SIGNATURE_LEN],
                [1_u8; SIGNATURE_LEN],
                [0xFF_u8; SIGNATURE_LEN],
            ] {
                assert!(
                    !credentials.verifies(1, &nonce, &signature),
                    "key {bytes:?} accepted a signature nobody produced"
                );
            }
        }
    }

    /// The all-zero key encodes the identity element -- a small-order point.
    ///
    /// `verify_strict` rejects it. If this ever passes, verification has been
    /// switched to the permissive form, and a signature under such a key
    /// verifies against messages its holder never saw.
    #[test]
    fn a_small_order_key_cannot_authenticate() {
        let mut credentials = Credentials::new();
        if credentials.insert(1, [0; PUBLIC_KEY_LEN]).is_ok() {
            let nonce = nonce();
            for signature in [[0_u8; SIGNATURE_LEN], [1_u8; SIGNATURE_LEN]] {
                assert!(
                    !credentials.verifies(1, &nonce, &signature),
                    "a small-order key authenticated; verification is not strict"
                );
            }
        }
    }

    #[test]
    fn revoking_a_key_stops_it_authenticating() {
        let key = keypair(7);
        let nonce = nonce();
        let mut credentials = credentials(1, &key);
        assert!(credentials.verifies(1, &nonce, &sign(&key, &nonce)));

        assert!(credentials.revoke(1), "there was a key to revoke");
        assert!(
            !credentials.verifies(1, &nonce, &sign(&key, &nonce)),
            "a revoked key still authenticated"
        );
        assert!(
            !credentials.revoke(1),
            "revoking twice reported a key twice"
        );
    }

    #[test]
    fn a_public_key_round_trips_through_hex() {
        let key = keypair(7);
        let bytes = key.verifying_key().to_bytes();
        let text: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(key_bytes_from_hex(&text).unwrap(), bytes);
    }

    #[test]
    fn hex_of_the_wrong_length_or_with_junk_is_refused() {
        assert!(key_bytes_from_hex("00").is_err());
        assert!(key_bytes_from_hex(&"z".repeat(64)).is_err());
        assert!(key_bytes_from_hex(&"0".repeat(63)).is_err());
    }

    /// Two nonces in a row must differ. A counter or a clock here would let an
    /// attacker who saw one signature pre-compute the next answer.
    #[test]
    fn nonces_do_not_repeat() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1_000 {
            assert!(seen.insert(nonce()), "a nonce repeated within 1,000 draws");
        }
    }
}
