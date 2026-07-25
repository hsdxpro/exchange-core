//! A fast hasher for integer keys.
//!
//! Every lookup on the command path is keyed by an integer: order ID, account
//! ID, asset ID. The standard library defaults to SipHash, which is designed to
//! resist hash-flooding from untrusted input and costs roughly 20–30 ns per
//! lookup. At seven lookups per command that alone is most of our budget.
//!
//! These keys are not untrusted in the sense SipHash defends against: they are
//! integers the venue itself assigned or validated, and a client cannot choose
//! them freely enough to force collisions. So we use the multiply-xor-rotate
//! finalizer that `rustc` itself uses (FxHash), which is a few instructions.
//!
//! The hasher has no random seed, so iteration order is stable across runs.
//! Nothing in the pipeline depends on that, but determinism is the property the
//! whole design rests on and it costs nothing to preserve here too.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// Odd constant with well-distributed bits, from the FxHash family.
const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

#[derive(Clone, Copy, Debug, Default)]
pub struct FastHasher {
    hash: u64,
}

impl FastHasher {
    #[inline]
    fn mix(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(SEED);
    }
}

impl Hasher for FastHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = [0_u8; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.mix(u64::from_le_bytes(word));
        }
    }

    #[inline]
    fn write_u8(&mut self, n: u8) {
        self.mix(u64::from(n));
    }

    #[inline]
    fn write_u32(&mut self, n: u32) {
        self.mix(u64::from(n));
    }

    #[inline]
    fn write_u64(&mut self, n: u64) {
        self.mix(n);
    }

    #[inline]
    fn write_usize(&mut self, n: usize) {
        self.mix(n as u64);
    }
}

/// A `HashMap` keyed by integers, without the SipHash tax.
pub type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<FastHasher>>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::Hash;

    fn hash_of<T: Hash>(value: &T) -> u64 {
        let mut hasher = FastHasher::default();
        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn distinct_keys_hash_distinctly_over_a_realistic_range() {
        // Order IDs are sequential, which is the pattern that breaks naive
        // hashers by leaving low bits correlated.
        let hashes: std::collections::HashSet<u64> =
            (0_u64..100_000).map(|k| hash_of(&k)).collect();
        assert_eq!(hashes.len(), 100_000, "sequential keys collided");
    }

    #[test]
    fn tuple_keys_distinguish_their_components() {
        assert_ne!(hash_of(&(1_u64, 2_u32)), hash_of(&(2_u64, 1_u32)));
        assert_ne!(hash_of(&(1_u64, 2_u32)), hash_of(&(1_u64, 3_u32)));
    }

    #[test]
    fn hashing_is_stable_across_instances_so_replay_is_reproducible() {
        assert_eq!(hash_of(&42_u64), hash_of(&42_u64));
    }

    #[test]
    fn the_map_behaves_like_a_hashmap() {
        let mut map: FastMap<u64, u32> = FastMap::default();
        for i in 0..10_000 {
            map.insert(i, i as u32 * 2);
        }
        assert_eq!(map.len(), 10_000);
        assert_eq!(map.get(&5_000), Some(&10_000));
        assert_eq!(map.remove(&5_000), Some(10_000));
        assert_eq!(map.get(&5_000), None);
    }
}
