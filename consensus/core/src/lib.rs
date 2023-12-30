extern crate alloc;
extern crate core;
extern crate self as consensus_core;

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::ops::BitXor;
use std::sync::Arc;

pub use kaspa_hashes::Hash;
use kaspa_hashes::ZERO_HASH;

pub mod acceptance_data;
pub mod api;
pub mod block;
pub mod block_count;
pub mod blockhash;
pub mod blockstatus;
pub mod coinbase;
pub mod config;
pub mod constants;
pub mod daa_score_timestamp;
pub mod errors;
pub mod hashing;
pub mod header;
pub mod mass;
pub mod merkle;
pub mod muhash;
pub mod network;
pub mod pruning;
pub mod sign;
pub mod subnets;
pub mod trusted;
pub mod tx;
pub mod utxo;

/// Integer type for accumulated PoW of blue blocks. We expect no more than
/// 2^128 work in a single block (btc has ~2^80), and no more than 2^64
/// overall blocks, so 2^192 is definitely a justified upper-bound.
pub type BlueWorkType = kaspa_math::Uint192;

/// The type used to represent the GHOSTDAG K parameter
pub type KType = u16;

/// Map from Block hash to K type
pub type HashKTypeMap = std::sync::Arc<BlockHashMap<KType>>;

/// This HashMap skips the hashing of the key and uses the key directly as the hash.
/// Should only be used for block hashes that have correct DAA,
/// otherwise it is susceptible to DOS attacks via hash collisions.
pub type BlockHashMap<V> = HashMap<Hash, V, BlockHasher>;

/// Same as `BlockHashMap` but a `HashSet`.
pub type BlockHashSet = HashSet<Hash, BlockHasher>;

pub type TxHashMap<V> = HashMap<Hash, V, TxHasher>;

pub type TxHashSet = HashSet<Hash, TxHasher>;

pub trait HashMapCustomHasher {
    fn new() -> Self;
    fn with_capacity(capacity: usize) -> Self;
}

// HashMap::new and HashMap::with_capacity are only implemented on Hasher=RandomState
// to avoid type inference problems, so we need to provide our own versions.
impl<V> HashMapCustomHasher for BlockHashMap<V> {
    #[inline(always)]
    fn new() -> Self {
        Self::with_hasher(BlockHasher::new())
    }
    #[inline(always)]
    fn with_capacity(cap: usize) -> Self {
        Self::with_capacity_and_hasher(cap, BlockHasher::new())
    }
}

pub trait HashMapCustomHasher2 {
    fn new(block_hash: Hash) -> Self;
    fn with_capacity(block_hash: Hash, capacity: usize) -> Self;
}

impl HashMapCustomHasher2 for TxHashSet {
    #[inline(always)]
    fn new(block_hash: Hash) -> Self {
        Self::with_hasher(TxHasher::new(block_hash))
    }
    #[inline(always)]
    fn with_capacity(block_hash: Hash, capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, TxHasher::new(block_hash))
    }
}

impl <V> HashMapCustomHasher2 for TxHashMap<V> {
    #[inline(always)]
    fn new(block_hash: Hash) -> Self {
        Self::with_hasher(TxHasher::new(block_hash))
    }
    #[inline(always)]
    fn with_capacity(block_hash: Hash, capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, TxHasher::new(block_hash))
    }
}

impl HashMapCustomHasher for BlockHashSet {
    #[inline(always)]
    fn new() -> Self {
        Self::with_hasher(BlockHasher::new())
    }
    #[inline(always)]
    fn with_capacity(cap: usize) -> Self {
        Self::with_capacity_and_hasher(cap, BlockHasher::new())
    }
}

#[derive(Default, Debug)]
pub struct ChainPath {
    pub added: Vec<Hash>,
    pub removed: Vec<Hash>,
}

impl ChainPath {
    pub fn new(added: Vec<Hash>, removed: Vec<Hash>) -> Self {
        Self { added, removed }
    }

    // Retrieves the checkpoint [],
    pub fn checkpoint_hash(&self) -> Option<&Hash> {
        self.added.last().or(self.removed.last())
    }
}

pub struct TxHasher{
    block_hash_uint64: u64,
    tx_block_hash_uint64: u64,
}

impl TxHasher {
    #[inline(always)]
    pub fn new(block_hash: Hash) -> Self {
        Self {
            block_hash_uint64: block_hash.to_le_u64()[0],
            tx_block_hash_uint64: 0u64,
        }
    }
}

impl Hasher for TxHasher {

    #[inline(always)]
    fn finish(&self) -> u64 {
        self.tx_block_hash_uint64
    }
    #[inline(always)]
    fn write_u64(&mut self, v: u64) {
        self.tx_block_hash_uint64 = v.bitxor(self.block_hash_uint64);
    }

    #[cold]
    fn write(&mut self, _: &[u8]) {
        unimplemented!("use write_u64")
    }
}

/// `hashes::Hash` writes 4 u64s so we just use the last one as the hash here
#[derive(Default, Clone, Copy)]
pub struct BlockHasher(u64);

impl BlockHasher {
    #[inline(always)]
    pub const fn new() -> Self {
        Self(0)
    }
}

impl Hasher for BlockHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline(always)]
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
    #[cold]
    fn write(&mut self, _: &[u8]) {
        unimplemented!("use write_u64")
    }
}

impl BuildHasher for TxHasher {
    type Hasher = Self;

    #[inline(always)]
    fn build_hasher(&self) -> Self::Hasher {
        Self::new(ZERO_HASH)
    }
}

impl BuildHasher for BlockHasher {
    type Hasher = Self;

    #[inline(always)]
    fn build_hasher(&self) -> Self::Hasher {
        Self(0)
    }
}

pub type BlockLevel = u8;

#[cfg(test)]
mod tests {
    use super::BlockHasher;
    use kaspa_hashes::Hash;
    use std::hash::{Hash as _, Hasher as _};
    #[test]
    fn test_block_hasher() {
        let hash = Hash::from_le_u64([1, 2, 3, 4]);
        let mut hasher = BlockHasher::default();
        hash.hash(&mut hasher);
        assert_eq!(hasher.finish(), 4);
    }
}
