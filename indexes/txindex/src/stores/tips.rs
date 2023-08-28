use std::collections::HashSet;
use std::sync::Arc;

use kaspa_consensus_core::BlockHashSet;
use kaspa_database::prelude::StoreResult;
use kaspa_database::prelude::DB;
use kaspa_database::prelude::{CachedDbItem, DirectDbWriter};
use kaspa_hashes::Hash;

// TODO (when pruning is implemented): Use this store to check sync and resync from earliest header pruning point. 
pub const STORE_PREFIX: &[u8] = b"txindex-tips";


/// Reader API for `Source`.
pub trait TxIndexTipsReader {
    fn get(&self) -> StoreResult<HashSet>;
}

pub trait TxIndexTips: TxIndexTipsReader {
    fn set(&mut self, tips: BlockHashSet) -> StoreResult<()>;
    fn add(&mut self, tip: Hash) -> StoreResult<()>;
    fn remove(&mut self, merged_block_hashes: BlockHashSet) -> StoreResult<()>;
    fn clear(&mut self) -> StoreResult<()>;
}

/// A DB + cache implementation of `Source` trait, with concurrent readers support.
#[derive(Clone)]
pub struct DbTxIndexTips {
    db: Arc<DB>,
    access: CachedDbItem<BlockHashSet, BlockHasher>,
}

const STORE_PREFIX: &[u8] = b"txindex-pruning-point";

impl DbTxIndexTips {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbItem::new(db.clone(), STORE_PREFIX.to_vec()) }
    }

    pub fn clone_with_new_cache(&self) -> Self {
        Self::new(Arc::clone(&self.db))
    }
}

impl TxIndexTipsReader for DbTxIndexTips {
    fn get(&self) -> StoreResult<HashSet> {
        self.access.read()
    }
}

impl TxIndexTips for DbTxIndexTips {
    fn set(&mut self, tips: Hash) -> StoreResult<()> {
        self.access.write(DirectDbWriter::new(&self.db), &source)
    }

    fn add(&mut self, tip: Hash) -> StoreResult<()> {
        self.access.update(DirectDbWriter::new(&self.db), move |tips: BlockHashSet<Hash>| { tips.insert(tip); tips } )
    }

    fn remove(&mut self, merged_block_hashes_1: BlockHashSet<Hash>, merged_block_hashes_2: HashSet<Hash>) -> StoreResult<()> {
        let merged_block_hashes_3 = &merged_block_hashes_1 - &merged_block_hashes_2;
        self.access.update(DirectDbWriter::new(&self.db), move |tips: BlockHashSet<Hash>| (&tips - &merged_block_hashes) )
    }
}
