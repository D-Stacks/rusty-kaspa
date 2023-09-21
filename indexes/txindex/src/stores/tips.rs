use std::collections::HashSet;
use std::ops::Sub;
use std::sync::Arc;

use kaspa_consensus_core::BlockHashSet;
use kaspa_database::{prelude::{CachedDbItem, DirectDbWriter, DB, StoreResult}, registry::DatabaseStorePrefixes};
use kaspa_hashes::Hash;

/// Reader API for `Source`.
pub trait TxIndexTipsReader {
    fn get(&self) -> StoreResult<HashSet>;
}

pub trait TxIndexTipsStore: TxIndexTipsReader {
    fn set(&mut self, tips: BlockHashSet) -> StoreResult<()>;
    fn update_add_tip(&mut self, tip: Hash) -> StoreResult<()>;
    fn update_remove_tips(&mut self, merged_block_hashes: BlockHashSet) -> StoreResult<()>;
    fn remove(&mut self) -> StoreResult<()>;
}

/// A DB + cache implementation of `Source` trait, with concurrent readers support.
#[derive(Clone)]
pub struct DbTxIndexTipsStore {
    db: Arc<DB>,
    access: CachedDbItem<BlockHashSet>,
}


impl DbTxIndexTipsStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbItem::new(db.clone(), DatabaseStorePrefixes::TxIndexTips) }
    }

    pub fn clone_with_new_cache(&self) -> Self {
        Self::new(Arc::clone(&self.db))
    }
}

impl TxIndexTipsReader for DbTxIndexTipsStore {
    fn get(&self) -> StoreResult<BlockHashSet> {
        self.access.read()
    }
}

impl TxIndexTipsStore for DbTxIndexTipsStore {
    fn set(&mut self, tips: Hash) -> StoreResult<()> {
        self.access.write(DirectDbWriter::new(&self.db), &source)
    }

    fn update_add_tip(&mut self, tip: Hash) -> StoreResult<()> {
        self.access.update(DirectDbWriter::new(&self.db), move |tips: BlockHashSet | { tips.insert(tip); tips } )
    }

    fn update_remove_tips(&mut self, merged_block_hashes: BlockHashSet ) -> StoreResult<()> {
        self.access.update(DirectDbWriter::new(&self.db), move |tips: BlockHashSet | tips.sub(&merged_block_hashes) )
    }

    fn remove(&mut self) -> StoreResult<()> {
        self.access.remove(DirectDbWriter::new(&self.db))
    } 
}
