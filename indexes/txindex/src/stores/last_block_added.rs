use std::sync::Arc;

use kaspa_database::prelude::StoreResult;
use kaspa_database::prelude::DB;
use kaspa_database::prelude::{CachedDbItem, DirectDbWriter};
use kaspa_hashes::Hash;

//Use this store to check sync and resync from last_block_added. 
pub const STORE_NAME: &[u8] = b"txindex-last-block-added";


/// Reader API for `LastBlockAddedStore`.
pub trait LastBlockAddedStoreReader {
    fn get(&self) -> StoreResult<Option<Hash>>;
}

pub trait LastBlockAddedStore: LastBlockAddedStoreReader {
    fn set(&mut self, last_block_added_hash: Hash) -> StoreResult<()>;
    fn remove(&mut self) -> StoreResult<()>;
}

/// A DB + cache implementation of `LastBlockAddedStore` trait, with concurrent readers support.
#[derive(Clone)]
pub struct DbLastBlockAddedStore {
    db: Arc<DB>,
    access: CachedDbItem<Hash, BlockHasher>,
}

pub const STORE_PREFIX: &[u8] = b"txindex-last-block-added";

impl DbLastBlockAddedStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbItem::new(db.clone(), STORE_PREFIX.to_vec()) }
    }

    pub fn clone_with_new_cache(&self) -> Self {
        Self::new(Arc::clone(&self.db))
    }
}

impl LastBlockAddedStoreReader for DbLastBlockAddedStore {
    fn get(&self) -> StoreResult<Hash> {
        self.access.read()
    }
}

impl LastBlockAddedStore for DbLastBlockAddedStore {
    fn set(&mut self, last_block_added: Hash) -> StoreResult<()> {
        self.access.write(DirectDbWriter::new(&self.db), &last_block_added)
    }

    fn remove(&mut self) -> StoreResult<()> {
        self.access.remove(DirectDbWriter::new(&self.db))
    }
}
