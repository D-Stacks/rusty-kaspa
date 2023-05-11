use std::sync::Arc;

use kaspa_database::prelude::StoreResult;
use kaspa_database::prelude::DB;
use kaspa_database::prelude::{CachedDbItem, DirectDbWriter};
use kaspa_hashes::Hash;

// TODO (when pruning is implemented): Use this store to check sync and resync from earliest header pruning point. 
pub const STORE_PREFIX: &[u8] = b"txindex-pruning-point";


/// Reader API for `PruningStore`.
pub trait TxIndexPruningStoreReader {
    fn get(&self) -> StoreResult<Hash>;
}

pub trait TxIndexPruningStore: TxIndexPruningStoreReader {
    fn set(&mut self, pruning_point: Hash) -> StoreResult<()>;
}

/// A DB + cache implementation of `PruningStore` trait, with concurrent readers support.
#[derive(Clone)]
pub struct DbTxIndexPruningStore {
    db: Arc<DB>,
    access: CachedDbItem<Hash, BlockHasher>,
}

const STORE_PREFIX: &[u8] = b"txindex-pruning-point";

impl DbTxIndexPruningStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbItem::new(db.clone(), STORE_PREFIX.to_vec()) }
    }

    pub fn clone_with_new_cache(&self) -> Self {
        Self::new(Arc::clone(&self.db))
    }
}

impl TxIndexPruningStoreReader for DbTxIndexPruningStore {
    fn get(&self) -> StoreResult<Hash> {
        self.access.read()
    }
}

impl TxIndexPruningStore for DbTxIndexPruningStore {
    fn set(&mut self, pruning_point: Hash) -> StoreResult<()> {
        self.access.write(DirectDbWriter::new(&self.db), &pruning_point)
    }
}
