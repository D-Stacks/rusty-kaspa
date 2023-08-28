use std::sync::Arc;

use kaspa_database::prelude::StoreResult;
use kaspa_database::prelude::DB;
use kaspa_database::prelude::{CachedDbItem, DirectDbWriter};
use kaspa_hashes::Hash;

// TODO (when pruning is implemented): Use this store to check sync and resync from earliest header pruning point. 
pub const STORE_PREFIX: &[u8] = b"txindex-pruning-point";


/// Reader API for `Source`.
pub trait TxIndexSourceReader {
    fn get(&self) -> StoreResult<Hash>;
}

pub trait TxIndexSource: TxIndexSourceReader {
    fn set(&mut self, source: Hash) -> StoreResult<()>;
    fn remove(&mut self) -> StoreResult<()>;
}

/// A DB + cache implementation of `Source` trait, with concurrent readers support.
#[derive(Clone)]
pub struct DbTxIndexSource {
    db: Arc<DB>,
    access: CachedDbItem<Hash, BlockHasher>,
}

const STORE_PREFIX: &[u8] = b"txindex-source";

impl DbTxIndexSource {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbItem::new(db.clone(), STORE_PREFIX.to_vec()) }
    }

    pub fn clone_with_new_cache(&self) -> Self {
        Self::new(Arc::clone(&self.db))
    }
}

impl TxIndexSourceReader for DbTxIndexSource {
    fn get(&self) -> StoreResult<Hash> {
        self.access.read()
    }
}

impl TxIndexSource for DbTxIndexSource {
    fn set(&mut self, source: Hash) -> StoreResult<()> {
        self.access.write(DirectDbWriter::new(&self.db), &source)
    }

    fn remove(&mut self) -> StoreResult<()> {
        self.access.remove(DirectDbWriter::new(&self.db))
    }
}
