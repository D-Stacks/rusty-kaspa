use std::sync::Arc;

use kaspa_database::{
    prelude::{BatchDbWriter, CachedDbItem, StoreError, StoreResult, DB},
    registry::DatabaseStorePrefixes,
};
use kaspa_hashes::Hash;
use rocksdb::WriteBatch;

// TODO (when pruning is implemented): Use this store to check sync and resync from earliest header pruning point.
// TODO: move to db registry
pub const STORE_PREFIX: &[u8] = b"txindex-source";

/// Reader API for `Source`.
pub trait TxIndexSourceReader {
    fn get(&self) -> StoreResult<Option<Hash>>;
}

pub trait TxIndexSourceStore: TxIndexSourceReader {
    fn set_via_batch_writer(&mut self, batch: &mut WriteBatch, sink: Hash) -> StoreResult<()>;
    fn remove_batch_via_batch_writer(&mut self, batch: &mut WriteBatch) -> StoreResult<()>;
}

/// A DB + cache implementation of `TxIndexSource` trait, with concurrent readers support.
#[derive(Clone)]
pub struct DbTxIndexSourceStore {
    db: Arc<DB>,
    access: CachedDbItem<Hash>,
}

impl DbTxIndexSourceStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbItem::new(db.clone(), DatabaseStorePrefixes::TxIndexSource.into()) }
    }

    pub fn clone_with_new_cache(&self) -> Self {
        Self::new(Arc::clone(&self.db))
    }
}

impl TxIndexSourceReader for DbTxIndexSourceStore {
    fn get(&self) -> StoreResult<Option<Hash>> {
        self.access.read().map(Some).or_else(|e| if let StoreError::KeyNotFound(_) = e { Ok(None) } else { Err(e) })
    }
}

impl TxIndexSourceStore for DbTxIndexSourceStore {
    fn remove_batch_via_batch_writer(&mut self, batch: &mut WriteBatch) -> StoreResult<()> {
        let mut writer = BatchDbWriter::new(batch);
        self.access.remove(&mut writer)
    }
    fn set_via_batch_writer(&mut self, batch: &mut WriteBatch, sink: Hash) -> StoreResult<()> {
        let mut writer = BatchDbWriter::new(batch);
        self.access.write(&mut writer, &sink)
    }
}
