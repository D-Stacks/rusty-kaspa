use std::sync::Arc;

use kaspa_database::{
    prelude::{CachedDbItem, DirectDbWriter, StoreResult, DB},
    registry::DatabaseStorePrefixes,
};
use kaspa_hashes::Hash;

/// Reader API for `SinkStore`.
pub trait TxIndexSinkReader {
    fn get(&self) -> StoreResult<Hash>;
}

pub trait TxIndexSinkStore: TxIndexSinkReader {
    fn set(&mut self, sink: Hash) -> StoreResult<()>;
    fn remove(&mut self) -> StoreResult<()>;
}

/// A DB + cache implementation of `SinkStore` trait, with concurrent readers support.
#[derive(Clone)]
pub struct DbTxIndexSinkStore {
    db: Arc<DB>,
    access: CachedDbItem<Hash>,
}

impl DbTxIndexSinkStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbItem::new(db.clone(), DatabaseStorePrefixes::TxIndexSink.into()) }
    }

    pub fn clone_with_new_cache(&self) -> Self {
        Self::new(Arc::clone(&self.db))
    }
}

impl TxIndexSinkReader for DbTxIndexSinkStore {
    fn get(&self) -> StoreResult<Hash> {
        self.access.read()
    }
}

impl TxIndexSinkStore for DbTxIndexSinkStore {
    fn set(&mut self, sink: Hash) -> StoreResult<()> {
        self.access.write(DirectDbWriter::new(&self.db), &sink)
    }

    fn remove(&mut self) -> StoreResult<()> {
        self.access.remove(DirectDbWriter::new(&self.db))
    }
}
