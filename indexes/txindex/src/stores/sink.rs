use std::sync::Arc;

use kaspa_database::prelude::StoreResult;
use kaspa_database::prelude::DB;
use kaspa_database::prelude::{CachedDbItem, DirectDbWriter};
use kaspa_hashes::Hash;

//Use this store to check sync and resync from last_block_added. 
pub const STORE_PREFIX: &[u8] = b"txindex-sink";


/// Reader API for `SinkStore`.
pub trait TxIndexSinkStoreReader {
    fn get(&self) -> StoreResult<Hash>;
}

pub trait TxIndexSinkStore: TxIndexSinkStoreReader {
    fn set(&mut self, sink: Hash) -> StoreResult<()>;
}

/// A DB + cache implementation of `SinkStore` trait, with concurrent readers support.
#[derive(Clone)]
pub struct DbTxIndexSinkStore {
    db: Arc<DB>,
    access: CachedDbItem<Hash, BlockHasher>,
}

const STORE_PREFIX: &[u8] = b"txindex-sink";

impl DbTxIndexSinkStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbItem::new(db.clone(), STORE_PREFIX.to_vec()) }
    }

    pub fn clone_with_new_cache(&self) -> Self {
        Self::new(Arc::clone(&self.db))
    }
}

impl TxIndexSinkStoreReader for DbTxIndexSinkStore {
    fn get(&self) -> StoreResult<Hash> {
        self.access.read()
    }
}

impl TxIndexSinkStore for DbTxIndexSinkStore {
    fn set(&mut self, Sink: Hash) -> StoreResult<()> {
        self.access.write(DirectDbWriter::new(&self.db), &Sink_point)
    }
}
