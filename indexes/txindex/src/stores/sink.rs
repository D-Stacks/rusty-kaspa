use std::sync::Arc;

use kaspa_database::prelude::StoreResult;
use kaspa_database::prelude::DB;
use kaspa_database::prelude::{CachedDbItem, DirectDbWriter};
use kaspa_hashes::Hash;

//Use this store to check sync and resync from last_block_added. 
pub const STORE_PREFIX: &[u8] = b"-sink";


/// Reader API for `SinkStore`.
pub trait SinkStoreReader {
    fn get(&self) -> StoreResult<Option<Hash>>;
}

pub trait SinkStore: SinkStoreReader {
    fn set(&mut self, sink: Hash) -> StoreResult<()>;
    fn remove(&mut self) -> StoreResult<()>;
}

/// A DB + cache implementation of `SinkStore` trait, with concurrent readers support.
#[derive(Clone)]
pub struct DbSinkStore {
    db: Arc<DB>,
    access: CachedDbItem<Hash, BlockHasher>,
}

const STORE_PREFIX: &[u8] = b"txindex-sink";

impl DbSinkStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbItem::new(db.clone(), STORE_PREFIX.to_vec()) }
    }

    pub fn clone_with_new_cache(&self) -> Self {
        Self::new(Arc::clone(&self.db))
    }
}

impl SinkStoreReader for DbSinkStore {
    fn get(&self) -> StoreResult<Hash> {
        self.access.read()
    }
}

impl SinkStore for DbSinkStore {
    fn set(&mut self, Sink: Hash) -> StoreResult<()> {
        self.access.write(DirectDbWriter::new(&self.db), &Sink_point)
    }

    fn remove(&mut self) -> StoreResult<()> {
        self.access.remove(DirectDbWriter::new(&self.db))
    }
}
