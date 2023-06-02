use std::sync::Arc;

use kaspa_database::prelude::StoreResult;
use kaspa_database::prelude::DB;
use kaspa_database::prelude::{CachedDbItem, DirectDbWriter};
use kaspa_hashes::Hash;

use crate::model::params::TxIndexParams;

// This Store is required for is_synced, in order to register changes in params between runs, so resyncing can occur with new parameters. 

/// Reader API for `TxIndexParams`.
pub trait TxIndexParamsStoreReader {
    fn get(&self) -> StoreResult<TxIndexParams>;
}

pub trait TxIndexParamsStore: TxIndexParamsStoreReader {
    fn set(&mut self, params: TxIndexParams) -> StoreResult<()>;
    fn remove(&mut self) -> StoreResult<()>;
}

/// A DB + cache implementation of `PruningStore` trait, with concurrent readers support.
#[derive(Clone)]
pub struct DbTxIndexParamsStore {
    db: Arc<DB>,
    access: CachedDbItem<TxIndexParams>,
}

const STORE_PREFIX: &[u8] = b"txindex-params";

impl DbTxIndexParamsStore {
    pub fn new(db: Arc<DB>) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbItem::new(db.clone(), STORE_PREFIX.to_vec()) }
    }

    pub fn clone_with_new_cache(&self) -> Self {
        Self::new(Arc::clone(&self.db))
    }
}

impl TxIndexParamsStoreReader for DbTxIndexParamsStore {
    fn get(&self) -> StoreResult<TxIndexParams> {
        self.access.read()
    }
}

impl TxIndexParamsStore for DbTxIndexParamsStore {
    fn set(&mut self, params: TxIndexParams) -> StoreResult<()> {
        self.access.write(DirectDbWriter::new(&self.db), &params)
    }

    fn remove(&mut self) -> StoreResult<()> {
        self.access.remove(DirectDbWriter::new(&self.db))
    }
}
