use crate::model::{TxOffsetById, TxCompactEntriesById, TxOffset};

use kaspa_consensus_core::tx::TransactionId;
use kaspa_database::{prelude::{CachedDbAccess, DirectDbWriter, StoreResult, DB}, registry::DatabaseStorePrefixes};
use kaspa_hashes::Hash;
use std::sync::Arc;

// Prefixes:

/// Prefixes the [`TransactionId`] indexed [`TransactionOffset`] store.
pub const STORE_PREFIX: &[u8] = b"txindex-accepted-offsets";

// Traits:

pub trait TxIndexAcceptedTxOffsetsReader {
    /// Get [`TransactionOffset`] queried by [`TransactionId`],
    fn get(&self, transaction_id: TransactionId) -> StoreResult<TransactionOffset>;
    fn has(&self, transaction_id: TransactionId) -> StoreResult<bool>;
}

pub trait TxIndexAcceptedTxOffsetsStore: TxIndexAcceptedTxOffsetsStoreReader {
    fn remove_many(&mut self, transaction_ids: Vec<TransactionId>) -> StoreResult<()>;
    fn insert_many(&mut self, transaction_offsets_by_id: TxOffsetById) -> StoreResult<()>;

    fn delete_all(&mut self) -> StoreResult<()>;
}

// Implementations:

#[derive(Clone)]
pub struct DbTxIndexAcceptedTxOffsetsStore {
    db: Arc<DB>,
    access: CachedDbAccess<TransactionId, TxOffset>,
}

impl DbTxIndexAcceptedTxOffsetsStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_size, DatabaseStorePrefixes::TxIndexAcceptedOffsets) }
    }
}

impl TxIndexAcceptedTxOffsetsReader for DbTxIndexAcceptedTxOffsetsStore {   
    fn get(&self, transaction_id: TransactionId) -> StoreResult<TransactionEntry> {
        self.access.read(transaction_id)
    }

    fn has(&self, transaction_id: TransactionId) -> StoreResult<bool> {
        self.access.has(transaction_id)
    }
    
}

impl TxIndexAcceptedTxOffsetsStore for DbTxIndexAcceptedTxOffsetsStore {
    fn remove_many(&mut self, mut transaction_ids: Vec<TransactionId>) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        self.access.delete_many(writer, &mut transaction_ids.into_iter()) // delete_many does "try delete" under the hood. 
    }

    fn insert_many(&mut self, transaction_offsets_by_id: TxOffsetById) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        self.access.write_many(writer, &mut transaction_entries_by_id.iter())

    }

    /// Removes all [`TxOffsetById`] values and keys from the cache and db.
    fn delete_all(&mut self) -> StoreResult<()> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.delete_all(&mut writer)
    }
}
