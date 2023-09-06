use crate::model::{TxOffsetById, TxCompactEntriesById};

use kaspa_consensus_core::tx::TransactionId;
use kaspa_database::{prelude::{CachedDbAccess, DirectDbWriter, StoreResult, DB}, registry::DatabaseStorePrefixes};
use kaspa_hashes::Hash;
use std::sync::Arc;

// Prefixes:

/// Prefixes the [`TransactionId`] indexed [`TransactionOffset`] store.
pub const STORE_PREFIX: &[u8] = b"txindex-accepted-offsets";

// Traits:

pub trait TxIndexAcceptedTxOffsetsStoreReader {
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
    access: CachedDbAccess<TransactionId, TxCompactEntry>,
}

impl DbTxIndexAcceptedTxOffsetsStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_size, DatabaseStorePrefixes::TxIndexTransactionEntries) }
    }
}

impl TxIndexAcceptedTxOffsetsStoreReader for DbTxIndexAcceptedTxOffsetsStore {   
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

    fn insert_many(&mut self, compact_transaction_entries_by_id: TxCompactEntriesById, overwrite: bool) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        let mut to_add = compact_transaction_entries_by_id.iter();
        
        if overwrite = false {
            to_add = to_add.filter_map(move |(transaction_id, _)|  {
                if self.has(transaction_id) {
                    None
                } else {
                    Some(transaction_id)
                }
            },
            )
        }

        self.access.write_many(writer, &mut transaction_entries_by_id.iter())

    }

    /// Removes all [`TxCompactEntry`] values and keys from the cache and db.
    fn delete_all(&mut self) -> StoreResult<()> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.delete_all(&mut writer)
    }
}
