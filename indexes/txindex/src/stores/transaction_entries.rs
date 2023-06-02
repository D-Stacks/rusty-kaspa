use crate::model::{transaction_entries::{TransactionEntry, TransactionIdsByBlockHash, TransactionEntriesById}, params::TxIndexParams};

use kaspa_consensus_core::{tx::{TransactionId, Transaction, TransactionIndexType, TransactionEntry}, acceptance_data::{BlockAcceptanceData, MergesetBlockAcceptanceData}};
use kaspa_database::prelude::{CachedDbAccess, DirectDbWriter, StoreResult, DB, CachedDbItem};
use kaspa_hashes::Hash;
use std::sync::Arc;

// Prefixes:

/// Prefixes the [`TransactionId`] indexed [`TransactionOffset`] store.
pub const STORE_PREFIX: &[u8] = b"txindex-accepted-offsets";

// Traits:

pub trait TransactionEntriesStoreReader {
    /// Get [`TransactionOffset`] queried by [`TransactionId`],
    fn get(&self, transaction_id: TransactionId) -> StoreResult<TransactionEntry>;
    fn has(&self, transaction_id: TransactionId) -> StoreResult<bool>;
}

pub trait TransactionEntriesStore: TransactionEntriesStoreReader {
    fn remove_many(&mut self, transaction_ids: Vec<TransactionId>) -> StoreResult<()>;
    fn insert_many(&mut self, transaction_entries_by_id: TransactionEntriesById, overwrite: bool) -> StoreResult<()>;

    fn delete_all(&mut self) -> StoreResult<()>;
}

// Implementations:

#[derive(Clone)]
pub struct DbTransactionEntriesStore {
    db: Arc<DB>,
    access: CachedDbAccess<TransactionId, TransactionOffset>,
}

impl DbTransactionEntriesStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_size, STORE_PREFIX) }
    }
}

impl TransactionEntriesStoreReader for DbTransactionEntriesStore {   
    fn get(&self, transaction_id: TransactionId) -> StoreResult<TransactionOffset> {
        self.access.read(transaction_id)
    }

    fn has(&self, transaction_id: TransactionId) -> StoreResult<bool> {
        self.access.has(transaction_id)
    }
}

impl TransactionEntriesStore for DbTransactionEntriesStore {
    fn remove_many(&mut self, mut transaction_ids: Vec<TransactionId>) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        self.access.delete_many(writer, &mut transaction_ids.into_iter()) // delete_many does "try delete" under the hood. 
    }

    fn insert_many(&mut self, transaction_entries_by_id: TransactionEntriesById, overwrite: bool) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        let mut to_add = transaction_entries_by_id.iter();
        
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

    /// Removes all Offset in the cache and db, besides prefixes themselves.
    fn delete_all(&mut self) -> StoreResult<()> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.delete_all(&mut writer)
    }
}
