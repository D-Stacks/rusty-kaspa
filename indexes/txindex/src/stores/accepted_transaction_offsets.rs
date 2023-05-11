use crate::model::{transaction_offsets::TransactionOffset, params::TxIndexParams};

use kaspa_consensus_core::{tx::{TransactionId, Transaction, TransactionIndexType, TransactionEntry}, acceptance_data::{BlockAcceptanceData, MergesetBlockAcceptanceData}};
use kaspa_database::prelude::{CachedDbAccess, DirectDbWriter, StoreResult, DB, CachedDbItem};
use kaspa_hashes::Hash;
use std::sync::Arc;

// Prefixes:

/// Prefixes the [`TransactionId`] indexed [`TransactionOffset`] store.
pub const STORE_PREFIX: &[u8] = b"txindex-accepted-offsets";

// Traits:

pub trait AcceptedTxOffsetStoreReader {
    /// Get [`TransactionOffset`] queried by [`TransactionId`],
    fn get(&self, transaction_id: TransactionId) -> StoreResult<TransactionOffset>;
    fn has(&self, transaction_id: TransactionId) -> StoreResult<bool>;
}

pub trait AcceptedTxOffsetStore: AcceptedTxOffsetStoreReader {
    fn remove_many(&mut self, transaction_ids: Vec<TransactionId>) -> StoreResult<()>;
    fn insert_many_from_block_accepted_mergeset(&mut self, including_block: Hash, transaction_offsets: Vec<TransactionOffset>) -> StoreResult<()>;
     
    fn delete_all(&mut self) -> StoreResult<()>;
}

// Implementations:

#[derive(Clone)]
pub struct DbAcceptedTxOffsetStore {
    db: Arc<DB>,
    access: CachedDbAccess<TransactionId, TransactionOffset>,
}

impl DbAcceptedTxOffsetStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_size, STORE_PREFIX) }
    }
}

impl AcceptedTxOffsetStoreReader for DbAcceptedTxOffsetStore {   
    fn get(&self, transaction_id: TransactionId) -> StoreResult<TransactionOffset> {
        self.access.read(transaction_id)
    }

    fn has(&self, transaction_id: TransactionId) -> StoreResult<bool> {
        self.access.has(transaction_id)
    }
}

impl AcceptedTxOffsetStore for DbAcceptedTxOffsetStore {
    fn remove_many(&mut self, mut transaction_ids: Vec<TransactionId>) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        self.access.delete_many(writer, &mut transaction_ids.into_iter()) // delete_many does "try delete" under the hood. 
    }

    fn insert_many_from_block_accepted_mergeset(&mut self, including_block: Hash, transaction_entries: Vec<TransactionEntry>) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        let mut to_add = transaction_entries
        .into_iter()
        .map(move |entry| {
            (
                entry.transaction_id,
                TransactionOffset {
                    including_block,
                    transaction_index: entry.transaction_index
                }
            )
        });
        

        self.access.write_many(writer, &mut to_add)

    }

    /// Removes all Offset in the cache and db, besides prefixes themselves.
    fn delete_all(&mut self) -> StoreResult<()> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.delete_all(&mut writer)
    }
}
