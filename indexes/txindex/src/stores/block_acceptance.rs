use crate::model::{TxAcceptanceData, TxAcceptanceDataByBlockHash};
use crate::model::transaction_entries::{TransactionEntry, TransactionEntriesById, TransactionOffset, TransactionAcceptanceData};

use kaspa_consensus_core::BlockHasher;
use kaspa_consensus_core::tx::TransactionId;
use kaspa_database::prelude::{CachedDbAccess, DirectDbWriter, StoreResult, DB};
use kaspa_database::registry::DatabaseStorePrefixes;
use kaspa_hashes::Hash;
use std::sync::Arc;

// Prefixes:

/// Prefixes the [`TransactionId`] indexed [`TransactionOffset`] store.
pub const STORE_PREFIX: &[u8] = b"txindex-block-acceptance";

// Traits:

pub trait TxIndexBlockAcceptanceStoreReader {
    /// Get [`TransactionOffset`] queried by [`TransactionId`],
    fn get(&self, block_hash: Hash) -> StoreResult<TxAcceptanceData>;
    fn has(&self, block_hash: Hash) -> StoreResult<bool>;
}

pub trait TxIndexBlockAcceptanceEntriesStore: TxIndexTransactionEntriesStoreReader {
    fn insert_many(&mut self, transaction_acceptance_data_by_block_hash: TxAcceptanceDataByBlockHash) -> StoreResult<()>;
    fn remove_many(&mut self, mut block_hashes: Vec<Hash>) -> StoreResult<()>;
    fn delete_all(&mut self) -> StoreResult<()>;
}

pub const STORE_PREFIX: &[u8] = "txindex_block_acceptance";
// Implementations:

#[derive(Clone)]
pub struct DbTxIndexBlockAcceptanceEntriesStore {
    db: Arc<DB>,
    access: CachedDbAccess<Hash, TxAcceptanceData, BlockHasher>,
}

impl DbTxIndexTBlockAcceptanceEntriesStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_size, DatabaseStorePrefixes::TxIndexBlockAcceptance) }
    }
}

impl TxIndexBlockAcceptanceStoreReader for DbTxIndexBlockAcceptanceStore {   
    fn get(&self, block_hash: Hash) -> StoreResult<TxAcceptanceData> {
        self.access.read(transaction_id)
    }

    fn has(&self, block_hash: Hash) -> StoreResult<bool> {
        self.access.has(transaction_id)
    }
    
}

impl TxIndexBlockAcceptanceStore for DbTxIndexBlockAcceptanceStore {
    fn remove_many(&mut self, mut block_hashes: Vec<Hash>) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        self.access.delete_many(writer, &mut block_hashes.iter()) // delete_many does "try delete" under the hood. 
    }

    fn insert_many(&mut self, transaction_acceptance_data_by_block_hash: TxAcceptanceDataByBlockHash) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        self.access.write_many(writer, &mut transaction_acceptance_data_by_block_hash.iter())

    }

    /// Removes all Offset in the cache and db, besides prefixes themselves.
    fn delete_all(&mut self) -> StoreResult<()> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.delete_all(&mut writer)
    }
}
