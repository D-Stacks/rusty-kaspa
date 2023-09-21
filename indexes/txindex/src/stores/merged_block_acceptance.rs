use crate::model::{TxAcceptanceData, TxAcceptanceDataByBlockHash};
use crate::model::transaction_entries::{TransactionEntry, TransactionEntriesById, TransactionOffset, TransactionAcceptanceData};

use kaspa_consensus_core::BlockHasher;
use kaspa_consensus_core::tx::TransactionId;
use kaspa_database::{prelude::{CachedDbAccess, DirectDbWriter, StoreResult, DB}, registry::DatabaseStorePrefixes};
use kaspa_hashes::Hash;
use std::sync::Arc;

// Traits:

pub trait TxIndexMergedBlockAcceptanceReader {
    /// Get [`TransactionOffset`] queried by [`TransactionId`],
    fn get(&self, block_hash: Hash) -> StoreResult<TxAcceptanceData>;
    fn has(&self, block_hash: Hash) -> StoreResult<bool>;
}

pub trait TxIndexMergedBlockAcceptanceStore: TxIndexMergedBlockAcceptanceReader {
    fn remove_many(&mut self, block_hashes: Vec<Hash>) -> StoreResult<()>;
    fn insert_many(&mut self, merged_block_acceptance: TxAcceptanceDataByBlockHash) -> StoreResult<()>;
    fn delete_all(&mut self) -> StoreResult<()>;
}

pub const STORE_PREFIX: &[u8] = "txindex_block_acceptance";
// Implementations:

#[derive(Clone)]
pub struct DbTxIndexMergedBlockAcceptanceStore {
    db: Arc<DB>,
    access: CachedDbAccess<Hash, TxAcceptanceData, BlockHasher>,
}

impl DbTxIndexMergedBlockAcceptanceStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_size, DatabaseStorePrefixes::TxIndexMergedBlockAcceptance) }
    }
}

impl TxIndexMergedBlockAcceptanceReader for DbTxIndexMergedBlockAcceptanceStore {   
    fn get(&self, block_hash: Hash) -> StoreResult<TxAcceptanceData> {
        self.access.read(&block_hash)
    }

    fn has(&self, block_hash: Hash) -> StoreResult<bool> {
        self.access.has(&block_hash)
    }
    
}

impl TxIndexMergedBlockAcceptanceStore for DbTxIndexMergedBlockAcceptanceStore {
    fn remove_many(&mut self, block_hashes: Vec<Hash>) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        self.access.delete_many(writer, &mut block_hashes.iter())
    }

    fn insert_many(&mut self, merged_block_acceptance: &TxAcceptanceDataByBlockHash) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        self.access.write_many(writer, &mut merged_block_acceptance.iter())

    }

    /// Removes all Offset in the cache and db, besides prefixes themselves.
    fn delete_all(&mut self) -> StoreResult<()> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.delete_all(&mut writer)
    }
}
