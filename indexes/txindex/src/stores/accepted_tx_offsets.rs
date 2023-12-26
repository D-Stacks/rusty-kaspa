use crate::model::{TxOffset, TxOffsetChanges};

use kaspa_consensus_core::tx::TransactionId;
use kaspa_database::{
    prelude::{CachedDbAccess, StoreResult, DB, BatchDbWriter, StoreError},
    registry::DatabaseStorePrefixes,
};
use rocksdb::WriteBatch;
use std::sync::Arc;

// Traits:
pub trait TxIndexAcceptedTxOffsetsReader {
    /// Get [`TransactionOffset`] queried by [`TransactionId`],
    fn get(&self, transaction_id: TransactionId) -> StoreResult<Option<TxOffset>>;
    fn has(&self, transaction_id: TransactionId) -> StoreResult<bool>;
}

pub trait TxIndexAcceptedTxOffsetsStore: TxIndexAcceptedTxOffsetsReader {
    fn write_diff_batch(&mut self, batch: &mut WriteBatch, tx_offset_changes: TxOffsetChanges) -> StoreResult<()>;
    fn delete_all_batched(&mut self, batch: &mut WriteBatch) -> StoreResult<()>;
    
}
// Implementations:

#[derive(Clone)]
pub struct DbTxIndexAcceptedTxOffsetsStore {
    db: Arc<DB>,
    access: CachedDbAccess<TransactionId, TxOffset>,
}

impl DbTxIndexAcceptedTxOffsetsStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_size, DatabaseStorePrefixes::TxIndexAcceptedOffsets.into()) }
    }
}

impl TxIndexAcceptedTxOffsetsReader for DbTxIndexAcceptedTxOffsetsStore {
    fn get(&self, transaction_id: TransactionId) -> StoreResult<Option<TxOffset>> {
        self.access.read(transaction_id)
        .map(Some)
        .or_else(|e| if let StoreError::KeyNotFound(_) = e { Ok(None) } else { Err(e) })
    }
    
    fn has(&self, transaction_id: TransactionId) -> StoreResult<bool> {
        self.access.has(transaction_id)
    }
}

impl TxIndexAcceptedTxOffsetsStore for DbTxIndexAcceptedTxOffsetsStore {
    
    fn write_diff_batch(&mut self, batch: &mut WriteBatch, tx_offset_changes: TxOffsetChanges) -> StoreResult<()> {
        let mut writer = BatchDbWriter::new(batch);
        self.access.delete_many(&mut writer, &mut tx_offset_changes.to_remove.iter().cloned())?;
        self.access.write_many(&mut writer, &mut tx_offset_changes.to_add.iter().map(|(a, b)| (*a, *b)))?;
        Ok(())
    }
    /// Removes all [`TxOffsetById`] values and keys from the cache and db.
    fn delete_all_batched(&mut self, batch: &mut WriteBatch) -> StoreResult<()> {
        let mut writer = BatchDbWriter::new(batch);
        self.access.delete_all(&mut writer)
    }
    
}