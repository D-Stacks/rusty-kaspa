use kaspa_consensus_core::BlockHasher;
use kaspa_database::{
    prelude::{BatchDbWriter, CachedDbAccess, StoreError, StoreResult, DB},
    registry::DatabaseStorePrefixes,
};
use kaspa_hashes::Hash;
use rocksdb::WriteBatch;
use std::sync::Arc;

use crate::model::{BlockAcceptanceOffset, BlockAcceptanceOffsetsChanges};

// Traits:

pub trait TxIndexMergedBlockAcceptanceReader {
    /// Get [`TransactionOffset`] queried by [`TransactionId`],
    fn get(&self, block_hash: Hash) -> StoreResult<Option<BlockAcceptanceOffset>>;
    fn has(&self, block_hash: Hash) -> StoreResult<bool>;
}

pub trait TxIndexMergedBlockAcceptanceStore {
    fn write_diff_batch(
        &mut self,
        batch: &mut WriteBatch,
        block_acceptance_offset_changes: BlockAcceptanceOffsetsChanges,
    ) -> StoreResult<()>;
    fn delete_all_batched(&mut self, batch: &mut WriteBatch) -> StoreResult<()>;
}

// Implementations:

#[derive(Clone)]
pub struct DbTxIndexMergedBlockAcceptanceStore {
    db: Arc<DB>,
    access: CachedDbAccess<Hash, BlockAcceptanceOffset, BlockHasher>,
}

impl DbTxIndexMergedBlockAcceptanceStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self {
            db: Arc::clone(&db),
            access: CachedDbAccess::new(db, cache_size, DatabaseStorePrefixes::TxIndexMergedBlockAcceptance.into()),
        }
    }
}

impl TxIndexMergedBlockAcceptanceReader for DbTxIndexMergedBlockAcceptanceStore {
    fn get(&self, block_hash: Hash) -> StoreResult<Option<BlockAcceptanceOffset>> {
        self.access.read(block_hash).map(Some).or_else(|e| if let StoreError::KeyNotFound(_) = e { Ok(None) } else { Err(e) })
    }

    fn has(&self, block_hash: Hash) -> StoreResult<bool> {
        self.access.has(block_hash)
    }
}

impl TxIndexMergedBlockAcceptanceStore for DbTxIndexMergedBlockAcceptanceStore {
    fn write_diff_batch(
        &mut self,
        batch: &mut WriteBatch,
        block_acceptance_offset_changes: BlockAcceptanceOffsetsChanges,
    ) -> StoreResult<()> {
        let mut writer = BatchDbWriter::new(batch);
        self.access.delete_many(&mut writer, &mut block_acceptance_offset_changes.to_remove.iter().map(|v| *v))?;
        self.access.write_many(&mut writer, &mut block_acceptance_offset_changes.to_add.iter().map(|(k, v)| (*k, *v)))?;
        Ok(())
    }

    /// Removes all [`TxOffsetById`] values and keys from the cache and db.
    fn delete_all_batched(&mut self, batch: &mut WriteBatch) -> StoreResult<()> {
        let mut writer = BatchDbWriter::new(batch);
        self.access.delete_all(&mut writer)
    }
}
