use crate::model::{TxOffset, TxOffsetById};

use kaspa_consensus_core::tx::TransactionId;
use kaspa_database::{
    prelude::{CachedDbAccess, DirectDbWriter, StoreResult, DB},
    registry::DatabaseStorePrefixes,
};
use kaspa_hashes::Hash;
use std::sync::Arc;

// Traits:

pub trait TxIndexUnacceptedTxOffsetsReader {
    /// Get [`TransactionOffset`] queried by [`TransactionId`],
    fn get(&self, transaction_id: TransactionId) -> StoreResult<TxOffset>;
    fn has(&self, transaction_id: TransactionId) -> StoreResult<bool>;
}

pub trait TxIndexUnacceptedTxOffsetsStore: TxIndexUnacceptedTxOffsetsReader {
    fn remove_many(&mut self, transaction_ids: Arc<Vec<TransactionId>>) -> StoreResult<()>;
    fn insert_many(&mut self, tx_offsets: Arc<TxOffsetById>) -> StoreResult<()>;

    fn delete_all(&mut self) -> StoreResult<()>;
}

// Implementations:

#[derive(Clone)]
pub struct DbTxIndexUnacceptedTxOffsetsStore {
    db: Arc<DB>,
    access: CachedDbAccess<TransactionId, TxOffset>,
}

impl DbTxIndexUnacceptedTxOffsetsStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self {
            db: Arc::clone(&db),
            access: CachedDbAccess::new(db, cache_size, DatabaseStorePrefixes::TxIndexUnacceptedOffsets.into()),
        }
    }
}

impl TxIndexUnacceptedTxOffsetsReader for DbTxIndexUnacceptedTxOffsetsStore {
    fn get(&self, transaction_id: TransactionId) -> StoreResult<TxOffset> {
        self.access.read(transaction_id)
    }

    fn has(&self, transaction_id: TransactionId) -> StoreResult<bool> {
        self.access.has(transaction_id)
    }
}

impl TxIndexUnacceptedTxOffsetsStore for DbTxIndexUnacceptedTxOffsetsStore {
    fn remove_many(&mut self, mut transaction_ids: Arc<Vec<TransactionId>>) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        self.access.delete_many(writer, &mut transaction_ids.into_iter()) // delete_many does "try delete" under the hood.
    }

    fn insert_many(&mut self, tx_offsets: TxOffsetById) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        let mut to_add = tx_offsets;

        self.access.write_many(writer, &mut to_add.iter().map(|(tx_id, tx_offset)| (*tx_id, *tx_offset)))
    }

    /// Removes all [`TxCompactEntry`] values and keys from the cache and db.
    fn delete_all(&mut self) -> StoreResult<()> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.delete_all(&mut writer)
    }
}
