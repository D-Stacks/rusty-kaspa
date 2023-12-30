// External imports
use kaspa_core::trace;
use kaspa_database::prelude::{StoreError, DB};
use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use rocksdb::WriteBatch;

// Local imports
use crate::{
    errors::TxIndexResult,
    config::perf::TxIndexPerfParams,
    stores::{
        accepted_tx_offsets::DbTxIndexAcceptedTxOffsetsStore, merged_block_acceptance::DbTxIndexMergedBlockAcceptanceStore,
        sink::DbTxIndexSinkStore, source::DbTxIndexSourceStore, TxIndexAcceptedTxOffsetsStore, TxIndexMergedBlockAcceptanceStore,
        TxIndexSinkStore, TxIndexSourceStore,
    },
};

/// Stores for the transaction index.
pub struct TxIndexStores {
    pub accepted_tx_offsets_store: DbTxIndexAcceptedTxOffsetsStore,
    pub merged_block_acceptance_store: DbTxIndexMergedBlockAcceptanceStore,
    pub source_store: DbTxIndexSourceStore,
    pub sink_store: DbTxIndexSinkStore,
    db: Arc<DB>,
}

impl TxIndexStores {
    pub fn new(txindex_db: Arc<DB>, txindex_perf: &TxIndexPerfParams) -> Result<Self, StoreError> {
        Ok(Self {
            accepted_tx_offsets_store: DbTxIndexAcceptedTxOffsetsStore::new(txindex_db.clone(), txindex_perf.offset_cache_size),
            merged_block_acceptance_store: DbTxIndexMergedBlockAcceptanceStore::new(
                txindex_db.clone(),
                txindex_perf.block_acceptance_cache_size,
            ),
            source_store: DbTxIndexSourceStore::new(txindex_db.clone()),
            sink_store: DbTxIndexSinkStore::new(txindex_db.clone()),
            db: txindex_db.clone(),
        })
    }

    pub fn write_batch(&self, batch: WriteBatch) -> TxIndexResult<()> {
        Ok(self.db.write(batch)?)
    }

    /// Resets the txindex database:
    pub fn delete_all(&mut self) -> TxIndexResult<()> {
        // TODO: explore possibility of deleting and replacing whole db, currently there is an issue because of file lock and db being in an arc.
        trace!("[{0:?}] attempting to clear txindex database...", self);

        let mut batch: rocksdb::WriteBatchWithTransaction<false> = WriteBatch::default();

        self.source_store.remove_batch_via_batch_writer(&mut batch)?;
        self.sink_store.remove_batch_via_batch_writer(&mut batch)?;
        self.accepted_tx_offsets_store.delete_all_batched(&mut batch)?;
        self.merged_block_acceptance_store.delete_all_batched(&mut batch)?;

        self.db.write(batch)?;

        trace!("[{0:?}] cleared txindex database", self);

        Ok(())
    }
}

impl Debug for TxIndexStores {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxIndexStores").finish()
    }
}
