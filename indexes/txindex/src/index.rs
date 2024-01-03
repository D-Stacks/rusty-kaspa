use std::{
    fmt::Debug,
    sync::{Arc, Weak},
};

use kaspa_consensus_core::tx::TransactionId;
use kaspa_consensusmanager::{ConsensusManager, ConsensusResetHandler, ConsensusSessionBlocking};
use kaspa_core::{error, info, trace};
use kaspa_database::prelude::DB;
use kaspa_hashes::Hash;
use kaspa_consensus_notify::notification::{VirtualChainChangedNotification as ConsensusVirtualChainChangedNotification, ChainAcceptanceDataPrunedNotification as ConsensusChainAcceptanceDataPrunedNotification};
use parking_lot::RwLock;
use rocksdb::WriteBatch;
use kaspa_index_core::{models::txindex::{TxOffset, BlockAcceptanceOffset}, reindexers::txindex::TxIndexReindexer};

use crate::{
    core::api::TxIndexApi,
    core::errors::TxIndexResult,
    core::errors::TxIndexError,
    stores::{
        TxIndexAcceptedTxOffsetsReader, TxIndexAcceptedTxOffsetsStore, TxIndexMergedBlockAcceptanceReader,
        TxIndexMergedBlockAcceptanceStore, TxIndexSinkReader, TxIndexSinkStore, TxIndexSourceReader, TxIndexStores, TxIndexSourceStore,
    }, config::Config,
};

pub struct TxIndex {
    stores: TxIndexStores,
    consensus_manager: Arc<ConsensusManager>,
    config: Arc<Config>, // move into config, once txindex is configurable.
}

impl TxIndex {
    pub fn new(
        consensus_manager: Arc<ConsensusManager>,
        db: Arc<DB>,
        config: Arc<Config>,
    ) -> TxIndexResult<Arc<RwLock<Self>>> {
        let mut txindex = Self {
            stores: TxIndexStores::new(db, &config)?,
            consensus_manager: consensus_manager.clone(),
            config,
        };

        if !txindex.is_synced()? {
            match txindex.resync() {
                Ok(_) => {
                    info!("[{0:?}] Resync Successful", txindex);
                }
                Err(e) => {
                    error!("[{0:?}] Failed to resync: {1}", txindex, e);
                    txindex.stores.delete_all()?; // we try and delete all, in order to remove any partial data that may have been written.
                    return Err(e);
                }
            };
        };

        let txindex = Arc::new(RwLock::new(txindex));
        consensus_manager.register_consensus_reset_handler(Arc::new(TxIndexConsensusResetHandler::new(Arc::downgrade(&txindex))));
        Ok(txindex)
    }

    // For internal usage only
    fn _resync_segement(
        &mut self,
        mut start_hash: Hash,
        end_hash: Hash,
        session: &ConsensusSessionBlocking<'_>,
        // Setting below to true will remove data added along the start_hash -> end_hash path.
        unsync_segment: bool,
    ) -> TxIndexResult<()> {
        info!("[{0:?}] Resyncing from {1} to {2}", self, start_hash, end_hash,);
        let split_hash = session.find_highest_common_chain_block(start_hash, end_hash)?;
        let split_daa_score = session.get_header(split_hash)?.daa_score;
        let total_blocks_to_remove = session.get_header(start_hash)?.daa_score - split_daa_score; // start_daa_score - split_daa_score;
        let total_blocks_to_add = session.get_header(end_hash)?.daa_score - split_daa_score; // end_daa_score - split_daa_score;

        let mut total_blocks_removed: u64 = 0u64;
        let mut total_blocks_added: u64 = 0u64;
        let mut removed_processed_in_batch = 0u64;
        let mut added_processed_in_batch = 0u64;
        while start_hash != end_hash {
            let mut chain_path =
                session.get_virtual_chain_from_block(start_hash, Some(end_hash), Some(self.config.perf.resync_chunksize as usize))?;

            // We switch added to removed, and clear removed, as we have no use for the removed data.
            if unsync_segment {
                chain_path.removed = chain_path.added;
                chain_path.added = Arc::new(vec![]);
            }

            let removed_chain_blocks_acceptance_data = if !chain_path.removed.is_empty() {
                session.get_blocks_acceptance_data(chain_path.added.clone())?
            } else {
                Arc::new(vec![])
            };
            removed_processed_in_batch += chain_path.added.len() as u64;

            let added_chain_blocks_acceptance_data = if !chain_path.added.is_empty() {
                session.get_blocks_acceptance_data(chain_path.removed.clone())?
            } else {
                Arc::new(vec![])
            };

            removed_processed_in_batch += chain_path.added.len() as u64;
            added_processed_in_batch += chain_path.removed.len() as u64;

            start_hash = *chain_path.checkpoint_hash().expect("chain path should not be empty");

            let vspcc_notification = ConsensusVirtualChainChangedNotification::new(
                chain_path.added,
                chain_path.removed,
                added_chain_blocks_acceptance_data,
                removed_chain_blocks_acceptance_data,
            );

            self.update_via_vspcc_added(vspcc_notification)?;

            if removed_processed_in_batch != 0 {
                total_blocks_removed += removed_processed_in_batch;
                // Log progress
                info!(
                    "[{0:?}] Removed {1}, {2} blocks out of {3}, {4:.2} completed",
                    self,
                    removed_processed_in_batch,
                    total_blocks_removed,
                    total_blocks_to_remove,
                    total_blocks_removed as f64 / total_blocks_to_remove as f64 * 100.0,
                );
                removed_processed_in_batch = 0;
            }

            if added_processed_in_batch != 0 {
                total_blocks_added += added_processed_in_batch;
                // Log progress
                info!(
                    "[{0:?}] Added {1}, {2} blocks out of {3}, {4:.2} completed",
                    self,
                    added_processed_in_batch,
                    total_blocks_removed,
                    total_blocks_to_add,
                    total_blocks_added as f64 / total_blocks_to_add as f64 * 100.0,
                );
                added_processed_in_batch = 0;
            }
        }

        Ok(())
    }
}

impl TxIndexApi for TxIndex {
    // Resync methods.
    fn resync(&mut self) -> TxIndexResult<()> {
        info!("[{0:?}] Started Resyncing", self);

        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session_blocking());
        // Gather the necessary block hashes
        let txindex_source = self.stores.source_store.get()?;
        let consensus_source = session.get_source();
        let txindex_sink = self.stores.sink_store.get()?;
        let consensus_sink = session.get_sink();

        let start_hash = txindex_source
        .filter(|txindex_source| *txindex_source == consensus_source)
        .unwrap_or({ 
            info!(
                "[{0:?}] txindex source is not synced with consensus source. txindex source: {1:?}, consensus source: {2:?} - Resetting the DB",
                self,
                txindex_source,
                consensus_source,
            );
            self.stores.delete_all()?; // we reset the txindex
            // We can set the source anew after clearing db
            self.stores.source_store.set(consensus_source)?;
            consensus_source 
        });

        let end_hash = txindex_sink.map_or_else(
            || Ok::<Hash, TxIndexError>(consensus_sink),
            |txindex_sink| {
                if txindex_sink != consensus_sink {
                    info!(
                        "[{0:?}] txindex sink is not synced with consensus sink. txindex sink: {1:?}, consensus sink: {2}",
                        self, txindex_sink, consensus_sink,
                    );
                    // If txindex sink is not synced with consensus sink, and not a chain block, we must first unsync from the highest common chain block.
                    if !session.is_chain_block(txindex_sink)? {
                        let hccb = session.find_highest_common_chain_block(txindex_sink, consensus_sink)?;
                        info!(
                            "[{0:?}] txindex sink is not a chain block, unsyncing from {1} to the highest common chain block: {2}",
                            self, txindex_sink, hccb,
                        );
                        self._resync_segement(txindex_sink, hccb, &session, true)?;
                    }
                }
                Ok(consensus_sink)
            },
        )?;

        self._resync_segement(start_hash, end_hash, &session, false)?;
        Ok(())
    }

    // Sync state methods
    fn is_synced(&self) -> TxIndexResult<bool> {
        trace!("[{0:?}] checking sync status...", self);

        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session_blocking());

        if let Some(txindex_sink) = self.stores.sink_store.get()? {
            if txindex_sink == session.get_sink() {
                if let Some(txindex_source) = self.stores.source_store.get()? {
                    if txindex_source == session.get_source() {
                        return Ok(true);
                    }
                }
            }
        };

        Ok(false)
    }

    fn get_merged_block_acceptance_offset(&self, hashes: Vec<Hash>) -> TxIndexResult<Arc<Vec<Option<BlockAcceptanceOffset>>>> {
        trace!("[{0:?}] Getting merged block acceptance offsets for {1} blocks", self, hashes.len());

        Ok(Arc::new(
            hashes
                .iter()
                .map(move |hash| self.stores.merged_block_acceptance_store.get(*hash))
                .collect::<Result<Vec<Option<BlockAcceptanceOffset>>, _>>()?,
        ))
    }

    fn get_tx_offsets(&self, tx_ids: Vec<TransactionId>) -> TxIndexResult<Arc<Vec<Option<TxOffset>>>> {
        trace!("[{0:?}] Getting tx offsets for {1} txs", self, tx_ids.len());

        Ok(Arc::new(
            tx_ids
                .iter()
                .map(move |tx_id| self.stores.accepted_tx_offsets_store.get(*tx_id))
                .collect::<Result<Vec<Option<TxOffset>>, _>>()?,
        ))
    }

    // Update methods
    fn update_via_vspcc_added(&mut self, vspcc_notification: ConsensusVirtualChainChangedNotification) -> TxIndexResult<()> {
        trace!(
            "[{0:?}] Updating db with {1} added chain blocks and {2} removed chain blocks",
            self,
            vspcc_notification.added_chain_block_hashes.len(),
            vspcc_notification.removed_chain_blocks_acceptance_data.len()
        );

        let txindex_reindexer = TxIndexReindexer::from(vspcc_notification);

        let mut batch: rocksdb::WriteBatchWithTransaction<false> = WriteBatch::default();

        self.stores.accepted_tx_offsets_store.write_diff_batch(&mut batch, txindex_reindexer.tx_offset_changes)?;
        self.stores
            .merged_block_acceptance_store
            .write_diff_batch(&mut batch, txindex_reindexer.block_acceptance_offsets_changes)?;
        self.stores.sink_store.set_via_batch_writer(&mut batch, txindex_reindexer.new_sink.expect("expected a new sink with each new VCC notification"))?;

        self.stores.write_batch(batch)
    }

    fn update_via_chain_acceptance_data_pruned(
        &mut self,
        chain_acceptance_data_pruned: ConsensusChainAcceptanceDataPrunedNotification,
    ) -> TxIndexResult<()> {
        let txindex_reindexer = TxIndexReindexer::from(chain_acceptance_data_pruned);

        let mut batch: rocksdb::WriteBatchWithTransaction<false> = WriteBatch::default();

        self.stores.accepted_tx_offsets_store.remove_many(&mut batch, txindex_reindexer.tx_offset_changes.removed)?;
        self.stores
            .merged_block_acceptance_store
            .remove_many(&mut batch, txindex_reindexer.block_acceptance_offsets_changes.removed)?;
        self.stores.source_store.replace_if_new(&mut batch, txindex_reindexer.source.expect("a source with each new CADP notification"))?;

        self.stores.write_batch(batch)
    }
}

impl Debug for TxIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxIndex").finish()
    }
}

struct TxIndexConsensusResetHandler {
    txindex: Weak<RwLock<TxIndex>>,
}

impl TxIndexConsensusResetHandler {
    fn new(txindex: Weak<RwLock<TxIndex>>) -> Self {
        Self { txindex }
    }
}

impl ConsensusResetHandler for TxIndexConsensusResetHandler {
    fn handle_consensus_reset(&self) {
        if let Some(txindex) = self.txindex.upgrade() {
            txindex.write().resync().unwrap();
        }
    }
}
