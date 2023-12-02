use kaspa_consensus_core::{
    acceptance_data::{AcceptedTxEntry, MergesetBlockAcceptanceData},
    block::Block,
    config::Config,
    header::Header,
    tx::{Transaction, TransactionId, TransactionIndexType, TransactionIndexType, COINBASE_TRANSACTION_INDEX},
    BlockHashMap, BlockHashSet,
};
use kaspa_consensusmanager::{ConsensusManager, ConsensusResetHandler, ConsensusSessionBlocking};
use kaspa_core::{info, trace};
use kaspa_database::prelude::DB;
use kaspa_hashes::Hash;
use kaspa_utils::arc::ArcExtensions;
use parking_lot::RwLock;
use std::{
    cmp::{max, min},
    collections::{
        hash_map::{Entry, OccupiedEntry, VacantEntry},
        HashMap, HashSet,
    },
    fmt::Debug,
    hash::Hash,
    iter::Zip,
    ops::{Sub, ControlFlow},
    sync::{Arc, Weak},
};

use crate::{
    api::TxIndexApi,
    errors::{TxIndexError, TxIndexResult},
    model::{
        transaction_entries::{TransactionEntriesById, TransactionEntry, TransactionOffset, TransactionOffsets},
        TxOffsetById,
    },
    params::TxIndexParams,
    reindexers::{
        transactions_included_reindexer, TxIndexAcceptanceDataReindexer, TxIndexBlockAddedReindexer, TxIndexBlockAddedReindexer,
        TxIndexInclusionDataReindexer,
    },
    stores::store_manager::TxIndexStore,
    IDENT,
};

const MIN_RESYNC_CHUNK_SIZE: usize = 256;

pub struct TxIndex {
    config: Arc<Config>,
    consensus_manager: Arc<ConsensusManager>,
    store: TxIndexStore,
}

impl TxIndex {
    /// Creates a new [`TxIndex`] within a [`RwLock`]
    pub fn new(
        config: Arc<Config>,
        consensus_manager: Arc<ConsensusManager>,
        txindex_db: Arc<DB>,
        consensus_db: Arc<DB>,
    ) -> TxIndexResult<Arc<RwLock<Self>>> {
        let mut txindex =
            Self { config, consensus_manager: consensus_manager.clone(), store: TxIndexStore::new(txindex_db, consensus_db) };

        let txindex = Arc::new(RwLock::new(txindex));
        consensus_manager.register_consensus_reset_handler(Arc::new(TxIndexConsensusResetHandler::new(Arc::downgrade(&txindex))));

        Ok(txindex)
    }
}

impl TxIndexApi for TxIndex {
    /// Resync the txindexes included transactions from the dag tips down to the vsp chain.
    ///
    /// Note: it is expected to call this after `sync_vspc_segment`, as the method most check if transactions are within the accepted tx store before commiting them as unaccepted.  
    fn sync_tips(&mut self) -> TxIndexResult<()> {
        info!("[{0}] Syncing unaccepted transactions from the virtual selected parent's antipast...", IDENT);

        // Sanity check: Assert we are syncing tips, only after all else is sync'd.
        assert!(self.is_sink_synced() && self.is_source_synced());

        // Set-up
        let consensus = self.consensus_manager.consensus();
        let consensus_session = futures::executor::block_on(consensus.session_blocking());
        let mut inclusion_reindexer = TxIndexInclusionDataReindexer::new();
        
        let tips = consensus_session.get_tips();
        let unaccepted_hashes = consensus_session.get_none_vspc_merged_blocks()?;

        // Loop unaccepted hashes, add them to the unaccepted db.
        for unaccepted_hashes_chunk in unaccepted_hashes.chunks(self.resync_chunk_size()).into_iter() {
            for unaccepted_hash in unaccepted_hashes_chunk.into_iter() {
                inclusion_reindexer.remove_transactions(
                    consensus_session
                        .get_block_transactions(hash)?
                        .into_iter()
                        .filter(move |tx| match self.store.has_accepted_transaction_offset(tx.id()) {
                            Ok(has) => has,
                            Err(err) => return ControlFlow::Break(err),
                        },
                ));
            }

            // Update counters and log info

            unaccepted_blocks_synced += self.resync_chunk_size();
            unaccepted_transactions_synced += inclusion_reindexer.included_tx_offsets().len();
            self.store.add_unaccepted_transaction_offsets(inclusion_reindexer.included_tx_offsets());
            info!(
                "[{0}] Synced {1} / {2} none-vspc-merged blocks ({3:.2} %), totaling {4} unaccepted txs",
                IDENT,
                unaccepted_blocks_synced,
                unaccepted_blocks.len(),
                (unaccepted_blocks_synced as f64 / unaccepted_hashes.len() as f64),
                unaccepted_transactions_synced
            );

            // Clear the reindexer
            block_added_reindexer.clear();
        }

        // Finished

        info!("[{0}] Finished Syncing unaccepted transactions from the virtual selected parent's antipast...", IDENT);

        drop(consensus_session);

        Ok(())
    }

    /// Resync the txindex acceptance data along the added vsp chain path.
    fn sync_vspc_segment(&mut self, start_hash: Hash, end_hash: Hash) -> TxIndexResult<()> {
        info!("[{0}] Syncing: accepted and unaccepted transactions along the vspc from {0} to {1}...", IDENT, start_hash, end_hash,);

        let consensus_session = futures::executor::block_on(self.consensus_manager.consensus().session_blocking());

        // Sanity check: Assert we are resyncing towards a vspc block.
        assert!(consensus_session.is_chain_block(end_hash)?);

        let start_daa_score = consensus_session.get_header(start_hash)?.daa_score;
        let end_daa_score = consensus_session.get_header(end_hash)?.daa_score;
        let mut checkpoint_hash = start_hash;

        let total_accepted_transactions = 0u64;
        let total_accepted_vspc_blocks = 0u64;
        let total_included_transactions = 0u64;
        let total_blocks = 0u64;

        // 1) remove all potentially re-orged txindex data.
        if !consensus_session.is_chain_block(start_hash)? {
            let target_hash = consensus_session.find_highest_common_chain_block(start_hash, end_hash)?;
            while checkpoint_hash != target_hash {
                info!(
                    "[{0}] Syncing: unpopulating segment of accepted and unaccepted transactions along the vspc from {0} to {1}...",
                    IDENT, checkpoint_hash, target_hash,
                );

                let chain_path =
                    consensus_session.get_virtual_chain_from_block(start_hash, Some(target_hash), Some(self.resync_chunk_size()))?;

                // Gather acceptance data
                let mut acceptance_reindexer = TxIndexAcceptanceDataReindexer::new();
                let chain_path =
                    consensus_session.get_virtual_chain_from_block(start_hash, Some(target_hash), Some(self.resync_chunk_size()))?;
                let end_segment_hash = *chain_path.checkpoint_hash().expect("expected a chain path");
                let chain_unaccepted_data = consensus_session.get_blocks_acceptance_data(chain_path.added.as_slice()).unwrap();
                acceptance_reindexer
                    .remove_unaccepted_acceptance_data(chain_path.removed, chain_unaccepted_data);
                drop(chain_path);
                drop(chain_unaccepted_data);

                // Gather unaccepted data
                let mut inclusion_reindexer = TxIndexInclusionDataReindexer::new();
                let none_vspc_blocks = consensus_session.get_hashes_between(
                    checkpoint_hash, 
                    end_segment_hash, 
                    usize::MAX, // function call is limited via high `end_segment_hash`.
                    true
                )?.0;
                for hash in none_vspc_blocks.into_iter() {
                    inclusion_reindexer.remove_transactions(consensus_session.get_block_transactions(hash)?.into_iter().filter_map(
                        move |tx| {
                            if self.store.has_accepted_transaction_offset(tx.id()).ok().is_some_and(|res| res) {
                                None
                            } else {
                                Some(tx)
                            }
                        },
                    ))
                }
                drop(none_vspc_blocks);

                // Commit acceptance data
                self.store.remove_accepted_transaction_offsets(acceptance_reindexer.unaccepted_block_hashes());
                total_accepted_transactions += acceptance_reindexer.unaccepted_block_hashes().len() as u64;
                self.store.remove_merged_block_acceptance(acceptance_reindexer.unaccepted_block_hashes());
                total_blocks += acceptance_reindexer.unaccepted_block_hashes().len() as u64;
                drop(acceptance_reindexer);

                // Commit unaccepted data
                self.store.remove_unaccepted_transaction_offsets(inclusion_reindexer.included_tx_offsets());
                total_included_transactions += inclusion_reindexer.unincluded_tx_ids() as u64;
                drop(inclusion_reindexer);

                // Commit new sink as last step - sync checkpoint
                self.store.set_sink(end_segment_hash);
                let checkpoint_daa_score = consensus_session.get_header(segment_end_hash)?.daa_score;

                // Log progress
                info!(
                    "[{0}] Syncing: Removed re-orged data from {1} to {2}, {3} / {4} chain blocks ({5:.2} %), totaling {6} blocks with {7} accepted and {8} unaccepted txs",
                    IDENT,
                    checkpoint_hash,
                    (checkpoint_daa_score - start_daa_score),
                    (end_daa_score - start_daa_score),
                    ((checkpoint_daa_score - start_daa_score) as f64 / (end_daa_score - start_daa_score) as f64),
                    total_blocks,
                    total_accepted_transactions,
                    total_included_transactions,
                );

                // set new checkpoint_hash
                checkpoint_hash = segment_end_hash
            }
        }

        // 2) fill txindex with all newly added txindex data.
        while checkpoint_hash != end_hash {
            info!(
                "[{0}] Syncing: Populating segment of accepted and unaccepted transactions along the vspc from {0} to {1}...",
                IDENT, check_point_hash, end_hash,
            );

            // Gather acceptance data
            let mut acceptance_reindexer = TxIndexAcceptanceDataReindexer::new();
            let chain_path =
                consensus_session.get_virtual_chain_from_block(checkpoint_hash, Some(end_hash), Some(self.resync_chunk_size()))?;
            let end_segment_hash = *chain_path.added.last().unwrap();
            let chain_acceptance_data = consensus_session.get_blocks_acceptance_data(chain_path.added.as_slice()).unwrap();
            acceptance_reindexer.add_accepted_acceptance_data(chain_path.added, chain_acceptance_data);
            drop(chain_path);
            drop(chain_acceptance_data);

            // Gather unaccepted data
            let mut inclusion_reindexer = TxIndexInclusionDataReindexer::new();
            let none_vspc_blocks = consensus_session
                .get_hashes_between(
                    checkpoint_hash,
                    end_segment_hash,
                    usize::MAX, // function call is limited via high `end_segment_hash`.
                    true,
                )?
                .0;
            for hash in none_vspc_blocks.into_iter() {
                inclusion_reindexer.add_transactions(
                    hash,
                    consensus_session.get_block_transactions(hash)?.into_iter().enumerate().filter_map(move |(i, tx)| {
                        if (acceptance_reindexer.accepted_tx_offsets().contains_key(&tx.id())
                            || self.store.has_accepted_transaction_offset(tx.id()).ok().is_some_and(|res| res))
                        {
                            None
                        } else {
                            Some((i as TransactionIndexType, tx))
                        }
                    }),
                )
            }
            drop(none_vspc_blocks);

            // Commit acceptance data
            self.store.add_accepted_transaction_offsets(acceptance_reindexer.accepted_tx_offsets());
            total_accepted_transactions += acceptance_reindexer.accepted_tx_offsets().len() as u64;
            self.store.add_merged_block_acceptance(acceptance_reindexer.accepted_block_acceptance_data());
            total_blocks += acceptance_reindexer.accepted_block_acceptance_data().len() as u64;
            drop(acceptance_reindexer);

            // Commit unaccepted data
            self.store.add_unaccepted_transaction_offsets(inclusion_reindexer.included_tx_offsets());
            total_included_transactions += inclusion_reindexer.included_tx_offsets().len() as u64;
            drop(inclusion_reindexer);

            // Commit new sink as last step - sync checkpoint
            self.store.set_sink(end_segment_hash);
            let checkpoint_daa_score = consensus_session.get_header(checkpoint_hash)?.daa_score;

            // Log progress
            info!(
                "[{0}] Syncing: Populated from {1} to {2}, {3} / {4} chain blocks ({5:.2} %), totaling {6} blocks with {7} accepted and {8} unaccepted txs",
                IDENT,
                checkpoint_hash,
                end_segment_hash,
                (checkpoint_daa_score - start_daa_score),
                (end_daa_score - start_daa_score),
                ((checkpoint_daa_score - start_daa_score) as f64 / (end_daa_score - start_daa_score) as f64),
                total_blocks,
                total_accepted_transactions,
                total_included_transactions,
            );

            // set new checkpoint_hash
            checkpoint_hash = end_segment_hash
        }

        // Finished
        drop(consensus_session);
        info!(
            "[{0}] Finished Syncing accepted and unaccepted transactions along the vspc from {0} to {1}...",
            IDENT, start_hash, end_hash,
        );

        Ok(())
    }

    fn resync(&mut self) -> TxIndexResult<()> {
        let txindex_sink = self.store.get_sink()?;
        let txindex_source = self.store.get_source()?;

        let consensus_session = futures::executor::block_on(self.consensus_manager.consensus().session_blocking());
        let consensus_sink = consensus_session.get_sink();
        let consensus_source = consensus_session.get_source();
        let tips = consensus_session.get_tips();

        if !self.is_source_synced()? {
            // txindex's source is not synced
            // Source is not synced
            // Resync whole txindex from scratch - easiest way.
            // TODO: explore better way via iterating txindex database, and remove if including block of offsets are not in consensus...
            self.store.delete_all();
            let start_hash = consensus_session.get_source();
            let end_hash = consensus_session.get_sink();
            self.sync_vspc_segment(start_hash, end_hash);
            self.sync_tips();
        } else if !self.is_sink_synced()? {
            // txindex's sink is not synced
            let end_hash = consensus_session.get_sink();
            let start_hash = self.store.get_sink()?.unwrap_or(default);
            self.sync_vspc_segment(start_hash, end_hash);
            // we expect tips to not be synced if the sink is not
            self.sync_tips();
        } else if !self.are_tips_synced()? {
            self.sync_tips();
        }

        drop(consensus_session);

        Ok(())
    }

    fn are_tips_synced(&self) -> TxIndexResult<bool> {
        let consensus_session = futures::executor::block_on(self.consensus_manager.consensus().session_blocking());
        let consensus_tips: BlockHashSet = consensus_session.get_tips().into_iter().collect();
        match self.store.get_tips()? {
            Some(tx_index_tips) => Ok(tx_index_tips == consensus_tips),
            None => Ok(false),
        }
    }

    fn is_sink_synced(&self) -> TxIndexResult<bool> {
        let consensus_session = futures::executor::block_on(self.consensus_manager.consensus().session_blocking());
        let consensus_sink = consensus_session.get_sink();
        match self.store.get_sink()? {
            Some(txindex_sink) => Ok(txindex_sink == consensus_sink),
            None => Ok(false),
        }
    }

    fn is_source_synced(&self) -> TxIndexResult<bool> {
        let consensus_session = futures::executor::block_on(self.consensus_manager.consensus().session_blocking());
        let consensus_source = consensus_session.get_source();
        match self.store.get_source()? {
            Some(txindex_source) => Ok(tindex_source == consensus_source),
            None => Ok(false),
        }
    }

    // TODO: move this to a txindex config
    fn resync_chunk_size(&self) -> usize {
        max(self.config.mergeset_size_limit as usize, MIN_RESYNC_CHUNK_SIZE)
    }

    fn get_transaction_offsets(self, transaction_ids: Vec<TransactionId>) -> TxIndexResult<Vec<TransactionOffset>> {
        todo!()
    }

    fn get_transaction_acceptance_data(self, transaction_ids: Vec<TransactionId>) -> TxIndexResult<Vec<TransactionAcceptance>> {
        todo!()
    }

    fn get_transactions_by_offsets(self, transaction_offsets: TransactionOffsets) -> TxIndexResult<Vec<Transaction>> {
        todo!()
    }

    fn get_transaction_by_ids(self, transaction_ids: Vec<TransactionId>) -> TxIndexResult<Vec<Option<Transaction>>> {
        todo!()
    }

    fn get_transaction_by_id(self, transaction_id: Vec<TransactionId>) -> TxIndexResult<Vec<Option<Transaction>>> {
        todo!()
    }

    fn get_transaction_offset(self, transaction_id: Vec<TransactionId>) -> TxIndexResult<Option<TransactionOffset>> {
        todo!()
    }

    fn get_transaction_acceptance_datum(self, transaction_id: Vec<TransactionId>) -> TxIndexResult<Option<TransactionAcceptance>> {
        todo!()
    }

    fn get_transaction_by_offset(self, transaction_offset: TransactionOffset) -> TxIndexResult<Option<Transaction>> {
        todo!()
    }

    fn update_block_added(&mut self) -> TxIndexResult<()> {
        todo!()
    }

    fn update_pruned_block(&mut self) -> TxIndexResult<()> {
        todo!()
    }

    fn update_acceptance_data(&mut self) -> TxIndexResult<()> {
        todo!()
    }

    fn sync_unaccepted_data(&mut self) -> TxIndexResult<()> {
        todo!()
    }

    fn sync_acceptance_data(&mut self, start_hash: Hash, end_hash: Hash) -> TxIndexResult<()> {
        todo!()
    }

    fn update_via_inclusion(&mut self) -> TxIndexResult<()> {
        todo!()
    }

    fn update_via_acceptance(&mut self) -> TxIndexResult<()> {
        todo!()
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

#[cfg(test)]
mod tests {
    // TODO
}
