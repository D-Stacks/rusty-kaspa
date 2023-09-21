use kaspa_consensus_core::{
    acceptance_data::{BlockAcceptanceData, MergesetBlockAcceptanceData},
    block::Block,
    config::Config,
    header::Header,
    tx::{Transaction, TransactionId, TransactionIndexType, TransactionIndexType, TransactionReference, COINBASE_TRANSACTION_INDEX},
    BlockHashMap, BlockHashSet, HashMapCustomHasher,
};
use kaspa_consensusmanager::{ConsensusManager, ConsensusResetHandler, ConsensusSessionBlocking};
use kaspa_core::{info, trace};
use kaspa_database::prelude::DB;
use kaspa_hashes::Hash;
use kaspa_utils::arc::ArcExtensions;
use parking_lot::RwLock;
use std::{
    cmp::min,
    collections::{
        hash_map::{Entry, OccupiedEntry, VacantEntry},
        HashMap,
    },
    fmt::Debug,
    hash::Hash,
    iter::Zip,
    ops::Sub,
    sync::{Arc, Weak},
};

use crate::{
    api::TxIndexApi,
    errors::{TxIndexError, TxIndexResult},
    model::transaction_entries::{TransactionEntriesById, TransactionEntry, TransactionOffset, TransactionOffsets},
    params::TxIndexParams,
    reindexers::{TxIndexAcceptanceDataReindexer, TxIndexBlockAddedReindexer, TxIndexBlockAddedReindexer},
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
    fn sync_unaccepted_data(&mut self) -> TxIndexResult<()> {
        info!("[{0}] Syncing unaccepted transaction offsets...", IDENT);

        // Sanity check: Assert we are resyncing unaccepted offsets after accepted offsets.
        assert!(self.is_sink_synced() && self.is_source_synced());

        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session_blocking());

        let consensus_tips: BlockHashSet = session.get_tips().collect();
        let sink = session.get_sink();
        let anticone = session.get_anticone(sink)?;
        let anticone_size = anticone.len();
        let unaccepted_blocks_synced = 0;
        let unaccepted_transactions_synced = 0;

        let mut block_added_reindexer = TxIndexBlockAddedReindexer::new();
        for (i, hash) in anticone.into_iter().enumerate() {
            let anticone_block = session.get_block(hash)?;
            block_added_reindexer.add_block_transactions(anticone_block);

            if (i % self.resync_chunk_size() == 0) || (i == anticone_size - 1) {
                // commit to DB
                unaccepted_blocks_synced += self.resync_chunk_size();
                unaccepted_transactions_synced += block_added_reindexer.added_transaction_offsets().len();
                info!(
                    "[{0}] Synced {1} / {2} unaccepted blocks ({3:.2} %), totaling {4} txs",
                    IDENT,
                    unaccepted_blocks_synced,
                    anticone_size,
                    (i as f64 / anticone_size as f64),
                    unaccepted_transactions_synced
                );
                self.store.add_unaccepted_transaction_offsets(
                    block_added_reindexer
                        .added_transaction_offsets()
                        .into_iter()
                        .filter(move |tx_id, _| !self.store.has_accepted_transaction_offset(tx_id))
                        .collect(),
                );
                block_added_reindexer.clear();
            }
        }

        Ok(())
    }

    /// Resync the txindex acceptance data along the added vsp chain path.
    fn sync_acceptance_data(&mut self, start_hash: Hash, end_hash: Hash) -> TxIndexResult<()> {
        info!("[{0}] Syncing Accepted transaction offsets...", IDENT);

        let consensus_session = futures::executor::block_on(self.consensus_manager.consensus().session_blocking());

        // Sanity check: Assert we are resyncing along the vsp chain path.
        assert!(consensus_session.is_chain_block(start_hash));
        assert!(consensus_session.is_chain_block(end_hash));
        let start_daa_score = consensus_session.get_header(start_hash)?.daa_score;
        let end_daa_score = consensus_session.get_header(end_hash)?.daa_score;
        let mut checkpoint_hash = start_hash;
        let mut acceptance_reindexer = TxIndexAcceptanceDataReindexer::new();

        loop {
            let chain_path =
                consensus_session.get_virtual_chain_from_block(*checkpoint_hash, Some(end_hash), Some(self.resync_chunk_size()))?;
            let check_point_hash = *chain_path.last_block_queried().expect("expected some chain path");
            for (i, hash) in chain_path.added.into_iter().enumerate() {
                let block_acceptance_data = consensus_session.get_block_acceptance_data(hash)?;
                acceptance_reindexer.update_acceptance(hash, Arc::new(vec![]), block_acceptance_data, Arc::new(vec![]))
            }
            let checkpoint_daa_score = consensus_session.get_header(check_point_hash)?.daa_score;
            info!(
                "[{0}] Synced {1} / {2} accepted blocks ({3:.2} %), totaling {4} txs",
                IDENT,
                (checkpoint_daa_score - start_daa_score),
                (end_daa_score - start_daa_score),
                ((checkpoint_daa_score - start_daa_score) as f64 / (end_daa_score - start_daa_score) as f64)
            );
            self.store.add_accepted_transaction_offsets(acceptance_reindexer.added_accepted_tx_offsets());
            self.store.add_merged_block_acceptance(acceptance_reindexer.added_block_acceptance());
            self.store.set_sink(acceptance_reindexer.sink());
            acceptance_reindexer.clear();
        }

        Ok(())
    }

    fn resync(&mut self) -> TxIndexResult<()> {
        let txindex_sink = self.store.get_sink()?;
        let txindex_source = self.store.get_source()?;

        let consensus_session = futures::executor::block_on(self.consensus_manager.consensus().session_blocking());
        let consensus_sink = consensus_session.get_sink();
        let consensus_source = consensus_session.get_source();

        //TODO: prune db if txindex source != consensus source..

        let tips = consensus_session.get_tips();

        if !self.is_source_synced() {
            // txindex's source is not synced
            // Source is not synced
            // Resync whole txindex from scratch - if we have unsynced source there is no other way.
            // TODO: better way is perhaps to iterate txindex database, and remove if including block of offsets are not in consensus...
            self.store.delete_all();
            self.sync_acceptance_data(consensus_source, consensus_sink)?;
        } else if !self.is_sink_synced() {
            // txindex's sink is not synced
            assert!(consensus_session.is_chain_ancestor_of(txindex_sink, consensus_sink)); // sanity check
            self.sync_acceptance_data(txindex_sink, consensus_sink)
        };

        drop(consensus_session);

        if !self.are_tips_synced() {
            self.sync_unaccepted_data()?
        };

        Ok(())
    }

    fn are_tips_synced(&self) -> TxIndexResult<bool> {
        let consensus_session = futures::executor::block_on(self.consensus_manager.consensus().session_blocking());
        let consensus_tips: BlockHashSet = consensus_session.get_tips().collect();
        let txindex_tips = self.store.get_tips()?.collect();
        if let Some(txindex_tips) = txindex_tips {
            return Ok(consensus_tips == txindex_tips);
        } else {
            Ok(false)
        }
    }

    fn is_sink_synced(&self) -> TxIndexResult<bool> {
        let consensus_session = futures::executor::block_on(self.consensus_manager.consensus().session_blocking());
        let consensus_sink = consensus_session.get_sink();
        let txindex_sink = self.store.get_sink()?;
        if let Some(txindex_sink) = txindex_sink {
            Ok(consensus_sink == txindex_sink)
        } else {
            Ok(false)
        }
    }

    fn is_source_synced(&self) -> TxIndexResult<bool> {
        let consensus_session = futures::executor::block_on(self.consensus_manager.consensus().session_blocking());
        let consensus_source = consensus_session.get_source();
        let tindex_source = self.store.get_source()?;
        if let Some(txindex_source) = tindex_source {
            Ok(consensus_source == tindex_source)
        } else {
            Ok(false)
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
