use kaspa_consensus_core::{
    config::Config, 
    block::Block, 
    BlockHashMap, 
    acceptance_data::{
        MergesetBlockAcceptanceData, 
        BlockAcceptanceData}, 
        tx::{
            TransactionId, 
            TransactionIndexType, 
            TransactionReference, 
            Transaction, 
            TransactionIndexType,
            COINBASE_TRANSACTION_INDEX
        }, HashMapCustomHasher, BlockHashSet};
use kaspa_consensusmanager::{ConsensusManager, ConsensusResetHandler, ConsensusSessionBlocking};
use kaspa_core::trace;
use kaspa_database::prelude::DB;
use kaspa_hashes::Hash;
use kaspa_utils::arc::ArcExtensions;
use parking_lot::RwLock;
use std::{
    fmt::Debug,
    sync::{Arc, Weak}, cmp::min, collections::{hash_map::{Entry, VacantEntry, OccupiedEntry}, HashMap}, hash::Hash, iter::Zip, ops::Sub,
};

use crate::{
    errors::{TxIndexResult, TxIndexError}, 
    stores::store_manager::TxIndexStore, 
    api::TxIndexApi, 
    IDENT, 
    model::transaction_entries::{TransactionOffset, TransactionOffsets, TransactionEntry, TransactionEntriesById}, 
    params::TxIndexParams,
    reindexers::TxIndexReindexers
};

pub struct TxIndex {
    config: Arc<Config>,
    consensus_manager: Arc<ConsensusManager>,
    reindexers: TxIndexReindexers,
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
                
        let mut txindex = Self { 
            config,
            consensus_manager: consensus_manager.clone(), 
            store: TxIndexStore::new(txindex_db, consensus_db),
            reindexers: TxIndexReindexers::new(), 
        };

        let txindex = Arc::new(RwLock::new(txindex));
        consensus_manager.register_consensus_reset_handler(Arc::new(TxIndexConsensusResetHandler::new(Arc::downgrade(&txindex))));

        Ok(txindex)
    }
}

impl TxIndexApi for TxIndex {

    /// Resync the txindexes included transactions from the dag tips down to the vsp chain. 
    fn sync_unaccepted_data(&mut self) -> TxIndexResult<()> {

        info!("Resyncing the utxoindex...");

        assert!(self.is_sink_synced() && self.is_source_synced());

        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session_blocking());

        let consensus_tips: BlockHashSet = session.get_tips().collect();
        let sink = session.get_sink();
        consensus_tips.remove(&sink); // we can remove the sink from the tips. 
        let to_resync_tips = match self.store.get_tips()? {
            Some(txindex_tips) => {
                consensus_tips.sub(&txindex_tips)
            }
            None => consensus_tips
        };

        for tip in to_resync_tips.into_iter().filter(move |tip| tip != sink ) {

            let end_hash = session.find_highest_common_chain_block(tip, sink)?;
            
            loop {

                let (hashes, end_hash) = session.get_hashes_between(
                    end_hash, tip, 
                    self.config.mergeset_size_limit // TODO: perhaps use `min()` between constant and mergeset_size_limit. 
                )?;

                for hash in hashes.into_iter() {
                    session.get_block(hash)?;
                    self.reindexers.block_added_reindexer.add_blocks_transactions(
                        block, 
                    );
                }

                
            }
        }

        self.store.add_unaccepted_transaction_offsets(
            self.reindexers.block_added_reindexer.added_transaction_offsets(), 
        );

    Ok(())
    }

    /// Resync the txindex acceptance data along the added vsp chain path. 
    fn sync_acceptance_data(&mut self, start_hash: Hash, end_hash: Hash) -> TxIndexResult<()> {

        let consensus_session = futures::executor::block_on(self.consensus_manager.consensus().session_blocking());
        
        // assert we are resyncing along the vsp chain path. 
        assert!(consensus_session.is_chain_block(start_hash));
        assert!(consensus_session.is_chain_block(end_hash));
        
        // If start hash is not a chain block we take the highest common chain block. 
        let checkpoint_hash = consensus_session.find_highest_common_chain_block(start_hash, end_hash)?;
        // 1) resync fom checkpoint chain_block to `end_hash`
        loop{

            for (i, hash) in consensus_session.get_virtual_chain_from_block(checkpoint_hash, Some(end_hash), Some(self.config.mergeset_size_limit as usize))?.added.into_iter().enumerate() {
                let block_acceptance_data = consensus_session.get_block_acceptance_data(hash)?;
                if i == self.config.mergeset_size_limit || hash == end_hash {
                    end_segment = hash;
                }
            };

            self.store.add_transaction_entries(
                transaction_entries_by_id, 
                true, 
                true
            );
            
            if end_segment == end_hash {
                break
            } else {
                checkpoint_hash = end_segment
            };
        }

        Ok(())
    }

    fn get_sync_state(&self) -> TxIndexResult<TxIndexSyncState> {
        
    }

    fn resync(&mut self) -> TxIndexResult<()> {
        
        let txindex_sink = self.store.get_sink()?;
        let txindex_source = self.store.get_source()?;
        
        let consensus_session = futures::executor::block_on(self.consensus_manager.consensus().session_blocking());
        let consensus_sink = consensus_session.get_sink();
        let consensus_source = consensus_session.get_source();
        
        //TODO: prune db if txindex source != consensus source.. 

        let tips = consensus_session.get_tips();

        if !self.is_source_synced() { // txindex's source is not synced
            // Source is not synced
            // Resync whole txindex from scratch - if we have unsynced source there is no other way. 
            // TODO: better way is perhaps to iterate txindex database, and remove if including block of offsets are not in consensus...
            self.store.delete_all();
            self.resync_blockdag_segment(consensus_source, consensus_sink)?;
        } else if !self.is_sink_synced() { // txindex's sink is not synced
            assert!(consensus_session.is_chain_ancestor_of(txindex_sink, consensus_sink)); // sanity check
            self.resync_accept(txindex_sink, consensus_sink)
        };
        
        drop(consensus_session);

        if !self.are_tips_synced() {
            self.re()?
        };

    }

    fn are_tips_synced(&self) -> TxIndexResult<bool> {
        let consensus_session = futures::executor::block_on(self.consensus_manager.consensus().session_blocking());
        let consensus_tips: BlockHashSet = consensus_session.get_tips().collect();
        let txindex_tips = self.store.get_tips()?.collect();
        if let Some(txindex_tips) = txindex_tips {
            return Ok(consensus_tips == txindex_tips)
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
        min(self.config.mergeset_size_limit as usize, 1024)
    }

    fn get_transaction_offsets(
        self, 
        transaction_ids: Vec<TransactionId>, 
    ) -> TxIndexResult<Vec<TransactionOffset>> {
        todo!()
    }

    fn get_transaction_acceptance_data(
        self, 
        transaction_ids: Vec<TransactionId>, 
    ) -> TxIndexResult<Vec<TransactionAcceptance>> {
        todo!()
    }

    fn get_transactions_by_offsets(
        self, 
        transaction_offsets: TransactionOffsets,
    ) -> TxIndexResult<Vec<Transaction>> {
        todo!()
    }

    fn get_transaction_by_ids(
        self,
        transaction_ids: Vec<TransactionId>,
     ) -> TxIndexResult<Vec<Option<Transaction>>> {
        todo!()
    }

    fn get_transaction_by_id(
        self,
        transaction_id: Vec<TransactionId>,
    ) -> TxIndexResult<Vec<Option<Transaction>>> {
        todo!()
    }

    fn get_transaction_offset(
        self, 
        transaction_id: Vec<TransactionId>, 
    ) -> TxIndexResult<Option<TransactionOffset>> {
        todo!()
    }

    fn get_transaction_acceptance_datum(
        self, 
        transaction_id: Vec<TransactionId>, 
    ) -> TxIndexResult<Option<TransactionAcceptance>> {
        todo!()
    }

    fn get_transaction_by_offset(
        self, 
        transaction_offset: TransactionOffset,
    ) -> TxIndexResult<Option<Transaction>> {
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
            txindex.write().resync().unwrap();;
        }
    }
}
