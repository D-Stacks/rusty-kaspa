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
            COINBASE_TRANSACTION_INDEX
        }, HashMapCustomHasher, BlockHashSet};
use kaspa_consensusmanager::{ConsensusManager, ConsensusResetHandler, ConsensusSession};
use kaspa_core::trace;
use kaspa_database::prelude::DB;
use kaspa_hashes::Hash;
use kaspa_utils::arc::ArcExtensions;
use parking_lot::RwLock;
use std::{
    fmt::Debug,
    sync::{Arc, Weak}, cmp::min, collections::{hash_map::{Entry, VacantEntry, OccupiedEntry}, HashMap}, hash::Hash, iter::Zip,
};

use crate::{
    errors::{TxIndexResult, TxIndexError}, 
    stores::store_manager::TxIndexStore, 
    api::TxIndexApi, 
    IDENT, 
    model::{transaction_entries::{TransactionOffset, TransactionOffsets}}, 
    params::TxIndexParams,
    reindexers::{AcceptanceDataReindexer, BlockAddedReindexer}
};

pub struct SyncStaus {
    is_acceptance_synced: bool,
    is_inclustion_synced: bool,
    is_reindex_configuration_synced: bool, 
}

/// 256 blocks per chunk - this corresponds to ~ 62976 txs under current blocksize max load. 
/// this should be adjusted on blocksize changes. 
/// note: min is defined by max mergeset size. 
const MAX_RESYNC_CHUNK_SIZE: usize = 256;

pub struct TxIndex {
    config: Arc<Config>,
    params: TxIndexParams, 
    consensus_manager: Arc<ConsensusManager>,
    acceptance_data_reindexer: AcceptanceDataReindexer,
    block_added_reindexer: BlockAddedReindexer,
    store: TxIndexStore,
}

impl TxIndex {
    /// Creates a new [`TxIndex`] within a [`RwLock`]
    pub fn new(
        config: Arc<Config>, 
        consensus_manager: Arc<ConsensusManager>, 
        txindex_db: Arc<DB>, 
        consensus_db: Arc<DB>,
        params: TxIndexParams,
    ) -> TxIndexResult<Arc<RwLock<Self>>> {
        
        assert!(config.merge_depth < MAX_RESYNC_CHUNK_SIZE); //
        
        let mut txindex = Self { 
            config,
            params, 
            consensus_manager: consensus_manager.clone(), 
            store: TxIndexStore::new(txindex_db, consensus_db),
            params,
            acceptance_data_reindexer: AcceptanceDataReindexer::new(
                process_acceptance,
                
            ),
            block_added_reindexer: todo!(), 
        };

        // Check if param changes warrent a from scratch reset. 
        let former_params = txindex.store.get_params()?;

        if former_params.is_none() || params != former_params.expect("expected params") {
            txindex.resync(true)
        } else {
            txindex.resync(false)
        }

        txindex.store.set_params(params)?; //we can reset to new params after resyncing is done. 

        let txindex = Arc::new(RwLock::new(txindex));
        consensus_manager.register_consensus_reset_handler(Arc::new(TxIndexConsensusResetHandler::new(Arc::downgrade(&txindex))));

        Ok(txindex)
    }
}

impl TxIndexApi for TxIndex {
      
    /// Checks if inclusion data is synced
    fn is_inclusion_synced(&self) -> TxIndexResult<bool> {
        
        // 0) Get access to the consensus session
        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session());

        let last_block_added = self.store.get_last_block_added()?;
        
        // 1) check if last block added is present
        let last_block_added = if let Some(last_block_added) = last_block_added{
            last_block_added
        } else {
            return Ok(false)
        };

        // 2) check that it is a tip
        let tips = session.get_tips();
        if !tips.has(last_block_added) { //quick and dirty check for most cases. 
            trace!("[{0}] sync status is {1}", IDENT, false);
            return Ok(false)
        }

        
        // 3) check we processed all transactions between other tips and the sink
        tips.remove(last_block_added);
        let sink = session.get_sink();
        let start_hash = tips.get(0);
        for hash in tips {
            for (hashes, end_hash) in session.get_hashes_between(start_hash, sink, MAX_RESYNC_CHUNK_SIZE) {
                for hash in hashes.into_iter() {
                    let block = session.get_block(hash)?;
                    for transaction in block.transactions.into_iter() {
                        if !self.store.has_transaction_entry(transaction.id()) {
                            return Ok(false)
                        }
                    }
                }
            }
        
        }
        
        Ok(true)
    }

    /// Checks if acceptence data is synced
    /// 
    /// Note: this returns true also if params do require to sync via acceptance
    fn is_acceptance_synced(&self) -> TxIndexResult<bool> {
                    
        // 0) Get access to the consensus session
        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session());


        let txindex_sync = self.store.get_sink()?;
        
        // 1) check if txindex sync is added is present
        let txindex_sync = if let Some(txindex_sync) = txindex_sync{
            txindex_sync
        } else {
            return Ok(false)
        };

        // 2) check if sink is same as consensus
        let tips = session.get();
        if !tips.has(last_block_added) { 
            trace!("[{0}] sync status is {1}", IDENT, false);
            return Ok(false)
        }

        
        // 3) check we processed all transactions between other tips and the sink
        tips.remove(last_block_added);
        let sink = session.get_sink();
        let start_hash = tips.get(0);
        for hash in tips {
            for (hashes, end_hash) in session.get_hashes_between(start_hash, sink, MAX_RESYNC_CHUNK_SIZE) {
                for hash in hashes.into_iter() {
                    let block = session.get_block(hash)?;
                    for transaction in block.transactions.into_iter() {
                        if !self.store.has_transaction_entry(transaction.id()) {
                            return Ok(false)
                        }
                    }
                }
            }
        
        }
        
        Ok(true)
    }


    fn resync_from_history_root() {
        // 1) if sources are the same exit as populated
        if consensus_source == txindex_source {
            return Ok(())
        }

        let consensus_history_root = session.get_dag_source();
        let txindex_history_root = self.store.get_pruning_point();

        if consensus_history_root == txindex_history_root {
            return Ok(())
        }

        // 2) set starting point:
        let start_hash = consensus_source;
        
        if session.is_chain_block(start_hash)? {
            let block_acceptance = BlockAcceptanceData::from(( hash, session.get_block_acceptance_data(hash)))?;
            self.acceptance_data_reindexer.add(block_acceptance, None);
            self.store.add_transaction_entries(
                self.block_added_reindexer.to_add_transaction_entries, 
                true, 
                true,
            );
            self.acceptance_data_reindexer.clear();
        } else {
            let block = session.get_block(hash)?;
            self.block_added_reindexer.add(block);
            self.store.add_transaction_entries(
                self.block_added_reindexer.to_add_transaction_entries, 
                false, 
                true
            );
            self.block_added_reindexer.clear();
        };
    
    while start_hash != txindex_source {
        self.acceptance_data_reindexer.clear();
        self.block_added_reindexer.clear();
        
        if self.params.process_via_inclusion() {
            let (hashes_between, end_hash) = session
            .get_hashes_between(
                start_hash, end_hash, MAX_RESYNC_CHUNK_SIZE
            )?;
        } else {
            end
        }
        
        if self.params.process_via_acceptance() {
            let chain_blocks = session
            .get_virtual_chain_from_block(
                start_hash, end_hash
            )?.added;
            
            let block_acceptance_data = chain_blocks
            .iter()
            .zip(
                session
                .get_blocks_acceptance_data(
                    chain_blocks.as_slice()
                )?
                .into_iter()
                .map( move |acceptance_data| {
                    acceptance_data.unwrap_or_clone()
                }
                )
            ).collect::<BlockAcceptanceData>();
        }

        self.block_added_reindexer.add(block_acceptance_data);

        for hash in hashes_between {
            if !block_acceptance_data.has(hash) {
                self.block_added_reindexer.add(session.get_block(hash))
                }
            }
        }
    self.store.add_transaction_entries(
        self.block_added_reindexer.to_add_transaction_entries, 
        false,
        true
    );
    self.block_added_reindexer.clear();
    self.store.add_transaction_entries(
        self.acceptance_data_reindexer.to_add_transaction_entries, 
        true,
        true
    );
    self.acceptance_data_reindexer.clear();
    }

    fn resync_to_terminal(session: ConsensusSession) {
        todo!()
    }

    fn resync_to_sink(session: ConsensusSession) {
        todo!()
    }

    fn get_sync_status(&self) -> TxIndexResult<SyncStaus> {
        todo!()
    }

    fn resync(&mut self) -> TxIndexResult<()> {
    
        // TODO: Resyncing can probably be greatly optimized:
        // - this should be done if high tps causes extensive sync times:
        // 1) sync from txindex last_block_added / sink to the consensuses dag terminal
        // 2) sync from consensus history_root to txindex pruning point
        // 3) re-indexing the available segment by param diff compared to the last run
        // 4) if the txindex syncs both via acceptance and block added, these can be done in tandem, with one pass.
        
        self.store.delete_all()?;

        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session());

        let consensus_source = session.get_dag_source();
        
        

        if self.params.process_via_acceptance_data() {

        }

        if self.params.process_via_block_added() {
            let start_hash = consensus_source;
            let tips: BlockHashSet = session.get_tips().collect();
                while !tips.has(start_hash) { 
                    let (to_process_hashes, end_hash) = session.get_hashes_between(start_hash, session.get_sink(), MAX_RESYNC_CHUNK_SIZE)?;
                    to_process_hashes.insert(0, start_hash); // prepend, since this gets skipped in `session.get_hashes_between`
                    self.block_added_reindexer.add(session.get_block(hash)?)
                }
                self.store.add_transaction_entries(
                    self.block_added_reindexer.to_add_transaction_entries, 
                    !self.params.process_via_acceptance_data(), //if we process by acceptance we do not overwrite it. 
                    true
                )?;
                self.block_added_reindexer.clear();
                start_hash = end_hash;
        };

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
