use kaspa_consensus_core::{config::Config, block::Block, BlockHashMap, acceptance_data::MergesetBlockAcceptanceData, tx::{TransactionId, TransactionIndexType, TransactionReference, Transaction, COINBASE_TRANSACTION_INDEX}, HashMapCustomHasher};
use kaspa_consensusmanager::{ConsensusManager, ConsensusResetHandler};
use kaspa_core::trace;
use kaspa_database::prelude::DB;
use kaspa_hashes::Hash;
use parking_lot::RwLock;
use std::{
    fmt::Debug,
    sync::{Arc, Weak}, cmp::min, collections::{hash_map::Entry, HashMap}, hash::Hash,
};

use crate::{
    errors::{TxIndexResult, TxIndexError}, 
    stores::store_manager::TxIndexStore, 
    api::TxIndexApi, 
    IDENT, 
    model::{transaction_entries::{TransactionOffset, TransactionOffsets}}, 
    params::TxIndexParams,
    reindexers::{BlockAddedReindexer, AcceptanceDataReindexer},
};

/// 256 blocks per chunk - this corresponds to ~ 62976 txs under current blocksize max load. 
/// this should be adjusted on blocksize changes. 
/// note: min is defined by max mergeset size. 
const MAX_RESYNC_CHUNK_SIZE: usize = 256;

pub struct TxIndex {
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
        
        let mut txindex = Self { 
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
            
        // 1) check if last block added is present
        // 2) check that it is a tip
        // 3) check we processed all transactions between other tips and the sink

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

    /// Checks if inclusion data is synced
    /// 
    /// Note: this returns true also if params do require to sync via acceptance
    fn is_acceptance_synced(&self) -> TxIndexResult<bool> {
        if !self.params.process_via_acceptance_data() {
            return Ok(true)
        }
            
        // 1) check if sink is 
        // 2) check that it is a tip
        // 3) check we processed all transactions between other tips and the sink

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

    fn resync_acceptance_data(&mut self, clean_delete_all: bool) -> TxIndexResult<()>;

    fn resync_offsets(&mut self, clean_up_txs_by_inclusion: bool, delete_all: bool) -> TxIndexResult<()> {
    }

    fn clean(&mut self, remove_acceptance: bool, remove_accepted_offsets: bool, remove_none_accepted_offsets: bool) -> TxIndexResult<()> TxIndexResult<()> {

    }

    fn resync(&mut self, from_scratch: bool) ->TxIndexResult<()> {

    }
  
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
