use kaspa_consensus_core::{config::Config, block::Block, BlockHashMap, acceptance_data::{MergesetBlockAcceptanceData, BlockAcceptanceData}, tx::{TransactionId, TransactionIndexType, TransactionReference, Transaction, COINBASE_TRANSACTION_INDEX}, HashMapCustomHasher};
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
    reindexers::{AcceptanceDataReindexer, BlockAddedReindexer}
};


enum TerminalEndPoint {
    Sink,
    Tips,
    HistoryRoot
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


    fn populate_to_terminal(&mut self, 
        start_sink: Option<Hash>,
        end_sink: Option<Hash>, 
        start_last_block_added: Option<Hash>,
        end_tips: Option<Vec<Hash>>
    ) -> TxIndexResult<()> {
        // 0) Get access to the consensus session
        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session());
    }

    fn populate_from_source(&mut self) -> TxIndexResult<()> {
        // 0) Get access to the consensus session
        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session());
        let consensus_source = session.get_dag_source();
        let txindex_source = self.store.get_pruning_point();
        
        // 1) if sources are the same exit as populated
        if consensus_source == txindex_source {
            return Ok(())
        }

        // 2) set starting point:
        let start_hash = consensus_source;

        // 3) define appropriate closure
        let processing_closure = match ( self.params.process_via_acceptance_data(), self.params.process_via_block_added() ) {
            (true, true) =>  move |start_hash: Hash, end_hash: Hash| {
                while start_hash != txindex_source {
                    self.acceptance_data_reindexer.clear();
                    self.block_added_reindexer.clear();
                    let (hashes_between, start_hash) = session.get_hashes_between(consensus_source, txindex_source, MAX_RESYNC_CHUNK_SIZE)?;                
                    for hash in hashes_between {
                        if session.is_chain_block(hash)? {
                            acceptance_data = session.get_block_acceptance_data(hash)?;
                            self.acceptance_data_reindexer.add(
                                BlockAcceptanceData::from((hash, acceptance_data)), 
                                None
                            );
                            self.store.add_transaction_entries(
                                self.acceptance_data_reindexer.to_add_transaction_entries, 
                                true, 
                                true
                            )
                        } else {
                            let to_add_block = session.get_block(hash);
                            self.block_added_reindexer.add(to_add_block);
                            self.store.add_transaction_entries(
                                self.block_added_reindexer.to_add_transaction_entries, 
                                false, 
                                true
                            )
                        }
                    }
                }
            self.acceptance_data_reindexer.clear();
            self.block_added_reindexer.clear();
            },
            (true, false) => move |start_hash: Hash, end_hash:Hash| while start_hash != end_hash {
                self.block_added_reindexer.clear();
                let (hashes_between, start_hash) = session.get_blocks(start_hash)?;                
                for hash in hashes_between {
                    let to_add_block = session.get_block(hash);
                    self.block_added_reindexer.add(to_add_block);
                    self.store.add_transaction_entries(
                        self.block_added_reindexer.to_add_transaction_entries, 
                        false, 
                        true
                    )
                    }
                },
            (false, true) => move |start_hash, end_hash| while start_hash != end_hash {
                self.block_added_reindexer.clear();
                let (hashes_between, start_hash) = session.get_hashes_between(consensus_source, txindex_source, MAX_RESYNC_CHUNK_SIZE)?;                
                for hash in hashes_between {
                    let to_add_block = session.get_block(hash);
                    self.block_added_reindexer.add(to_add_block);
                    self.store.add_transaction_entries(
                        self.block_added_reindexer.to_add_transaction_entries, 
                        false, 
                        true
                    )
                    }
                }
        };
        let resync_acceptance_and_inclusion = move |start_hash: Hash, end_hash| {
            while start_hash != txindex_source {
                self.acceptance_data_reindexer.clear();
                self.block_added_reindexer.clear();
                let (hashes_between, start_hash) = session.get_hashes_between(consensus_source, txindex_source, MAX_RESYNC_CHUNK_SIZE)?;                
                for hash in hashes_between {
                    if session.is_chain_block(hash)? {
                        acceptance_data = session.get_block_acceptance_data(hash)?;
                        self.acceptance_data_reindexer.add(
                            BlockAcceptanceData::from((hash, acceptance_data)), 
                            None
                        );
                        self.store.add_transaction_entries(
                            self.acceptance_data_reindexer.to_add_transaction_entries, 
                            true, 
                            true
                        )
                    } else {
                        let to_add_block = session.get_block(hash);
                        self.block_added_reindexer.add(to_add_block);
                        self.store.add_transaction_entries(
                            self.block_added_reindexer.to_add_transaction_entries, 
                            false, 
                            true
                        )
                    }
                }
            }
        self.acceptance_data_reindexer.clear();
        self.block_added_reindexer.clear();
        };
        // resync 
        // in this case sync in tandem:
        if self.params.process_via_acceptance_data() && self.params.process_via_block_added() {
            
    } else if self.params.process_via_block_added() {
        while start_hash != txindex_source {
            self.block_added_reindexer.clear();
            let (hashes_between, start_hash) = session.get_hashes_between(consensus_source, txindex_source, MAX_RESYNC_CHUNK_SIZE)?;                
            for hash in hashes_between {
                let to_add_block = session.get_block(hash);
                self.block_added_reindexer.add(to_add_block);
                self.store.add_transaction_entries(
                    self.block_added_reindexer.to_add_transaction_entries, 
                    false, 
                    true
                )
                }
            }
            self.block_added_reindexer.clear();
    } else if self.params.process_via_acceptance_data() {
        let hashes_between = session.get_virtual_chain_from_block(consensus_source, txindex_source, MAX_RESYNC_CHUNK_SIZE)?;                
    } else {
        Err(TxIndexError::NoProcessingPurposeError(
            "Not Processing via block added nor acceptance".to_string()
        ))
    }
    
    Ok(())

    }



    fn resync_to_tips(&mut self, start_hash: Hash, end_hash: Hash) -> TxIndexResult<()> {
        // 0) Get access to the consensus session
        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session());
    }

    fn reindex_segment(&mut self,
        start_hash: Hash,
        end_hash: Hash, 
        remove_offsets: bool,
        remove_acceptance: bool,
        remove_offsets_via_inclusion: bool
    ) -> TxIndexResult<()> {
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
