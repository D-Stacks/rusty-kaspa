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
    sync::{Arc, Weak}, cmp::min, collections::{hash_map::{Entry, VacantEntry, OccupiedEntry}, HashMap}, hash::Hash, iter::Zip,
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

/// 256 blocks per chunk - this corresponds to ~ 62976 txs under current blocksize max load. 
/// this should be adjusted on blocksize changes. 
/// note: max should not exceed max mergeset size. 
const MAX_RESYNC_CHUNK_SIZE: usize = 256;

pub struct TxIndex {
    config: Arc<Config>,
    params: TxIndexParams, 
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
        
        assert!(config.merge_depth < MAX_RESYNC_CHUNK_SIZE); // Ensure we don't go beyond max merge depth
        
        let mut txindex = Self { 
            config,
            consensus_manager: consensus_manager.clone(), 
            store: TxIndexStore::new(txindex_db, consensus_db),
            params,
            reindexers: TxIndexReindexers::new(), 
        };

        let txindex = Arc::new(RwLock::new(txindex));
        consensus_manager.register_consensus_reset_handler(Arc::new(TxIndexConsensusResetHandler::new(Arc::downgrade(&txindex))));

        Ok(txindex)
    }
}

impl TxIndexApi for TxIndex {

    /// Resync the txindexes included transactions from the dag tips down to the vsp chain. 
    fn resync_from_tips(&mut self, consensus_session: ConsensusSessionBlocking<'a>) -> TxIndexResult<()> {
        // get included transactions between tips and sink
        let sink = consensus_session.get_sink();
        for tip in consensus_session.get_tips().into_iter() {
            // we expect one tip to be the sink, we can "short-circuit" this tip. 
            if tip == sink{
                continue;
            }

            let end_hash = consensus_session.find_highest_common_chain_block(tip, sink)?;


            // TODO: Possible optimization is to use the common ancestor between tip and sink, instead of the sink
            loop {

                let (hashes, end_hash) = consensus_session.get_hashes_between(end_hash, tip, MAX_RESYNC_CHUNK_SIZE)?;

                for hash in hashes.into_iter() {
                    consensus_session.get_block(hash)?;
                    self.reindexers.add_blocks_transactions(
                        block, 
                        false // we do not want to overwrite accepted blocks
                    );
                }

                self.reindexers.

                
            }
        }
    Ok(())
    }

    /// Resync the txindex along an added vsp chain path. 
    /// 
    /// Note: `end_hash` is expected to be a chain block. 
    fn resync_block_dag_segment(&mut self, consensus_session: ConsensusSessionBlocking<'a>, start_hash: Hash, end_hash: Hash) -> TxIndexResult<()> {
        
        let end_segment: Hash;

        // If start hash is not a chain block we take the highest common chain block. 
        let checkpoint_hash = consensus_session.find_highest_common_chain_block(start_hash, end_hash)?;

        // 1) remove from start_hash to checkpoint
        if start_hash != checkpoint_hash {
            loop {
                let (end_hashes, end_segment) = consensus_session.get_hashes_between(checkpoint_hash, start_hash, MAX_RESYNC_CHUNK_SIZE)?;
                
                if end_segment == start_hash {
                    break
                } else {

                }
            }
        }

        // 2) resync fom checkpoint chain_block to `end_hash`
        loop{

            for (i, hash) in consensus_session.get_virtual_chain_from_block(checkpoint_hash, end_hash, MAX_RESYNC_CHUNK_SIZE)?.added.into_iter().enumerate() {
                let block_acceptance_data = consensus_session.get_block_acceptance_data(hash)?;
                if i == MAX_RESYNC_CHUNK_SIZE || hash == end_hash {
                    end_segment = hash;
                } else { continue }
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
        
        let tips = consensus_session.get_tips();

        if Some(txindex_source) != consensus_source || txindex_source.is_none() {
            // Source is not synced
            // Resync whole txindex from scratch - if we have unsynced source there is no other way. 
            self.store.delete_all();
            self.resync_segment(consensus_source, consensus_sink)?;
        } else if Some(txindex_sink) != consensus_sink || txindex_sink.is_none() {
            // Sink is not synced
            // We may resync from the sink of the txindex to the consensus sink
        } else {
            // Tips are not synced. 
        }

        match txindex_source {
            Some(txindex_source) => {
                if txindex_source != consensus_source {
                    // Resync whole txindex from scratch - if we have unsynced source there is no other way. 
                    self.store.delete_all();
                    self.resync_segment(consensus_source, consensus_sink)?;
                } else if match txindex_sink {
                    Some(txindex_sink) => {
                        if txindex_sink != consensus_sink {
                            self.resync_segment(txindex_sink, consensus_sink)
                        }
                    }
                }
            },
            None => {
                // Resync whole txindex from scratch - if we have unsynced source there is no other way. 
                self.store.delete_all();
                self.resync_segment(consensus_source, consensus_sink)?;
            }
                } {
                    
                }
            }
            None => {

            },
        }

        // 1) Resync up the virtual selected parent chain form source
        self.resync_segment(source, sink)?;

        // 2) Resync included transactions from the tips to sink common ancestor
        self.resync_tips(sink, tips)?;
    
        Ok(())
    }

    fn is_synced(self) -> TxIndexResult<bool> {
        todo!()
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

    fn update_via_inclusion(&mut self) -> TxIndexResult<()> {
        todo!()
    }

    fn update_via_acceptance(&mut self) -> TxIndexResult<()> {
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
