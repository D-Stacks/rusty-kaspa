use kaspa_consensus_core::{config::Config, block::Block, BlockHashMap, acceptance_data::MergesetBlockAcceptanceData, tx::{TransactionId, TransactionIndexType, TransactionReference, Transaction}, HashMapCustomHasher};
use kaspa_consensusmanager::{ConsensusManager, ConsensusResetHandler};
use kaspa_core::trace;
use kaspa_database::prelude::DB;
use kaspa_hashes::Hash;
use parking_lot::RwLock;
use std::{
    fmt::Debug,
    sync::{Arc, Weak}, cmp::min, collections::{hash_map::Entry, HashMap},
};

use crate::{errors::{TxIndexResult, TxIndexError}, stores::store_manager::TxIndexStore, api::TxIndexApi, IDENT, model::{transaction_offsets::{TransactionOffset, TransactionOffsets}, params::TxIndexParams}};

/// 256 blocks per chunk - this corresponds to ~ 62976 txs under current blocksize max load. 
/// note: min is defined by max mergeset size. 
const MAX_RESYNC_CHUNK_SIZE: usize = 256;

pub struct TxIndex {
    config: Arc<Config>,
    process_offsets_by_inclusion: bool, // TODO (when scoped configs are implemented): move this to a separate txindex config
    process_offsets_by_acceptance: bool, // TODO (when scoped configs are implemented): move this to a separate txindex config
    process_acceptance: bool, // TODO (when scoped configs are implemented): move this to a separate txindex config
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
        process_offsets_by_inclusion: bool,
        process_offsets_by_acceptance: bool,
        process_acceptance: bool,
    ) -> TxIndexResult<Arc<RwLock<Self>>> {
        
        let check_conflicts = if !(process_offsets_by_acceptance || process_offsets_by_inclusion) {
            Err(TxIndexError::ConflictingProcessingMechanics)
        } else { 
            Ok(())
        };
        if check_conflicts.is_err() {
            return check_conflicts
        }
        
        let mut txindex = Self { 
            config, 
            consensus_manager: consensus_manager.clone(), 
            store: TxIndexStore::new(txindex_db, consensus_db), 
            process_offsets_by_inclusion,
            process_offsets_by_acceptance,
            process_acceptance,
        };
        
        if !txindex.is_synced()? {
            txindex.resync()?;
        };

        txindex.store.set_params(TxIndexParams{
            process_offsets_by_inclusion,
            process_offsets_by_acceptance,
            process_acceptance,
        })?;

        let txindex = Arc::new(RwLock::new(txindex));
        consensus_manager.register_consensus_reset_handler(Arc::new(TxIndexConsensusResetHandler::new(Arc::downgrade(&txindex))));

        Ok(txindex)
    }
}

impl TxIndexApi for TxIndex {
    fn is_synced(self) -> TxIndexResult<bool> {
        
        // TODO (when pruning is implemented): Check sync i.e. if earliest header pruning point is same as txindex.
        // TODO: As an optimization, only resync elements which need to be re-synced, after failing is_synced. 

        trace!("[{0}] checking sync status...", IDENT);

        if self.process_offsets_by_acceptance && self.process_offsets_by_inclusion {
            return Err(TxIndexError::ConflictingProcessingMechanics);
        }

        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session());

        let params = self.store.get_params()?; //check if params changed i.e. need for sync under new premise
        if params.is_none() {
            return Some(false);
        }
        let params = params.unwrap();
        
        if !( 
            params.process_acceptance == self.process_acceptance &&
            params.process_offsets_by_acceptance == self.process_offsets_by_acceptance &&
            params.process_offsets_by_inclusion == self.process_offsets_by_inclusion
        ) { 
            return Some(false);
        }


        if self.process_offsets_by_inclusion {

            let tips = session.get_tips();

            for hash in tips {
                let transactions = self.store.get_block_transactions(hash)?
                if transactions.is_none() {
                    return Ok(false)
                } 
                for transaction in transactions.unwrap().into_iter() {
                    if self.store.get_transaction_offset(transaction.id())?.is_none() {
                        trace!("[{0}] sync status is {1}", IDENT, false);
                        return Ok(false)
                    } 
                }
            }
        }
        
        if self.process_offsets_by_acceptance || self.process_acceptance {

            let sink = session.get_sink();
            let sink_acceptance_data = session.get_block_acceptance_data(sink)?;

            let tx_id_iter = sink_acceptance_data
            .into_iter()
            .flat_map(|mergeset| {
                mergeset.accepted_transactions
                .into_iter()
                .map(|tx_id_index_pair| (tx_id_index_pair.transaction_id))
            });

            for tx_id in tx_id_iter {
                if self.process_offsets_by_acceptance {
                    if self.store.get_transaction_offset(tx_id)?.is_none() {
                        trace!("[{0}] sync status is {1}", IDENT, false);
                        return Ok(false)
                    }
                }
                if self.process_acceptance {
                    if self.store.get_accepting_hash(block_hash)?.is_none() {
                        trace!("[{0}] sync status is {1}", IDENT, false);
                        return Ok(false)
                    }
                }
            }
        };

        Ok(true)
        }
    }

    fn get_transaction_data_by_id(
        self, 
        tx_id: TransactionId, 
        include_transaction: bool,
        include_tx_offset: bool, 
        include_accepting_hash: bool,
    ) -> TxIndexResult<(Option<Transaction>, Option<TransactionOffset>, Option<Hash>)> {
        if !include_tx_entry && !include_transaction && !include_accepting_hash {
            Ok((None, None, None))
        }
        
        let tx_offset = self.store.get_transaction_offset(tx_id)?;
        
        let transaction = if include_transaction {
            Some(*self.store.get_block_transactions(tx_offset.including_block)?.get(tx_offset.transaction_index as usize)?)
        } else {
            None
        };
        
        let accepting_hash = if include_accepting_hash {
            Some(self.store.get_accepting_hash(tx_offset.including_block)?)
        } else {
            None
        };

        let tx_offset = if include_tx_offset { Some(tx_offset) } else { None };

        Ok((transaction, tx_offset, accepting_hash))
    }

    /// This is a convenience method which combines `get_transaction_offset` with `get_transaction_by_offset`. 
    fn get_transaction_by_id(
        self,
        transaction_id: Vec<TransactionId>,
    ) -> TxIndexResult<Vec<Option<Transaction>>> {
        let offset = self.store.get_transaction_offset(transaction_id)?;
        if offset.is_none() {
            Ok(None)
        }
        let offset = offset.unwrap();

        let block_transactions = self.store.get_block_transactions(offset.including_block)?;
        if block_transactions.is_none() {
            Ok(None)
        }

        Ok(block_transactions.as_deref().unwrap().get(offset.transaction_index))
    }

    fn get_transaction_offset(
        self, 
        transaction_id: Vec<TransactionId>, 
    ) -> TxIndexResult<Option<TransactionOffset>> {
        Ok(self.store.get_transaction_offset(transaction_id)?)
    }

    fn get_transaction_acceptance_datum(
        self, 
        transaction_id: TransactionId, 
    ) -> TxIndexResult<Option<TransactionAcceptance>> {
        Ok(self.store.get_accepting_hash(transaction_id)?)
    }

    fn get_transaction_by_offset(
        self, 
        transaction_offset: TransactionOffset,
    ) -> TxIndexResult<Option<Transaction>> {
        Ok(self.store
        .get_block_transactions(transaction_offset.including_block)?
        .get(transaction_offset.transaction_index))
    }

    /// This is an optimized call for multi-transaction queries, as it indexes by including block hashes. 
    /// if maintaining query order is important consider iterating over `get_transaction_by_offset`.
    /// transactions not found in the db, will be vacant from the result. 
    fn get_transactions_by_offsets(
        self, 
        transaction_offsets: TransactionOffsets,
    ) -> TxIndexResult<Vec<Transaction>> {
        
        Ok(transaction_offsets
        .into_iter()
        .map(move |(hash, indices)| {
            self.store.get_block_transactions(hash)?
            .into_iter()
            .map(move |transactions| {
                indices
                .into_iter()
                .map( move |index| {
                    *transactions.get(index as usize).unwrap()
                })
            })      
        }).collect())
    }

    // This is an optimized call for multi-transaction queries, as it indexes by including block hashes. 
    // if maintaining query order is important consider iterating over `get_transaction_by_offset`.
    // transactions not found in the db, will be vacant from the result. 
    fn get_transactions_by_ids(
        self, 
        transaction_ids: Vec<TransactionId>,
    ) -> TxIndexResult<Vec<Transaction>> {
        
        let transaction_offsets: BlockHashMap<Vec<TransactionId>> = BlockHashMap::new();

        transaction_ids
        .into_iter()
        .map(move |transactionId| {
            let offset = self.store.get_transaction_offset(transaction_id)?;
            if offset.is_some() {
                let offset = offset.unwrap();
                match transaction_offsets.entry(offset.including_block) {
                    Entry::Occupied(entry) => {
                        entry.get_mut().append(offset.transaction_index)
                    },
                    Entry::Vacant(entry) => {
                        entry.insert(offset.transaction_index)
                    }
                };
            }
        });
        
        Ok(transaction_offsets
        .into_iter()
        .map(move |(hash, indices)| {
            self.store.get_block_transactions(hash)?
            .into_iter()
            .map(move |transactions| {
                indices
                .into_iter()
                .map( move |index| {
                    *transactions.get(index as usize).unwrap()
                })
            })      
        }).collect())
    }

    fn update_inclusion(&mut self, block: Block) -> TxIndexResult<()> {
        trace!("[{0}] adding {1} txs from including block {2}", assignment, block.transactions.len(), block.hash());
        if self.process_offsets_by_inclusion {
            self.store.add_transaction_offsets(block.hash(), block.transactions, false)?;
        }

        Ok(())
    }

    fn update_acceptance(&mut self, added_acceptance: BlockHashMap<Arc<Vec<MergesetBlockAcceptanceData>>>, removed_acceptance: BlockHashMap<Arc<Vec<MergesetBlockAcceptanceData>>>) -> TxIndexResult<()> {
        trace!(("[{0}] adding txs from accepting block {2}", IDENT, block.hash()));
        
        if self.process_acceptance {
            self.store.update_accepting_block_hashes(added_acceptance, removed_acceptance, false)?;
        }
        
        if self.process_acceptance && !self.process_offsets_by_inclusion {
            self.store.update_transaction_offsets_via_acceptance_data(added_acceptance, removed_acceptance, false)?;
        };

        Ok(())
    }

    fn resync(&mut self) -> TxIndexResult<()> {

        if self.process_offsets_by_acceptance && self.process_offsets_by_inclusion {
            return TxIndexError::ConflictingPopulationMechanics;
        }

        self.store.delete_all()?;

        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session());

        
        resync_check_size = min(self.config.mergeset_size_limit, MAX_RESYNC_CHUNK_SIZE as u64);
        sink = session.get_sink();
        // TODO (when pruning is implemented): Sync from earliest header pruning point.
        low_hash = session.get_start_pruning_point()?;
        high_hash = session.get_sink();

        if self.process_offsets_by_inclusion {
            let resync_segment = move |start_hash: Hash| {
                let (including_hashes, high_hash) = session.get_hashes_between(start_hash, sink, resync_check_size)?;
                for including_hash in including_hashes {
                    let transactions = self.store.get_block_transactions(hash)?;
                    self.store.add_transaction_offsets(including_hash, transactions, true)?
                }
                high_hash
            };

            high_hash = resync_segment(low_hash);

            while high_hash != sink {
                high_hash = resync_segment(high_hash);
            }
        }

        if self.process_offsets_by_acceptance || self.process_acceptance {
            let virtual_chain = session.get_virtual_chain_from_block(low_hash)?;
            for chain_hash in virtual_chain.added {
                let mergeset = session.get_block_acceptance_data(chain_hash)?;
                for block_acceptance in mergeset {
                    if self.process_offsets_by_acceptance {
                        self.store.add_accepting_block_hashes((chain_hash, block_acceptance).into(), true)?;
                    }
                    if self.process_offsets_by_acceptance {
                        self.store.add_transaction_offsets_via_acceptance_data((chain_hash, block_acceptance).into(), true)?;
                    }
                }
            }
        };
    
    Ok(())
    
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
