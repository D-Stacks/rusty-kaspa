use std::sync::Arc;

use kaspa_consensus::model::stores::{
    block_transactions::{
        BlockTransactionsStoreReader, 
        DbBlockTransactionsStore
    }, pruning::DbPruningStore, 
};
use kaspa_consensus_core::{
    tx::{Transaction, TransactionId, TransactionReference, TransactionIndexType},
    acceptance_data::BlockAcceptanceData, block::Block,
};
use kaspa_core::trace;
use kaspa_database::prelude::{StoreResult, DB, StoreError};
use kaspa_hashes::Hash;

use crate::{
    model::{
        transaction_offsets::TransactionOffset,
        params::TxIndexParams, merge_acceptance::{TransactionAcceptance, MergeAcceptance},
    }, 
    stores::{
        params::{TxIndexParamsStoreReader, DbTxIndexParamsStore, TxIndexParamsStore},
        merge_acceptance::{DbTxAcceptanceStore, TxAcceptanceStoreReader, TxAcceptanceStore},
        accepted_transaction_offsets::{DbAcceptedTxOffsetStore, MergeAcceptanceStore, MergeAcceptanceStoreReader},
        included_transaction_offsets::{DbIncludedTxOffsetStore, IncludedTxOffsetStore, IncludedTxOffsetStoreReader},
        last_block_added::{DbTxIndexLastBlockAddedStore, TxIndexLastBlockAddedStore, TxIndexLastBlockAddedStoreReader},
        sink::{DbTxIndexSinkStore, TxIndexSinkStore, TxIndexSinkStoreReader},
        pruning_point::{DbTxIndexPruningStore, TxIndexPruningStore, TxIndexPruningStoreReader},
    }, IDENT
};

use super::{merge_acceptance::{DbMergeAcceptanceStore, MergeAcceptanceStoreReader, MergeAcceptanceStore}, pruning_point};

struct ConsensusStores {
    block_transaction_store: DbBlockTransactionsStore
}

struct TxIndexStores {
    params: DbTxIndexParamsStore,
    pruning_point: DbTxIndexPruningStore,
    accepted_offsets: Option<DbAcceptedTxOffsetStore>,
    included_offsets: Option<DbIncludedTxOffsetStore>,
    merge_acceptance: Option<DbMergeAcceptanceStore>,
    sink: Option<DbTxIndexSinkStore>,
    last_block_added: Option<DbTxIndexLastBlockAddedStore>,
}

#[derive(Clone)]
pub struct TxIndexStore {
    consensus_stores: ConsensusStores,
    txindex_stores: TxIndexStores,
}

impl TxIndexStore {
    pub fn new(txindex_db: Arc<DB>, consensus_db: Arc<DB>, txindex_params: TxIndexParams) -> Self {
        Self { 
            consensus_stores: ConsensusStores { 
                block_transaction_store: DbBlockTransactionsStore::new(consensus_db, 0) //TODO: cache_size from params
            },
            txindex_stores: TxIndexStores { 
                params: DbTxIndexParamsStore::new(txindex_db), 
                pruning_point: DbPruningStore::new(txindex_db), 
                accepted_offsets: if txindex_params.process_offsets_by_acceptance {
                    Some(DbAcceptedTxOffsetStore::new(txindex_db, 0)) //TODO: cache_size from params
                } else { None },
                included_offsets: if txindex_params.process_offsets_by_inclusion {
                    Some(DbAcceptedTxOffsetStore::new(txindex_db, 0)) //TODO: cache_size from params
                } else { None },
                merge_acceptance: if txindex_params.process_acceptance {
                    Some(DbAcceptedTxOffsetStore::new(txindex_db, 0)) //TODO: cache_size from params
                } else { None }, 
                sink: if txindex_params.process_acceptance {
                    Some(DbAcceptedTxOffsetStore::new(txindex_db, 0)) //TODO: cache_size from params
                } else { None },
                last_block_added: if txindex_params.process_offsets_by_inclusion {
                    Some(DbAcceptedTxOffsetStore::new(txindex_db, 0)) //TODO: cache_size from params
                } else { None },
            }
        }
    }

    pub fn get_block_transactions(&self, hash: Hash) -> StoreResult<Option<Arc<Vec<Transaction>>>> {
        match self.consensus_stores.block_transaction_store.get(hash) {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?
            },
        }
    }

    pub fn has_block_transactions(&self, hash: Hash) -> StoreResult<bool> {
        match self.consensus_stores.block_transaction_store.has(hash) {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?
            },
        }
    }

    pub fn get_accepted_transaction_offset(&self, tx_id: TransactionId) -> StoreResult<Option<TransactionOffset>> {
        let accepted_offsets_store = self.txindex_stores.accepted_offsets.unwrap_or(
            return Ok(None)
        );
        
        match accepted_offsets_store.get(tx_id) {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
            }
        }
    
    pub fn get_included_transaction_offset(&self, tx_id: TransactionId) -> StoreResult<Option<TransactionOffset>> {
        let included_offsets_store = self.txindex_stores.included_offsets.unwrap_or(
            return Ok(None)
        );
        
        match included_offsets_store.get(tx_id) {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
            }
        }

    pub fn get_block_merge_acceptance(&self, hash: Hash)-> StoreResult<Option<MergeAcceptance>> {
        let merge_acceptance_store = self.txindex_stores.merge_acceptance.unwrap_or(
            return Ok(None)
        );
        match merge_acceptance_store.get(hash) {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn get_params(
        self,
    ) -> StoreResult<Option<TxIndexParams>> {
        match self.txindex_stores.params.get() {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn get_pruning_point(
        self,
    ) -> StoreResult<Option<TxIndexParams>> {
        match self.txindex_stores.pruning_point.get() {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn get_sink(
        self,
    ) -> StoreResult<Option<TxIndexParams>> {
        let sink = self.txindex_stores.sink.unwrap_or(
            return Ok(None)
        );
        match self.txindex_stores.sink.get() {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn get_last_block_added(
        self,
    ) -> StoreResult<Option<TxIndexParams>> {
        let sink = self.txindex_stores.last_block_added.unwrap_or(
            return Ok(None)
        );
        match self.txindex_stores.sink.get() {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }


    pub fn remove_accepted_transaction_offsets(
        &mut self,
        transaction_ids: Vec<TransactionId>,
        try_reset_on_err: bool
    ) -> StoreResult<()> {

        let res = self.txindex_transaction_offset_store.remove_transaction_offsets(transaction_ids);
        if try_reset_on_err && res.is_err() {
            self.delete_all()?;
        };
        res
    }

    pub fn add_included_transaction_offsets_from_block(
        &mut self,
        block: Block,
        try_reset_on_err: bool
    ) -> StoreResult<()> {        
        let res = self.txindex_stores.included_offsets
        .expect("expected store")
        .add_transaction_offsets_by_reference(hash, transaction_refs);
        if try_reset_on_err && res.is_err() {
            self.delete_all()?;
        };
        
        res
    }

    pub fn add_transaction_acceptance_data( 
        &mut self,
        block_mergeset: BlockAcceptanceData,
        try_reset_on_err: bool) -> StoreResult<()> {

        
        let res = self.txindex_stores.merge_acceptance
        .expect("expected store")
        .insert_many(
            block_mergeset
            .into_iter()
            .flat_map(|(accepting_hash, mergeset|) {
                (
                    .into_iter()
                    .map(|mergeset| {
                        mergeset.into_iter()
                        MergeAcceptance {
                            mergeset
                        }
                    })
                )
            });
        if try_reset_on_err && res.is_err() {
            self.delete_all()?;
        };
        res
    }

    pub fn update_accepting_block_hashes( 
        &mut self,
        to_add: TransactionOffsets,
        to_remove: TransactionOffsets,
        try_reset_on_err: bool) -> StoreResult<()> {
        
        let accepted_offset_store = self.txindex_stores.accepted_offsets.unwrap_or(
            return 
        )
        let res = self.txindex_acceptance_store.remove_accepting_block_hashes(to_remove);
        if res.is_err() {
            if try_reset_on_err {
                self.delete_all()?;
            }
            return res;
        }

        let res = self.txindex_acceptance_store.add_accepting_block_hashes(to_add);
        if try_reset_on_err && res.is_err() {
            self.delete_all()?;
        };
        res
    }

    pub fn set_params(
        &mut self,
        params: TxIndexParams
    ) -> StoreResult<()> {
        self.txindex_params_store.set(params)
    }

    /// Resets the txindex database:
    pub fn delete_all(&mut self) -> StoreResult<()> {
        // TODO: explore possibility of deleting and replacing whole db, currently there is an issue because of file lock and db being in an arc.
        trace!("[{0}] attempting to clear txindex database...", IDENT);

        // Clear all
        self.txindex_acceptance_store.delete_all()?;
        self.txindex_transaction_offset_store.delete_all()?;
        self.txindex_params_store.remove()?;

        trace!("[{0}] clearing txindex database - success!", IDENT);

        Ok(())
    }
}
