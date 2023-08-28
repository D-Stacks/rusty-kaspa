use std::{sync::Arc, string::FromUtf8Error};

use kaspa_consensus::model::stores::{
    block_transactions::{
        BlockTransactionsStoreReader, 
        DbBlockTransactionsStore,
        STORE_PREFIX as BLOCK_TRANSACTIONS_STORE_PREFIX,
        
    }, pruning::DbPruningStore, acceptance_data::DbAcceptanceDataStore, 
};
use kaspa_consensus_core::{
    tx::{Transaction, TransactionId, TransactionReference, TransactionIndexType},
    acceptance_data::BlockAcceptanceData, block::Block,
};
use kaspa_core::{trace, warn};
use kaspa_database::prelude::{StoreResult, DB, StoreError};
use kaspa_hashes::Hash;

use crate::{
    model::{
        transaction_entries::{TransactionEntry, TransactionEntriesById},
    }, 
    stores::{
        params::{TxIndexParamsStoreReader, DbTxIndexParamsStore, TxIndexParamsStore},
        sink::{DbSinkStore, SinkStore, SinkStoreReader, STORE_PREFIX as SINK_STORE_PREFIX},
        source::{DbTxIndexSourceStore, TxIndexSourceStore, TxIndexSourceStoreReader},
        transaction_entries::{DbTransactionEntriesStore, TransactionEntriesStore, TransactionEntriesStoreReader}
        tips::{DbTxIndexTips, TxIndexTips, },
    },
    params::TxIndexParams,
    errors::{TxIndexError, TxIndexResult}, 
    IDENT
};

use super::{merge_acceptance::{DbMergeAcceptanceStore, MergeAcceptanceStoreReader, MergeAcceptanceStore}, pruning_point, transaction_entries::DbTransactionEntriesStore, last_block_added};

struct ConsensusStores {
    block_transaction_store: DbBlockTransactionsStore, //required when processing transaction_offsets
}

struct TxIndexStores {
    pruning_point: DbTxIndexPruningStore,
    transaction_entries: DbTransactionEntriesStore,
    source: DbTxIndexSourceStore,
    sink: DbSinkStore, //required when processing accepting block hashes
}

#[derive(Clone)]
pub struct TxIndexStore {
    consensus_stores: ConsensusStores,
    txindex_stores: TxIndexStores,
}

impl TxIndexStore {
    pub fn new(txindex_db: Arc<DB>, consensus_db: Arc<DB>) -> Self {
        Self { 
            consensus_stores: ConsensusStores { 
                block_transaction_store: DbBlockTransactionsStore::new(consensus_db, 0), //TODO: cache_size from params
            },
            txindex_stores: TxIndexStores { 
                pruning_point: DbPruningStore::new(txindex_db), 
                transaction_entries: DbTransactionEntriesStore::new(txindex_db, 0),
                sink: DbSinkStore::new(txindex_db),
                source: DbTxIndexSourceStore::new(),
            }
        }
    }

    pub fn get_block_transactions(&self, hash: Hash) -> TxIndexResult<Option<Arc<Vec<Transaction>>>> {
        match self.consensus_stores.block_transaction_store.unwrap_or(
            return Err(
                TxIndexError::DBReadingFromUninitializedStoreError(
                    String::from_utf8(BLOCK_TRANSACTIONS_STORE_PREFIX.to_vec())?
                )
            )
        ).get(hash) {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?
            },
        }
    }

    pub fn has_block_transactions(&self, hash: Hash) -> TxIndexResult<bool> {
        match self.consensus_stores.block_transaction_store.unwrap_or(
            return Err(
                TxIndexError::DBReadingFromUninitializedStoreError(
                    String::from_utf8(BLOCK_TRANSACTIONS_STORE_PREFIX.to_vec())?
                )
            )
        ).has(hash) {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?
            },
        }
    }

    pub fn get_transaction_entry(self, transaction_id: TransactionId) -> TxIndexResult<Option<TransactionEntry>> {        
        match self.txindex_stores.transaction_entries.get(transaction_id)
        {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
            }
        }
    
    pub fn has_transaction_entry(self, transaction_id: TransactionId) -> TxIndexResult<Option<TransactionEntry>> {        
        match self.txindex_stores.transaction_entries.has(transaction_id)
        {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
            }
        }

    pub fn get_params(
        self,
    ) -> TxIndexResult<Option<TxIndexParams>> {
        trace!("[{0}] retrieving params", IDENT);

        match self.txindex_stores.params.get() {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn get_source(
        self,
    ) -> TxIndexResult<Option<Hash>> {
        trace!("[{0}] retrieving pruning point", IDENT);

        match self.txindex_stores.source.get() {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn get_sink(
        self,
    ) -> TxIndexResult<Option<Hash>> {
        trace!("[{0}] retrieving sink", IDENT);

        match self.txindex_stores.sink.unwrap_or(
            return Err(
                TxIndexError::DBReadingFromUninitializedStoreError(
                    String::from_utf8(SINK_STORE_PREFIX.to_vec())?
                )
            )
        ).get() {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn get_last_block_added(
        self,
    ) -> TxIndexResult<Option<Hash>> {
        trace!("[{0}] retrieving last_block_added", IDENT);

        match self.txindex_stores.last_block_added.unwrap_or(
            return Err(
                TxIndexError::DBReadingFromUninitializedStoreError(
                    String::from_utf8(LAST_BLOCK_ADDED_STORE_PREFIX.to_vec())?
                ))
        ).get() {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn remove_transaction_entries(
        &mut self,
        transaction_ids: TransactionIds,
        try_reset_on_err: bool
    ) -> TxIndexResult<()> {
        trace!("[{0}] removing {1} transaction entires: {1}", IDENT, transaction_ids.len());

        let res = self.txindex_stores.transaction_entries.remove_many(transaction_ids);
        if try_reset_on_err && res.is_err() {
            warn!("[{0}] failed to remove transaction entries, attempting to reset the txindex db...");
            self.delete_all()?;
        };
        res
    }

    pub fn add_transaction_entries(
        &mut self,
        transaction_entries_by_id: TransactionEntriesById,
        overwrite: bool,
        try_reset_on_err: bool
    ) -> TxIndexResult<()> {
        trace!("[{0}] removing {1} transaction entires: {1}", IDENT, transaction_ids.len());

        let res = self.txindex_stores.transaction_entries.insert_many(
            transaction_entries_by_id,
            overwrite
        );
        if try_reset_on_err && res.is_err() {
            warn!("[{0}] failed to remove transaction entries, attempting to reset the txindex db...");
            self.delete_all()?;
        };
        res
    }

    pub fn set_params(
        &mut self,
        params: TxIndexParams
    ) -> TxIndexResult<()> {
        trace!("[{0}] setting params: {1}", IDENT, params);
        self.txindex_stores.params.set(params)
    }

    pub fn set_pruning_point(
        &mut self,
        pruning_point: Hash
    ) -> TxIndexResult<()> {
        trace!("[{0}] setting params: {1}", IDENT, params);
        self.txindex_stores.pruning_point.set(pruning_point)
    }

    pub fn set_sink(
        &mut self,
        sink: Hash
    ) -> TxIndexResult<()> {
        trace!("[{0}] setting params: {1}", IDENT, params);
        
        self.txindex_stores.sink.unwrap_or(
            return Err(
                TxIndexError::DBReadingFromUninitializedStoreError(
                    String::from_utf8(SINK_STORE_PREFIX.to_vec())?
                )
            )
        ).set(sink)
    }

    pub fn set_last_block_added(
        &mut self,
        last_block_added: Hash
    ) -> TxIndexResult<()> {
        trace!("[{0}] setting params: {1}", IDENT, params);
        self.txindex_stores.last_block_added.unwrap_or(
            return Err(
                TxIndexError::DBReadingFromUninitializedStoreError(
                    String::from_utf8(SINK_STORE_PREFIX.to_vec())?
                )
            )
        ).set(last_block_added)
    }

    /// Resets the txindex database:
    pub fn delete_all(&mut self) -> TxIndexResult<()> {
        // TODO: explore possibility of deleting and replacing whole db, currently there is an issue because of file lock and db being in an arc.
        trace!("[{0}] attempting to clear txindex database...", IDENT);

        // Clear all
        trace!("[{0}] clearing last_block_added database...", IDENT);
        self.txindex_stores.last_block_added.remove()?;
        trace!("[{0}] clearing sink database...", IDENT);
        self.txindex_stores.sink.remove()?;
        trace!("[{0}] clearing pruning_point database...", IDENT);
        self.txindex_stores.pruning_point.remove()?;
        trace!("[{0}] clearing params database...", IDENT);
        self.txindex_stores.params.remove()?;
        trace!("[{0}] clearing transaction_entries database...", IDENT);
        self.txindex_stores.transaction_entries.delete_all()?;

        trace!("[{0}] clearing txindex database - success!", IDENT);

        Ok(())
    }
}
