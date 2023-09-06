use std::sync::Arc;

use kaspa_consensus::model::stores::{
    block_transactions::{
        BlockTransactionsStoreReader, 
        DbBlockTransactionsStore,        
    },
    headers::{
        HeaderStore,
        DbHeadersStore,
    } 
};
use kaspa_consensus_core::{
    tx::{Transaction, TransactionId, TransactionIndexType},
    acceptance_data::AcceptanceData, BlockHashSet,
};
use kaspa_core::{trace, warn};
use kaspa_database::prelude::{StoreResult, DB, StoreError};
use kaspa_database::registry::DatabaseStorePrefixes;
use kaspa_hashes::Hash;

use crate::{
    model::transaction_entries::{TransactionEntry, TransactionEntriesById, TransactionOffset, TransactionAcceptanceData},
    stores::{
        sink::{DbTxIndexSinkStore, TxIndexSinkStore, TxIndexSinkStoreReader},
        source::{DbTxIndexSourceStore, TxIndexSourceStore, TxIndexSourceStoreReader},
        tx_offsets::{DbTransactionEntriesStore, TransactionEntriesStore, TransactionEntriesStoreReader},
        tips::{DbTxIndexTipsStore, TxIndexTipsReader, TxIndexTipsStore },
    },
    params::TxIndexParams,
    errors::{TxIndexError, TxIndexResult}, 
    IDENT
};

struct ConsensusStores {
    block_transaction_store: DbBlockTransactionsStore, // required when processing transaction_offsets
    block_header_store: DbHeadersStore, // required to query block header data related to the transaction
}

struct TxIndexStores {
    transaction_entries: DbTransactionEntriesStore,
    source: DbTxIndexSourceStore,
    sink: DbTxIndexSinkStore, //required when processing accepting block hashes
    tips: DbTxIndexTipsStore,
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
                block_header_store: DbHeadersStore::new(consensus_db, 0) //TODO: cache_size from params
            },
            txindex_stores: TxIndexStores { 
                transaction_entries: DbTransactionEntriesStore::new(txindex_db, 0),
                sink: DbTxIndexSinkStore::new(txindex_db),
                source: DbTxIndexSourceStore::new(txindex_db),
                tips: DbTxIndexTipsStore::new(txindex_db),
            }
        }
    }

    pub fn get_block_transactions(&self, hash: Hash) -> TxIndexResult<Option<Arc<Vec<Transaction>>>> {
        match self.consensus_stores.block_transaction_store
        .expect("expected txindex's block transaction store to be intialized")
        .get(hash) {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?
            },
        }
    }

    pub fn has_block_transactions(&self, hash: Hash) -> TxIndexResult<bool> {
        match self.consensus_stores.block_transaction_store
        .expect("expected txindex's block transaction store to be intialized")
        .has(hash) {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?
            },
        }
    }

    pub fn get_transaction_entry(self, transaction_id: TransactionId) -> TxIndexResult<Option<TransactionEntry>> {        
        trace!("[{0}] retrieving transaction entry for {1},", IDENT, transaction_id);     
        match self.txindex_stores.transaction_entries.get(transaction_id) {
            Ok(mut item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
            }
        }

    pub fn get_transaction_offset(self, transaction_id: TransactionId) -> TxIndexResult<Option<TransactionOffset>> {        
        trace!("[{0}] retrieving transaction entry for {1}, with inclustion data {2} and acceptance data {3}");     
        match self.txindex_stores.transaction_entries.get(transaction_id)
        {
            Ok(item) => Ok(Some(item.offset)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
            }
        }

    pub fn get_transaction_acceptance_data(self, transaction_id: TransactionId) -> TxIndexResult<Option<TransactionAcceptanceData>> {        
        trace!("[{0}] retrieving transaction acceptance data {1}", transaction_id);     
        match self.txindex_stores.transaction_entries.get(transaction_id)
        {
            Ok(mut item) => Ok(item.acceptance_data),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
            }
        }
    
    pub fn has_transaction_entry(self, transaction_id: TransactionId) -> TxIndexResult<Option<TransactionEntry>> {   
        trace!("[{0}] checking if db has transaction entry {1}", IDENT, transaction_id);     
        match self.txindex_stores.transaction_entries.has(transaction_id)
        {
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
        trace!("[{0}] retrieving source", IDENT);

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

        match self.txindex_stores.sink.get() {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn get_tips(
        self,
    ) -> TxIndexResult<Option<BlockHashSet<Hash>>> {
        trace!("[{0}] retrieving tips", IDENT);

        match self.txindex_stores.tips.get() {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn add_tip(
        self,
        tip: Hash
    ) -> TxIndexResult<Hash> {
        trace!("[{0}] adding tip {1}", IDENT, tip);

        match self.txindex_stores.tips {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn remove_tips(
        self,
        tips: BlockHashSet<Hash>
    ) -> TxIndexResult<Hash> {
        trace!("[{0}] filtering tips {1}", IDENT, tips);
        match self.txindex_stores.tips {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }


    pub fn remove_transaction_entries(
        &mut self,
        transaction_ids: Vec<TransactionId>,
    ) -> TxIndexResult<()> {
        trace!("[{0}] removing {1} transaction entires: {1}", IDENT, transaction_ids.len());
        self.txindex_stores.transaction_entries.remove_many(transaction_ids)
    }

    pub fn add_transaction_entries(
        &mut self,
        transaction_entries_by_id: TransactionEntriesById,
        overwrite: bool,
    ) -> TxIndexResult<()> {
        trace!("[{0}] adding {1} transaction entires: {1}", IDENT, transaction_entries_by_id.len());

        let res = self.txindex_stores.transaction_entries.insert_many(
            transaction_entries_by_id,
            overwrite
        );
        res
    }

    pub fn set_source(
        &mut self,
        source: Hash
    ) -> TxIndexResult<()> {
        trace!("[{0}] setting source: {1}", IDENT, source);
        self.txindex_stores.source.set(source)
    }

    pub fn set_sink(
        &mut self,
        sink: Hash
    ) -> TxIndexResult<()> {
        trace!("[{0}] setting sink: {1}", IDENT, sink);   
        self.txindex_stores.sink.set(sink)
    }

    /// Resets the txindex database:
    pub fn delete_all(&mut self) -> TxIndexResult<()> {
        // TODO: explore possibility of deleting and replacing whole db, currently there is an issue because of file lock and db being in an arc.
        trace!("[{0}] attempting to clear txindex database...", IDENT);

        // Clear all
        trace!("[{0}] clearing source database...", IDENT);
        self.txindex_stores.source.remove()?;
        trace!("[{0}] clearing tips database...", IDENT);
        self.txindex_stores.tips.remove()?;
        trace!("[{0}] clearing sink database...", IDENT);
        self.txindex_stores.sink.remove()?;
        trace!("[{0}] clearing transaction_entries database...", IDENT);
        self.txindex_stores.transaction_entries.delete_all()?;

        trace!("[{0}] clearing txindex database - success!", IDENT);

        Ok(())
    }
}
