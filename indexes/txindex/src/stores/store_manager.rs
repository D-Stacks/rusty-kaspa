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
use kaspa_hashes::Hash;

use crate::{
    model::{transaction_entries::{TransactionEntry, TransactionEntriesById, TransactionOffset, TransactionAcceptanceData}, TxOffset, TxOffsetById},
    stores::{
        sink::{DbTxIndexSinkStore, TxIndexSinkStore, TxIndexSinkReader},
        source::{DbTxIndexSourceStore, TxIndexSourceStore, TxIndexSourceReader},
        accepted_tx_offsets::{DbTxIndexAcceptedTxOffsetsStore, TxIndexAcceptedTxOffsetsStore, TxIndexAcceptedTxOffsetsReader},
        unaccepted_tx_offsets::{DbTxIndexUnacceptedTxOffsetsStore, TxIndexUnacceptedTxOffsetsStore, TxIndexUnacceptedTxOffsetsReader},
        tips::{DbTxIndexTipsStore, TxIndexTipsReader, TxIndexTipsStore},
        merged_block_acceptance::{DbTxIndexMergedBlockAcceptanceStore, TxIndexMergedBlockAcceptanceStore, TxIndexMergedBlockAcceptanceReader},
    },
    errors::{TxIndexError, TxIndexResult}, 
    IDENT
};

struct ConsensusStores {
    block_transaction_store: DbBlockTransactionsStore, // required when processing transaction_offsets
}

struct TxIndexStores {
    accepted_tx_offsets: DbTxIndexAcceptedTxOffsetsStore,
    unaccepted_tx_offsets: DbTxIndexUnacceptedTxOffsetsStore,
    merged_block_acceptance: DbTxIndexMergedBlockAcceptanceEntriesStore,
    source: DbTxIndexSourceStore,
    sink: DbTxIndexSinkStore,
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
                block_transaction_store: DbBlockTransactionsStore::new(consensus_db, 0), //TODO: cache_size
            },
            txindex_stores: TxIndexStores { 
                accepted_tx_offsets: DbTxIndexAcceptedTxOffsetsStore::new(txindex_db, 0), //TODO: cache_size
                unaccepted_tx_offsets: DbTxIndexUnacceptedTxOffsetsStore::new(txindex_db, 0), //TODO: cache_size
                merged_block_acceptance: DbTxIndexMergedBlockAcceptanceStore::new(txindex_db, 0), //TODO: cache_size
                source: DbTxIndexSourceStore::new(txindex_db),
                sink: DbTxIndexSinkStore::new(txindex_db),
                tips: DbTxIndexTipsStore::new(txindex_db),
            }
        }
    }

    pub fn get_block_transactions(&self, block_hash: Hash) -> TxIndexResult<Option<Arc<Vec<Transaction>>>> {
        trace!("[{0}] retrieving transactions from block: {1}", IDENT, including_block);     
        match self.consensus_stores.block_transaction_store
        .get(block_hash) {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?
            },
        }
    }

    pub fn get_accepted_transaction_offsets(self, transaction_id: TransactionId) -> TxIndexResult<Option<TxOffset>> {        
        trace!("[{0}] retrieving accepted transaction entry for txID: {1},", IDENT, transaction_id);     
        
        match self.txindex_stores.accepted_tx_offsets.get(transaction_id) {
            Ok(mut item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
            }
        }

    pub fn get_unaccepted_transaction_offset(self, transaction_id: TransactionId) -> TxIndexResult<Option<TxOffset>> {        
        trace!("[{0}] retrieving unaccepted transaction offset for txID: {1}", IDENT, transaction_id);     
        
        match self.txindex_stores.unaccepted_tx_offsets.get(transaction_id)
        {
            Ok(item) => Ok(Some(item.offset)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
            }
        }

    pub fn get_merge_acceptance_data(self, including_block: Hash) -> TxIndexResult<Option<TransactionAcceptanceData>> {        
        trace!("[{0}] retrieving acceptance data for block: {1}", IDENT, including_block);     
        
        match self.txindex_stores.merged_block_acceptance.get(including_block)
        {
            Ok(mut item) => Ok(item),
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
    ) -> TxIndexResult<Option<BlockHashSet>> {
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
        tip_hash: Hash
    ) -> TxIndexResult<()> {
        trace!("[{0}] adding tip: {1}", IDENT, tip);

        self.txindex_stores.tips.update_add_tip(tip_hash)
    }

    pub fn remove_tips(
        self,
        tip_hashes: BlockHashSet
    ) -> TxIndexResult<Hash> {
        trace!("[{0}] removing potential tips: {1}", IDENT, tip_hashes);
        
        self.txindex_stores.tips.update_remove_tips(tip_hashes)
    }


    pub fn remove_accepted_transaction_offsets(
        &mut self,
        transaction_ids: Vec<TransactionId>,
    ) -> TxIndexResult<()> {
        trace!("[{0}] removing {1} accepted transaction offsets", IDENT, transaction_ids.len());

        self.txindex_stores.accepted_tx_offsets.remove_many(transaction_ids)
    }

    pub fn add_accepted_transaction_offsets(
        &mut self,
        tx_offsets_by_id: TxOffsetById,
    ) -> TxIndexResult<()> {
        trace!("[{0}] adding {1} accepted transaction offsets", IDENT, transaction_entries_by_id.len());

        self.txindex_stores.accepted_tx_offsets.insert_many(tx_offsets_by_id)
    }

    pub fn remove_unaccepted_transaction_offsets(
        &mut self,
        transaction_ids: Vec<TransactionId>,
    ) -> TxIndexResult<()> {
        trace!("[{0}] removing {1} unaccepted transaction offsets", IDENT, transaction_ids.len());

        self.txindex_stores.unaccepted_tx_offsets.remove_many(transaction_ids)
    }

    pub fn add_unaccepted_transaction_offsets(
        &mut self,
        tx_offsets_by_id: TxOffsetById,
    ) -> TxIndexResult<()> {
        trace!("[{0}] adding {1} unaccepted transaction offsets", IDENT, transaction_entries_by_id.len());

        self.txindex_stores.unaccepted_tx_offsets.insert_many(tx_offsets_by_id)
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
        self.txindex_stores.accepted_tx_offsets.delete_all()?;
        trace!("[{0}] clearing accepted transaction offset database...", IDENT);
        self.txindex_stores.accepted_tx_offsets.delete_all()?;
        trace!("[{0}] clearing unaccepted transaction offset database...", IDENT);
        self.txindex_stores.unaccepted_tx_offsets.delete_all()?;
        trace!("[{0}] clearing merged block acceptance database...", IDENT);
        self.txindex_stores.merged_block_acceptance.delete_all()?;

        trace!("[{0}] clearing txindex database - success!", IDENT);

        Ok(())
    }
}
