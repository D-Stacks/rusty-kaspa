use std::{sync::Arc, mem};

use kaspa_consensus::{model::stores::{block_transactions::{
    BlockTransactionsStore, BlockTransactionsStoreReader, DbBlockTransactionsStore,
}, headers::{DbHeadersStore, HeaderStoreReader, HeaderStore, CompactHeaderData}, acceptance_data::{DbAcceptanceDataStore, AcceptanceDataStore, AcceptanceDataStoreReader}}, consensus::storage::ConsensusStorage};
use kaspa_consensus_core::{
    acceptance_data::AcceptanceData,
    tx::{Transaction, TransactionId},
    BlockHashSet, config::Config as ConsensusConfig, header::Header,
};
use kaspa_core::{trace, warn};
use kaspa_database::prelude::{StoreError, StoreResult, DB};
use kaspa_hashes::Hash;

use crate::{
    errors::TxIndexResult,
    model::{TxOffset, BlockAcceptanceOffset},
    stores::{
        tx_offsets::{DbTxIndexAcceptedTxOffsetsStore, TxIndexAcceptedTxOffsetsReader, TxIndexAcceptedTxOffsetsStore},
        merged_block_acceptance::{
            DbTxIndexMergedBlockAcceptanceStore, TxIndexMergedBlockAcceptanceReader, TxIndexMergedBlockAcceptanceStore,
        },
        sink::{DbTxIndexSinkStore, TxIndexSinkReader, TxIndexSinkStore},
        source::{DbTxIndexSourceStore, TxIndexSourceReader, TxIndexSourceStore},
        tips::{DbTxIndexTipsStore, TxIndexTipsReader, TxIndexTipsStore},
        unaccepted_tx_offsets::{
            DbTxIndexUnacceptedTxOffsetsStore, TxIndexUnacceptedTxOffsetsReader, TxIndexUnacceptedTxOffsetsStore,
        },
    },
    IDENT,
};

struct TxIndexConsensusStores {
    block_transaction_store: DbBlockTransactionsStore, // required when processing transaction_offsets
    block_header_store: DbHeadersStore,
    acceptance_store: DbAcceptanceDataStore,
}

impl TxIndexConsensusStores {
    fn new(consensus_db: Arc<DB>) -> TxIndexConsensusStores {
        Self { 
            block_transaction_store: DbBlockTransactionsStore::new(consensus_db, 0), 
            block_header_store: DbHeadersStore::new(consensus_db, 0), 
            acceptance_store: DbAcceptanceDataStore::new(consensus_db, 0) 
        }
    }
}
struct TxIndexNativeStores {
    accepted_tx_offsets: DbTxIndexAcceptedTxOffsetsStore,
    unaccepted_tx_offsets: DbTxIndexUnacceptedTxOffsetsStore,
    merged_block_acceptance: DbTxIndexMergedBlockAcceptanceStore,
    source: DbTxIndexSourceStore,
    sink: DbTxIndexSinkStore,
    tips: DbTxIndexTipsStore,
}
pub struct TxIndexStore {
    consensus_stores: TxIndexConsensusStores,
    txindex_stores: TxIndexNativeStores,
}

impl TxIndexStore {
    pub fn new(txindex_db: Arc<DB>, consensus_db: Arc<DB>, consensus_config: ConsensusConfig) -> Self {
        
        // consensus block data cache size normalized to Txoffset + TransactionId sizes. 
        let offset_cache_size = consensus_config.perf.block_data_cache_size * (((consensus_config.max_block_mass as f64 / consensus_config.mass_per_tx_byte as f64) / (mem::size_of::<TxOffset>() + mem::size_of::<TransactionId>()) as f64).floor() as u64);
        // consensus block header cache size normalized to BlockAcceptanceOffset + Hash sizes. 
        let block_acceptance_cache_size = consensus_config.perf.headers_cache_size * (mem::size_of::<Header>() as f64 / (mem::size_of::<BlockAcceptanceOffset> + mem::size_of::<Hash>()) as f64).floor() as u64;
        Self {
            consensus_stores: TxIndexConsensusStores::new(consensus_db),
            txindex_stores: TxIndexStores {
                accepted_tx_offsets: DbTxIndexAcceptedTxOffsetsStore::new(txindex_db, offset_cache_size),
                unaccepted_tx_offsets: DbTxIndexUnacceptedTxOffsetsStore::new(txindex_db, offset_cache_size), 
                merged_block_acceptance: DbTxIndexMergedBlockAcceptanceStore::new(txindex_db, block_acceptance_cache_size),
                source: DbTxIndexSourceStore::new(txindex_db),
                sink: DbTxIndexSinkStore::new(txindex_db),
                tips: DbTxIndexTipsStore::new(txindex_db),
            },
        }
    }

    pub fn add_block_transactions(&self, block_hash: Hash, txs: Arc<Vec<Transaction>>) -> TxIndexResult<()> {
        trace!("[{0}] retrieving transactions from block: {1}", IDENT, block_hash);
        Ok(self.consensus_stores.block_transaction_store.insert(block_hash, txs)?)
    }

    pub fn has_block_transactions(&self, block_hash: Hash) -> TxIndexResult<bool> {
        trace!("[{0}] retrieving transactions from block: {1}", IDENT, block_hash);
        Ok(self.consensus_stores.block_transaction_store.has(block_hash)?)
    }

    pub fn get_block_transactions(&self, block_hash: Hash) -> TxIndexResult<Option<Arc<Vec<Transaction>>>> {
        trace!("[{0}] retrieving transactions from block: {1}", IDENT, block_hash);
        match self.consensus_stores.block_transaction_store.get(block_hash) {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn add_unaccepted_transaction_offsets(&mut self, tx_offsets_by_id: Vec<(TransactionId, TxOffset)>) -> TxIndexResult<()> {
        trace!("[{0}] adding {1} unaccepted transaction offsets", IDENT, tx_offsets_by_id.len());

        Ok(self.txindex_stores.unaccepted_tx_offsets.insert_many(tx_offsets_by_id)?)
    }

    pub fn remove_unaccepted_transaction_offsets(&mut self, transaction_ids: Arc<Vec<TransactionId>>) -> TxIndexResult<()> {
        trace!("[{0}] removing {1} unaccepted transaction offsets", IDENT, transaction_ids.len());

        Ok(self.txindex_stores.unaccepted_tx_offsets.remove_many(transaction_ids)?)
    }

    pub fn has_unaccepted_transaction_offset(&self, tx_id: TransactionId) -> TxIndexResult<bool> {
        trace!("[{0}] checking if {1} is in the unaccepted transaction offset store", IDENT, tx_id);

        Ok(self.txindex_stores.unaccepted_tx_offsets.has(tx_id)?)
    }

    pub fn get_unaccepted_transaction_offset(self, transaction_id: TransactionId) -> TxIndexResult<Option<TxOffset>> {
        trace!("[{0}] retrieving unaccepted transaction offset for txID: {1}", IDENT, transaction_id);

        match self.txindex_stores.unaccepted_tx_offsets.get(transaction_id) {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn add_merged_blocks(
        &mut self,
        accepting_block_hash: Hash,
        block_hashes: Vec<Hash>,
    ) -> TxIndexResult<()> {
        trace!("[{0}] adding {1} merged block acceptance data", IDENT, block_hashes.len());

        Ok(self.txindex_stores.merged_block_acceptance.insert_many(accepting_block_hash, block_hashes)?)
    }

    pub fn remove_unmerged_blocks(&mut self, block_hashes: Vec<Hash>) -> TxIndexResult<()> {
        trace!("[{0}] removing {1} merged block acceptance data", IDENT, block_hashes.len());

        Ok(self.txindex_stores.merged_block_acceptance.remove_many(block_hashes)?)
    }

    pub fn has_merged_block_acceptance(&mut self, block_hash: Hash) -> TxIndexResult<bool> {
        trace!("[{0}] checking if {1} is in the merged block acceptance store", IDENT, block_hash);

        Ok(self.txindex_stores.merged_block_acceptance.has(block_hash)?)
    }

    pub fn get_accepting_block_hash(self, block_hash: Hash) -> TxIndexResult<Option<Hash>> {
        trace!("[{0}] retrieving acceptance data for block: {1}", IDENT, block_hash);

        match self.txindex_stores.merged_block_acceptance.get(block_hash) {
            Ok(mut item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn add_accepted_transaction_offsets(&mut self, tx_offsets_by_id: Arc<TxOffsetById>) -> TxIndexResult<()> {
        trace!("[{0}] adding {1} accepted transaction offsets", IDENT, tx_offsets_by_id.len());

        Ok(self.txindex_stores.accepted_tx_offsets.insert_many(tx_offsets_by_id)?)
    }

    pub fn remove_accepted_transaction_offsets(&mut self, tx_ids: Arc<Vec<TransactionId>>) -> TxIndexResult<()> {
        trace!("[{0}] removing {1} accepted transaction offsets", IDENT, tx_ids.len());

        Ok(self.txindex_stores.accepted_tx_offsets.remove_many(tx_ids)?)
    }

    pub fn has_accepted_transaction_offset(&self, tx_id: TransactionId) -> TxIndexResult<bool> {
        trace!("[{0}] checking if {1} is in the accepted transaction offsets store", IDENT, tx_id);

        Ok(self.txindex_stores.accepted_tx_offsets.has(tx_id)?)
    }

    pub fn get_accepted_transaction_offsets(self, tx_id: TransactionId) -> TxIndexResult<Option<TxOffset>> {
        trace!("[{0}] retrieving accepted transaction entry for txID: {1},", IDENT, tx_id);

        match self.txindex_stores.accepted_tx_offsets.get(tx_id) {
            Ok(mut item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn set_source(&mut self, source: Hash) -> TxIndexResult<()> {
        trace!("[{0}] setting source: {1}", IDENT, source);
        Ok(self.txindex_stores.source.set(source)?)
    }

    pub fn get_source(self) -> TxIndexResult<Option<Hash>> {
        trace!("[{0}] retrieving source", IDENT);

        match self.txindex_stores.source.get() {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn set_sink(&mut self, sink: Hash) -> TxIndexResult<()> {
        trace!("[{0}] setting sink: {1}", IDENT, sink);
        Ok(self.txindex_stores.sink.set(sink)?)
    }

    pub fn get_sink(self) -> TxIndexResult<Option<Hash>> {
        trace!("[{0}] retrieving sink", IDENT);

        match self.txindex_stores.sink.get() {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
    }

    pub fn add_tip(&mut self, tip_hash: Hash) -> TxIndexResult<()> {
        trace!("[{0}] adding tip: {1}", IDENT, tip);

        Ok(self.txindex_stores.tips.update_add_tip(tip_hash)?)
    }

    pub fn remove_tips(&mut self, tip_hashes: BlockHashSet) -> TxIndexResult<()> {
        trace!("[{0}] removing potential tips: {1:?}", IDENT, tip_hashes);

        Ok(self.txindex_stores.tips.update_remove_tips(tip_hashes)?)
    }

    pub fn get_tips(&mut self) -> TxIndexResult<Option<BlockHashSet>> {
        trace!("[{0}] retrieving tips", IDENT);

        match self.txindex_stores.tips.get() {
            Ok(item) => Ok(Some(item)),
            Err(err) => match err {
                StoreError::KeyNotFound(_) => Ok(None),
                default => Err(err)?,
            },
        }
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
