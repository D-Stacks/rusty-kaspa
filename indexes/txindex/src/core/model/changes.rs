use crate::{
    errors::{TxIndexError, TxIndexResult},
    model::{TxAcceptanceData, TxAcceptanceDataByBlockHash, TxCompactEntriesById, TxCompactEntry, TxOffset, TxOffsetById},
};
use kaspa_consensus::processes::ghostdag::mergeset;
use kaspa_consensus_core::{
    acceptance_data::{AcceptanceData, AcceptedTxEntry, MergesetBlockAcceptanceData},
    block::Block,
    tx::{Transaction, TransactionId},
    BlockHashMap, HashMapCustomHasher,
};
use kaspa_hashes::Hash;
use std::sync::Arc;

pub struct TxInclusionChanges {
    pub included_tx_offsets: Arc<TxOffsetById>,
    pub unincluded_tx_ids: Arc<Vec<TransactionId>>,
}

impl TxInclusionChanges {
    pub fn new() -> Self {
        Self { included_tx_offsets: Arc::new(TxOffsetById::new()), unincluded_tx_ids: Arc::new(Vec::<TransactionId>::new()) }
    }
}

pub struct TxAcceptanceChanges {
    pub accepted_tx_offsets: Arc<TxOffsetById>,
    pub unaccepted_tx_ids: Arc<Vec<TransactionId>>,
}

impl TxAcceptanceChanges {
    pub fn new() -> Self {
        Self { accepted_tx_offsets: Arc::new(TxOffsetById::new()), unaccepted_tx_ids: Arc::new(Vec::<TransactionId>::new()) }
    }
}

pub struct BlockAcceptanceChanges {
    pub accepted_chain_block_acceptance_data: Arc<TxAcceptanceDataByBlockHash>,
    pub unaccepted_chain_block_hashes: Arc<Vec<Hash>>,
}

impl BlockAcceptanceChanges {
    pub fn new() -> Self {
        Self {
            accepted_chain_block_acceptance_data: Arc::new(TxAcceptanceDataByBlockHash::new()),
            unaccepted_chain_block_hashes: Arc::new(Vec::<Hash>::new()),
        }
    }
}
