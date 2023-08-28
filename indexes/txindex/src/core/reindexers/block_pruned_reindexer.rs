use std::sync::Arc;
use kaspa_consensus_core::{
    block::Block, 
    BlockHashMap, 
    HashMapCustomHasher, 
    acceptance_data::{BlockAcceptanceData, MergesetBlockAcceptanceData, AcceptanceData},
    tx::TransactionId,
};
use kaspa_hashes::Hash;

pub struct TxIndexBlockPrunedReindexer {
    to_remove_transaction_ids: Vec<TransactionId>,
    history_root: Option<Hash>,
}

impl TxIndexBlockPrunedReindexer {
    pub fn new() -> Self {
        Self {
            to_remove_transaction_ids: Vec<TransactionId>,
            history_root: None,
        }
    }

    pub fn get_to_remove_transaction_ids(&self) {
        self.to_remove_transaction_ids
    }

    pub fn add_history_root(&mut self, history_root: Hash) {
        self.history_root = Some(history_root)
    }

    pub fn remove_block_transactions(&mut self, to_remove_block: Block) {
        self.to_remove_transaction_ids.extend(to_remove_block
        .into_iter()
        .map(move |(transaction)| transaction.id()),
        )
    }
}