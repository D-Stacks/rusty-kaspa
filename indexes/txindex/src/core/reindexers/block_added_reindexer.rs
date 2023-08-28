use std::sync::Arc;
use kaspa_consensus_core::{
    block::Block, 
    BlockHashMap, 
    HashMapCustomHasher, 
    acceptance_data::{BlockAcceptanceData, MergesetBlockAcceptanceData, AcceptanceData},
    tx::TransactionIds,
};
use kaspa_hashes::Hash;
use crate::model::transaction_entries::{TransactionEntriesById, TransactionOffset};

pub struct TxIndexBlockAddedReindexer {
    to_remove_transaction_ids: TransactionEntriesById,
}

impl TxIndexBlockAddedReindexer {
    pub fn new() -> Self {
        Self {
            to_remove_transaction_ids: TransactionEntriesById::new(),
        }
    }

    pub fn get_to_add_transaction_entries(&self) {
        self.to_remove_transaction_ids
    }
    
    pub fn remove_block_transactions(&mut self, to_remove_block: Block) {
        self.to_remove_transaction_ids.extend(to_remove_block
        .into_iter()
        .map(move |(transaction)| transaction.id()),
        )
    }
}
