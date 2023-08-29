use kaspa_consensus_core::{
    block::Block, 
    HashMapCustomHasher, 
};
use kaspa_hashes::Hash;
use crate::model::{transaction_entries::{TransactionCompactEntriesById, TransactionOffset}, TxCompactEntry, TxOffset};

pub struct TxIndexBlockAddedReindexer {
    to_add_compact_transaction_entries: TransactionCompactEntriesById,
}

impl TxIndexBlockAddedReindexer {
    pub fn new() -> Self {
        Self {
            to_add_compact_transaction_entries: TransactionCompactEntriesById::new(),
        }
    }

    pub fn get_to_add_compact_transaction_entries(&self) {
        self.to_add_compact_transaction_entries
    }
    
    pub fn add_block_transactions(&mut self, to_add_block: Block) {
        self.to_add_compact_transaction_entries.extend(to_add_block.transactions
        .into_iter()
        .enumerate()
        .map(move |(transaction_index, transaction)| {
            TxCompactEntry::new(TxOffset::new(to_add_block.hash(), transaction_index), false)        
            }
            ),
        )
    }
}
