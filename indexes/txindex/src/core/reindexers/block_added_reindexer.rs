use std::sync::Arc;

use kaspa_consensus_core::{
    block::Block, 
    HashMapCustomHasher, 
};
use kaspa_hashes::Hash;
use crate::model::{transaction_entries::{TransactionCompactEntriesById, TransactionOffset}, TxCompactEntry, TxOffset, TxOffsetById};

pub struct TxIndexBlockAddedReindexer {
    added_transaction_offsets: Arc<TxOffsetById>,
    block_added: Option<Hash>
}

impl TxIndexBlockAddedReindexer {
    pub fn new() -> Self {
        Self {
            added_transaction_offsets: Arc::new(TxOffsetById::new()),
            block_added: None,
        }
    }

    pub fn added_transaction_offsets(&self) -> Arc<TxOffsetById> {
        self.added_transaction_offsets
    }
    
    pub fn add_block_transactions(&mut self, to_add_block: Block) {
        self.block_added = Some(to_add_block.hash());
        drop(to_add_block.header);


        self.added_transaction_offsets.extend(to_add_block.transactions
        .into_iter()
        .enumerate()
        .map(move |(transaction_index, transaction)| {
            TxCompactEntry::new(TxOffset::new(to_add_block.hash(), transaction_index), false)        
                }
            ),
        )
        }
}
