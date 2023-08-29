use kaspa_consensus_core::{
    block::Block, 
    HashMapCustomHasher, 
    tx::TransactionId,
};

pub struct TxIndexBlockPrunedReindexer {
    to_remove_transaction_ids: Vec<TransactionId>,
}

impl TxIndexBlockPrunedReindexer {
    pub fn new() -> Self {
        Self {
            to_remove_transaction_ids: Vec::new::<TransactionId>(),
        }
    }

    pub fn get_to_remove_transaction_ids(&self) {
        self.to_remove_transaction_ids
    }

    pub fn remove_block_transactions(&mut self, to_remove_block: Block) {
        self.to_remove_transaction_ids.extend(to_remove_block.transactions
        .into_iter()
        .map(move |(transaction)| transaction.id()),
        )
    }
}