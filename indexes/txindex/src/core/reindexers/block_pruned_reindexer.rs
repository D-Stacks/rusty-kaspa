use std::sync::Arc;

use kaspa_consensus_core::{block::Block, tx::TransactionId, HashMapCustomHasher};

pub struct TxIndexBlockPrunedReindexer {
    removed_transaction_offsets: Arc<Vec<TransactionId>>,
}

impl TxIndexBlockPrunedReindexer {
    pub fn new() -> Self {
        Self { removed_transaction_offsets: Arc::new(Vec::new::<TransactionId>()) }
    }

    pub fn removed_transaction_offsets(&self) {
        self.removed_transaction_offsets
    }

    pub fn remove_block_transactions(&mut self, to_remove_block: Block) {
        self.removed_transaction_offsets.extend(to_remove_block.transactions.into_iter().map(move |(transaction)| transaction.id()))
    }

    pub fn clear(&mut self) {
        self.removed_transaction_offsets = Arc::new(Vec::new::<TransactionId>());
    }
}
