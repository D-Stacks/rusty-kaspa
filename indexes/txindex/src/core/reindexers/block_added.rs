use kaspa_hashes::{Hash, ZERO_HASH};

use crate::{core::model, model::transaction_entries::{TransactionEntry, TransactionOffset, TransactionEntriesById}};
use kaspa_consensus_core::block::Block;
/// A struct holding all block-added changes to the txindex with on-the-fly conversions and processing.
pub struct BlockAddedReindexer {
    pub to_add_transaction_entries: TransactionEntriesById,
    pub to_add_block: Hash,
}

impl BlockAddedReindexer {
    
    pub fn new(&self) -> Self {
        Self {
            to_add_transaction_entries: vec![],
            to_add_block: ZERO_HASH,
        }
    }

    pub fn add(&self, to_add_block: Block) {
        self.to_add_transaction_entries.append(
            block_to_transaction_entries_by_id(block)
        );
        self.to_add_block = to_add_block.hash();
    }
}

fn block_to_transaction_entries_by_id(block: Block) -> TransactionEntriesById {
    block.transactions
    .into_iter()
    .enumerate()
    .map(move |(i, transaction)|  {
        (
        transaction.id(),
        TransactionEntry {
            offset: Some(TransactionOffset {
                including_block: block.hash(),
                transaction_index: i,
            }),
            accepting_block: None, // block added notifications should never over acceptance data. 
        }
        )
    }).collect()
}