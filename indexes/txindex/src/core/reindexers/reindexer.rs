use kaspa_consensus_core::{block::Block, BlockHashMap, HashMapCustomHasher};

pub struct TxIndexReindexer {
    params: Arc<TxIndexParams>,
    to_add_transaction_entries: TransactionEntriesById,
    to_remove_transaction_entries: TransactionEntriesById,
    sink: Option<Hash>,
    history_root: Option<Hash>,
    last_block_added: Option<Hash>
}

impl TxIndexReindexer {
    fn new(params: Arc<TxIndexParams>) -> Self {
        Self {
            params,
            to_add_transaction_entries: TransactionEntriesById::new(),
            to_remove_transaction_entries: TransactionEntriesById::new(),
            sink: None,
            history_root: None,
            last_block_added: None,
        }
    }

    fn add_block(&mut self, block: Block) {
        self.to_add_transaction_entries.append(
            block_to_transaction_entries_by_id(block, move |transaction_id| { !self.to_add_transaction_entries.has(transaction_id) })
        );
        self.last_block_added = Some(to_add_block.hash());
    }

    fn add_block_acceptance_data(&mut self, block_acceptance: BlockAcceptanceData) {
        self.to_add_transaction_entries.append(
            block_to_transaction_entries_by_id(block)
        );
        self.sink = Some(block_acceptance.get(block_acceptance.len()).0);
    }
}

fn block_acceptance_to_transaction_entries_by_id(
    block_acceptance: BlockAcceptanceData, 
    process_acceptance: bool, 
    process_offsets: bool
) -> TransactionEntriesById {        
    block_acceptance
    .into_iter()
        .map(move |(accepting_block_hash, acceptance_data)| {
            acceptance_data
            .into_iter()
            .map(move |merged_block_acceptance| {
                merged_block_acceptance.accepted_transactions
                .into_iter()
                .map(move|transaction_occurrence| {
                    (
                    transaction_occurrence.transaction_id,
                    TransactionEntry {
                        offset: if process_offsets {
                            Some(TransactionOffset {
                            including_block: merged_block_acceptance.merged_block_hash,
                            transaction_index: transaction_occurrence.transaction_index,
                            })
                        } else { 
                            None 
                        },
                        accepting_block: if process_acceptance {
                            Some(accepting_block_hash)
                        } else {
                            None
                        }
                    }
                    )
                })
            })
        }).collect()
}

fn block_acceptance_to_transaction_ids<F>(
    block_acceptance: BlockAcceptanceData, 
    filter_op: F 
) -> Vec<TransactionId>
where 
    F: Fn(TransactionId) -> bool
{
    block_acceptance
    .into_iter()
    .map(move |(accepting_block_hash, acceptance_data)| {
        acceptance_data
        .into_iter()
        .map(move |merged_block_acceptance| {
            merged_block_acceptance.accepted_transactions
            .into_iter()
            .filter_map(move|transaction_occurrence| {
                if filter_op(transaction_occurrence.transaction_id) {
                    Some(transaction_occurrence.transaction_id)
                } else {
                    None
                }
            })
     })
    }).collect()
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