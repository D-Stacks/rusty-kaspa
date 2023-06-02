
use kaspa_consensus_core::{tx::TransactionId, acceptance_data::BlockAcceptanceData};
use kaspa_hashes::{Hash, ZERO_HASH};
use crate::{core::model, model::{transaction_entries::{TransactionEntry, TransactionEntriesByBlockHash, TransactionOffset, TransactionEntriesById, TransactionAcceptanceData}, params::TxIndexParams}};

/// A struct holding all acceptance data changes to the txindex with on-the-fly conversions and processing.
pub struct AcceptanceDataReindexer {
    pub to_add_transaction_entries: TransactionEntriesById,
    pub to_remove_transaction_ids: Vec<TransactionId>,
    pub sink: Hash,
    pub process_acceptance: bool,
    pub process_offsets: bool,
}

impl AcceptanceDataReindexer {
    
    pub fn new(&self,
        process_acceptance: bool,
        process_offsets: bool,
    ) -> Self {
        Self { 
            to_add_transaction_entries: vec![],
            to_remove_transaction_ids: vec![],
            sink: ZERO_HASH, 
            process_acceptance,
            process_offsets,
        }
    }

    pub fn add(&mut self, 
        to_add_acceptance_data: BlockAcceptanceData, 
        to_remove_acceptance_data: Option<BlockAcceptanceData>,
    ) {
        self.to_add_transaction_entries.append(
            block_acceptance_to_transaction_entries_by_id(
                to_add_acceptance_data,
                self.process_acceptance,
                self.process_offsets,
            )
        );
        self.to_remove_transaction_ids = if let Some(to_remove_acceptance_data) = to_remove_acceptance_data {
            block_acceptance_to_transaction_ids(
                to_remove_acceptance_data, 
                |transaction_id: TransactionId| !self.to_add_transaction_entries.contains_key(t)
            )
        } else {
            vec![]
        };
        self.sink = to_add_acceptance_data.last().expect("expected new acceptance data").0;

    }

    pub fn clear(&mut self) {
        self.to_add_transaction_entries = vec![];
        self.to_remove_transaction_ids = vec![];
        self.sink = ZERO_HASH;
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
