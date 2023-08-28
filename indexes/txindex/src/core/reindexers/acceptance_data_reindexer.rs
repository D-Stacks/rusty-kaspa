use std::sync::Arc;
use kaspa_consensus_core::{
    block::Block, BlockHashMap, HashMapCustomHasher, 
    acceptance_data::{BlockAcceptanceData, MergesetBlockAcceptanceData, AcceptanceData},
    tx::TransactionId,
};
use kaspa_hashes::Hash;
use crate::model::transaction_entries::{TransactionEntriesById, TransactionOffset};

pub struct TxIndexAcceptanceDataReindexer {
    to_add_transaction_entries: TransactionEntriesById,
    to_remove_acceptance_data_ids: TransactionEntriesById,
    sink: Option<Hash>,
}

impl TxIndexAcceptanceDataReindexer {
    pub fn new() -> Self {
        Self {
            to_add_transaction_entries: TransactionEntriesById::new(),
            to_remove_acceptance_data_ids: Vec::<TransactionId>::new(),
            sink: None,
        }
    }

    pub fn get_to_add_transaction_entries(&self) -> TransactionEntriesById {
        self.to_add_transaction_entries
    }

    pub fn get_to_remove_acceptance_data_ids(&self) -> TransactionIds{
        self.to_remove_acceptance_data_ids
    }

    pub fn add_added_block_acceptance_data_transactions(
        &mut self, 
        to_add_accepting_block_hash: Hash, 
        to_add_added_acceptance_data: Arc<AcceptanceData>
    ) {
        self.sink = Some(to_add_accepting_block_hash);
        self.to_add_transaction_entries.extend(
            to_add_added_acceptance_data
            .into_iter()
            .flat_map(move |merged_block_acceptance| {
                merged_block_acceptance.accepted_transactions
                .into_iter()
                .flat_map(move |transaction_occurrence| {
                    (
                        transaction_occurrence.transaction_id,
                        TransactionEntry::new(
                            TransactionOffset::new(
                                merged_block_acceptance.merged_block_hash,
                                transaction_occurrence.transaction_index,
                            ),
                            Some(TransactionAcceptanceData::new(
                                to_add_accepting_block_hash, 
                                merged_block_acceptance.accepted_blue_score,
                            )),
                        )
                    )
                        }
                    )
            })
        )
    }

    pub fn remove_block_acceptance_data_transactions(&mut self, to_remove_acceptance_data: Arc<AcceptanceData>) {
        self.to_remove_acceptance_data_ids.extend(
        to_remove_acceptance_data
        .into_iter()
        .flat_map(move |merged_block_acceptance| {
                merged_block_acceptance.accepted_transactions
                .into_iter()
                .flat_map(move|transaction_occurrence| transaction_occurrence.transaction_id)
            })
        )
    }
}
