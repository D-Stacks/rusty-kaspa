use std::sync::Arc;
use kaspa_consensus_core::{
    block::Block, BlockHashMap, HashMapCustomHasher, 
    acceptance_data::AcceptanceData,
    tx::TransactionId,
};
use kaspa_hashes::Hash;
use crate::model::{TxCompactEntriesById, TxCompactEntry, TxAcceptanceDataByBlockHash, TxAcceptanceData};

pub struct TxIndexAcceptanceDataReindexer {
    to_add_transaction_entries: Arc<TxCompactEntriesById>,
    to_add_tx_acceptance: Arc<TxAcceptanceDataByBlockHash>,
    to_remove_acceptance_data_ids: Arc<TxCompactEntriesById>,
    to_remove_block_acceptance_data: Vec<Hash>,
    sink: Option<Hash>,
}

impl TxIndexAcceptanceDataReindexer {
    pub fn new() -> Self {
        Self {
            to_add_transaction_entries: Arc::new(TxCompactEntriesById::new()),
            to_add_tx_acceptance: Arc::new(TxAcceptanceDataByBlockHash::new()),
            to_remove_acceptance_data_ids: Arc::new(Vec::<TransactionId>::new()),
            to_remove_block_acceptance_data: Arc::new(Vec::<Hash>::new()),
            sink: None,
        }
    }

    pub fn get_to_add_transaction_entries(&self) -> TxCompactEntriesById {
        self.to_add_transaction_entries
    }

    pub fn get_to_remove_acceptance_data_ids(&self) -> Vec<TransactionId>{
        self.to_remove_acceptance_data_ids
    }

    fn add_sink(&mut self, sink: Hash ) {
        self.sink = Some(sink)
    }

    fn add_added_block_acceptance_data_transactions(
        &mut self, 
        to_add_accepting_block_hash: Hash, 
        to_add_added_acceptance_data: Arc<AcceptanceData>
    ) {
        self.sink = Some(to_add_accepting_block_hash);
        self.to_add_transaction_entries.extend(
            to_add_added_acceptance_data
            .into_iter()
            .flat_map(move |merged_block_acceptance| {
                
                // For block acceptance store
                self.to_add_tx_acceptance.insert(
                    merged_block_acceptance.block_hash, 
                    TxAcceptanceData::new(
                        to_add_accepting_block_hash, 
                        merged_block_acceptance.accepting_blue_score)
                    );
                
                merged_block_acceptance.accepted_transactions
                .into_iter()
                .flat_map(move |transaction_occurrence| {
                    (
                        transaction_occurrence.transaction_id,
                        TxCompactEntry::new(
                            TransactionOffset::new(
                                merged_block_acceptance.block_hash,
                                transaction_occurrence.index_within_block,
                            ),
                            true
                        )
                    )
                        }
                    )
            })
        )
    }

    fn remove_block_acceptance_data_transactions(&mut self, to_remove_acceptance_data: Arc<AcceptanceData>) {
        self.to_remove_acceptance_data_ids.extend(
        to_remove_acceptance_data
        .into_iter()
        .flat_map(move |merged_block_acceptance| {
                
                // For block acceptance store
                self.to_remove_block_acceptance_data.push(merged_block_acceptance.block_hash);
                
                merged_block_acceptance.accepted_transactions
                .into_iter()
                .flat_map(move|accepted_tx_entry| accepted_tx_entry.transaction_id)
            })
        )
    }
}
