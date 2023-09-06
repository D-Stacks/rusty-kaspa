use std::sync::Arc;
use kaspa_consensus_core::{
    block::Block, BlockHashMap, HashMapCustomHasher, 
    acceptance_data::AcceptanceData,
    tx::TransactionId,
};
use kaspa_hashes::Hash;
use crate::model::{TxCompactEntriesById, TxCompactEntry, TxAcceptanceDataByBlockHash, TxAcceptanceData, TxOffsetById};

pub struct TxIndexAcceptanceDataReindexer {
    added_transaction_offsets: Arc<TxOffsetById>,
    removed_transaction_offsets: Vec<TransactionId>,
    added_tx_acceptance: Arc<TxAcceptanceDataByBlockHash>,
    removed_block_acceptance: Vec<Hash>,
    sink: Option<Hash>,
}

impl TxIndexAcceptanceDataReindexer {
    pub fn new() -> Self {
        Self {
            added_transaction_offsets: Arc::new(TxOffsetById::new()),
            removed_transaction_offsets: Arc::new(Vec::<TransactionId>::new()),
            added_tx_acceptance: Arc::new(TxAcceptanceDataByBlockHash::new()),
            removed_block_acceptance: Arc::new(Vec::<Hash>::new()),
            sink: None,
        }
    }

    pub fn added_transaction_offsets(&self) -> TxCompactEntriesById {
        self.added_transaction_offsets
    }

    pub fn removed_transaction_offsets(&self) -> Vec<TransactionId>{
        self.removed_transaction_offsets
    }

    fn add_sink(&mut self, sink: Hash ) {
        self.sink = Some(sink)
    }

    fn add_tx_acceptance_data(
        &mut self, 
        accepting_block_hash: Hash, 
        acceptance_data: Arc<AcceptanceData>
    ) {
        self.sink = Some(to_add_accepting_block_hash);
        self.added_transaction_offsets.extend(
            to_add_added_acceptance_data
            .into_iter()
            .flat_map(move |merged_block_acceptance| {
                
                // For block acceptance store
                self.added_tx_acceptance.insert(
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
