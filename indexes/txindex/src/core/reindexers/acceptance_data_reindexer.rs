use std::sync::Arc;
use kaspa_consensus_core::{
    block::Block, BlockHashMap, HashMapCustomHasher, 
    acceptance_data::AcceptanceData,
    tx::TransactionId,
};
use kaspa_hashes::Hash;
use crate::{model::{TxCompactEntriesById, TxCompactEntry, TxAcceptanceDataByBlockHash, TxAcceptanceData, TxOffsetById, TxOffset}, errors::{TxIndexResult, TxIndexError}};

pub struct TxIndexAcceptanceDataReindexer {
    added_accepted_tx_offsets: Arc<TxOffsetById>,
    removed_accepted_tx_offsets: Arc<TxOffsetById>,
    added_block_acceptance: Arc<TxAcceptanceDataByBlockHash>,
    removed_block_acceptance: Arc<Vec<Hash>>,
    sink: Option<Hash>,
}

impl TxIndexAcceptanceDataReindexer {
    pub fn new() -> Self {
        Self {
            added_accepted_tx_offsets: Arc::new(TxOffsetById::new()),
            removed_accepted_tx_offsets: Arc::new(TxOffsetById::new()),
            added_block_acceptance: Arc::new(TxAcceptanceDataByBlockHash::new()),
            removed_block_acceptance: Arc::new(Vec::<Hash>::new()),
            sink: None,
        }
    }

    pub fn added_accepted_tx_offsets(&self) -> Arc<TxOffsetById> {
        self.added_accepted_tx_offsets
    }

    pub fn removed_accepted_tx_offsets(&self) -> Arc<TxOffsetById> {
        self.removed_accepted_tx_offsets
    }

    pub fn added_block_acceptance(&self) -> Arc<TxAcceptanceDataByBlockHash> {
        self.added_block_acceptance
    }

    pub fn removed_block_acceptance(&self) -> Arc<Vec<Hash>> {
        self.removed_block_acceptance
    }

    pub fn sink(&self) -> Option<Hash> {
        self.sink
    }

    pub fn update_acceptance(
        &mut self,
        to_add_chain_blocks: Arc<Vec<Hash>>, 
        to_remove_chain_blocks: Arc<Vec<Hash>>, 
        to_add_acceptance_data: Arc<AcceptanceData>, 
        to_remove_acceptance_data: Arc<AcceptanceData>) -> TxIndexResult<()> {
            
            // 1) Do some checks to verify input
            if to_add_chain_blocks.len() != to_add_acceptance_data.len() {
                TxIndexError::ReindexingError(
                    format!(
                        "Failed reindexing acceptance data - amount of added chain blocks {0} does not match amount of added acceptance data {1}", 
                        to_add_chain_blocks.len(), 
                        to_add_acceptance_data.len()
                    ))
            } else if to_remove_chain_blocks.len() != to_remove_acceptance_data.len() {
                TxIndexError::ReindexingError(
                    format!(
                        "Failed reindexing acceptance data - amount of removed chain blocks {0} does not match amount of removed acceptance data {1}", 
                        to_add_chain_blocks.len(), 
                        to_add_acceptance_data.len()
                    )) 
            } else if to_add_chain_blocks.is_empty() {
                TxIndexError::ReindexingError(
                    "Failed reindexing acceptance data - must have at least one added chain block".to_string()
                )
            }
            
            // 2) set new sink
            self.sink = to_add_chain_blocks.last().unwrap(); // It is safe to call `.unwrap()` since we verify it is none-empty in checks above
            
            // 3) Process added acceptance
            self.added_accepted_tx_offsets.extend(
                to_add_acceptance_data
                .into_iter()
                .enumerate()
                .flat_map(move |(i, merged_block_acceptance)| {
                    
                    // For block acceptance store
                    self.added_block_acceptance.insert(
                        to_add_chain_blocks.get(i).unwrap(), // it is safe to `.unwrap()` since we check for equal length in checks. 
                        TxAcceptanceData::new(
                            to, 
                            merged_block_acceptance.accepting_blue_score)
                        );
                    
                    merged_block_acceptance.accepted_transactions
                    .into_iter()
                    .flat_map(move |transaction_occurrence| {
                        (
                        transaction_occurrence.transaction_id,
                            TransactionOffset::new(
                                merged_block_acceptance.block_hash,
                                transaction_occurrence.index_within_block,
                            ),
                        )
                })
            })
        );

        // 4) Process removed acceptance
        self.removed_accepted_tx_offsets.extend(
            to_remove_acceptance_data
            .into_iter()
            .flat_map(move |unmerged_block_acceptance| {
                if !self.added_block_acceptance.contains_key(&unmerged_block_acceptance.block_hash) {
                    self.removed_block_acceptance.append(unmerged_block_acceptance.block_hash);
                };
                unmerged_block_acceptance.accepted_transactions.into_iter().filter_map(
                    move |unaccepted_transaction| {
                        if self.added_accepted_tx_offsets.contains_key(unaccepted_transaction) {
                            None
                        } else { 
                            Some((
                                unaccepted_transaction.transaction_id,
                                TxOffset::new(
                                    unmerged_block_acceptance.block_hash,
                                    unaccepted_transaction.index_within_block
                                    )
                                )) 
                        }
                    }
                    )
                })
            );
        Ok(())
    }

}