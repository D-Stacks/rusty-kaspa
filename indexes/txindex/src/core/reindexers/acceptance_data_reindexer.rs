use crate::{
    errors::TxIndexResult,
    model::{
        BlockAcceptanceChanges, TxAcceptanceChanges, TxAcceptanceData, TxAcceptanceDataByBlockHash, TxCompactEntriesById, TxOffset,
        TxOffsetById,
    },
};
use kaspa_consensus_core::{acceptance_data::AcceptanceData, tx::TransactionId};
use kaspa_hashes::Hash;
use std::sync::Arc;

pub struct TxIndexAcceptanceDataReindexer {
    tx_acceptance_changes: TxAcceptanceChanges,
    block_acceptance_changes: BlockAcceptanceChanges,
}

impl TxIndexAcceptanceDataReindexer {
    pub fn new() -> Self {
        Self { tx_acceptance_changes: TxAcceptanceChanges::new(), block_acceptance_changes: BlockAcceptanceChanges::new() }
    }

    pub fn accepted_tx_offsets(&self) -> Arc<TxOffsetById> {
        self.tx_acceptance_changes.accepted_tx_offsets
    }

    pub fn unaccepted_tx_ids(&self) -> Arc<Vec<TransactionId>> {
        self.tx_acceptance_changes.unaccepted_tx_ids
    }

    pub fn accepted_block_acceptance_data(&self) -> Arc<TxAcceptanceDataByBlockHash> {
        self.block_acceptance_changes.accepted_chain_block_acceptance_data
    }

    pub fn unaccepted_block_hashes(&self) -> Arc<Vec<Hash>> {
        self.block_acceptance_changes.unaccepted_chain_block_hashes
    }

    pub fn add_accepted_acceptance_data(
        &mut self,
        chained_block_hashes: Arc<Vec<Hash>>,
        accepted_mergeset_data: Arc<Vec<Arc<AcceptanceData>>>,
    ) -> TxIndexResult<()> {
        for (chained_block_hash, accepted_mergesets) in chained_block_hashes.into_iter().zip(accepted_mergeset_data.into_iter()) {
            for mergeset in accepted_mergesets.into_iter() {
                self.tx_acceptance_changes.unaccepted_tx_ids.extend(mergeset.accepted_transactions.into_iter().map(
                    move |accepted_tx_entry| {
                        (accepted_tx_entry.transaction_id, TxOffset::new(mergeset.block_hash, accepted_tx_entry.index_within_block))
                    },
                ));
                self.block_acceptance_changes
                    .accepted_chain_block_acceptance_data
                    .insert(mergeset.block_hash, TxAcceptanceData::new(chained_block_hash, mergeset.accepting_blue_score));
            }
        }

        Ok(())
    }

    pub fn remove_unaccepted_acceptance_data(
        &mut self,
        unchained_block_hashes: Arc<Vec<Hash>>,
        unaccepted_mergeset_data: Arc<Vec<Arc<AcceptanceData>>>,
    ) -> TxIndexResult<()> {
        for (unchained_block_hash, unaccepted_mergesets) in unchained_block_hashes.into_iter().zip(unaccepted_mergeset_data.into_iter()) {
            for mergeset in unaccepted_mergesets.into_iter() {
                self.tx_acceptance_changes
                    .unaccepted_tx_ids
                    .extend(mergeset.into_iter().map(move |accepted_tx_entry| accepted_tx_entry.transaction_id));
                self.block_acceptance_changes.unaccepted_chain_block_hashes.push(mergeset.block_hash);
            }
        }

        Ok(())
    }
}
