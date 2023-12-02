use kaspa_consensus_core::tx::{Transaction, TransactionId, TransactionIndexType};
use kaspa_hashes::Hash;

use crate::model::{TxInclusionChanges, TxOffset, TxOffsetById};
pub struct TxIndexInclusionDataReindexer {
    tx_inclusion_changes: TxInclusionChanges,
}

impl TxIndexInclusionDataReindexer {
    pub fn new() -> Self {
        Self { tx_inclusion_changes: TxInclusionChanges::new() }
    }

    pub fn included_tx_offsets(&self) -> Arc<TxOffsetById> {
        self.tx_inclusion_changes.included_tx_offsets
    }

    pub fn unincluded_tx_ids(&self) -> Arc<TransactionId> {
        self.tx_inclusion_changes.unincluded_tx_ids
    }

    pub fn add_transactions(&mut self, including_block: Hash, tx_iter: impl Iterator<Item = (TransactionIndexType, Transaction)>) {
        self.tx_inclusion_changes
            .included_tx_offsets
            .extend(tx_iter.map(move |(transaction_index, tx)| (tx.id(), TxOffset::new(including_block, transaction_index))))
    }

    pub fn remove_transactions(&mut self, tx_iter: impl Iterator<Item = Transaction>) {
        self.tx_inclusion_changes.unincluded_tx_ids.extend(tx_iter);
    }
}
