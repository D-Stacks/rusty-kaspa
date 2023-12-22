use kaspa_consensus::model::stores::{headers::CompactHeaderData, acceptance_data::DbAcceptanceDataStore, block_transactions::DbBlockTransactionsStore};
use kaspa_consensus_core::{
    tx::{TransactionIndexType, Transaction}, acceptance_data::MergesetBlockAcceptanceData,
};
use kaspa_hashes::Hash;
use serde::{Deserialize, Serialize};

pub struct TxInclusionVerboseData {
    /// This corrosponds to a block hash which includes the transaction 
    /// 
    /// 1) in cases where the tx is not yet accepted, it will be the last seen block which holds the tx, this is not deterministic between nodes. 
    /// 2) in cases where the tx is accepted it will corrospond to the block hash which held the tx when it was successfully merged. This is deterministic between nodes.
    block: Hash,
    daa_score: u64,
    blue_score: u64,
    timestamp: u64,
}

impl TxInclusionVerboseData {
    fn new(block: Hash, daa_score: u64, blue_score: u64, timestamp: u64) -> Self {
        Self { block, daa_score, blue_score, timestamp }
    }
}

pub struct TxAcceptanceVerboseData {
    // this corrosponds to the block hash which accepted / successfully merged the transaction
    block: Hash,
    daa_score: u64,
    blue_score: u64,
    timestamp: u64,
    confirmations: Option<u64>,
}


impl TxAcceptanceVerboseData {

    /// When passing the `pov_blue_score`, preference should be to use the blue score of the sink cached by the txindex itself. 
    pub fn new(&self, block: Hash, daa_score: u64, blue_score: u64, timestamp: u64, pov_blue_score: Option<u64>) -> Self {
        Self { block, daa_score, blue_score, timestamp, confirmations: match pov_blue_score {
            Some(pov_blue_score) => pov_blue_score.checked_sub(blue_score),
            None => None,
        }
    }
}
}

pub struct TxIndexEntryCompact {
    tx_offset: TxOffset, 
    is_accepted: bool,
}

/// Holds a [`Transaction`]'s inlcluding_block [`Hash`] and [`TransactionIndexType`], for reference to the [`Transaction`] of a [`DbBlockTransactionsStore`].
#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash)]
pub struct TxOffset {
    including_block: Hash,
    transaction_index: TransactionIndexType,
}

impl TxOffset {
    pub fn new(including_block: Hash, transaction_index: TransactionIndexType) -> Self {
        Self { including_block, transaction_index }
    }

    pub fn including_block(&self) -> Hash {
        self.including_block
    }

    pub fn transaction_index(&self) -> TransactionIndexType {
        self.transaction_index
    }
}

/// Holds a Block's accepting [`Hash`] and `ordered_mergeset_index` of a block, for reference to the block's [`MergesetBlockAcceptanceData`] of a [`DbAcceptanceDataStore`].
#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash)]
pub struct BlockAcceptanceOffset {
    accepting_block: Hash,
    ordered_mergeset_index: usize,
}

impl BlockAcceptanceOffset {
    pub fn new(accepting_block: Hash, ordered_mergeset_index: usize) -> Self {
        Self { accepting_block, ordered_mergeset_index }
    }

    pub fn accepting_block(&self) -> Hash {
        self.accepting_block
    }

    pub fn ordered_mergeset_index(&self) -> usize {
        self.ordered_mergeset_index
    }
}