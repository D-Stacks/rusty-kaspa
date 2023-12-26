use kaspa_consensus_core::tx::TransactionIndexType;
use kaspa_hashes::Hash;
use serde::{Deserialize, Serialize};

pub struct TxInclusionVerboseData {
    /// This corrosponds to a block hash which includes the transaction
    ///
    /// corrosponds to the block hash which held the tx when it was successfully merged. This is deterministic between nodes.
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
        Self {
            block,
            daa_score,
            blue_score,
            timestamp,
            confirmations: pov_blue_score.map_or(None, move |pov_blue_score| blue_score.checked_sub(pov_blue_score)),
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
    ordered_mergeset_index: u16,
}

impl BlockAcceptanceOffset {
    pub fn new(accepting_block: Hash, ordered_mergeset_index: u16) -> Self {
        Self { accepting_block, ordered_mergeset_index }
    }

    pub fn accepting_block(&self) -> Hash {
        self.accepting_block
    }

    pub fn ordered_mergeset_index(&self) -> u16 {
        self.ordered_mergeset_index
    }
}
