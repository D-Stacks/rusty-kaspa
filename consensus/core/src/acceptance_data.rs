use kaspa_hashes::Hash;
use serde::{Deserialize, Serialize};

use crate::{tx::TransactionId, BlockHashMap};

// Acceptance data indexed by accepting chain blocks
pub type ChainAcceptanceData = BlockHashMap<Vec<MergesetBlockAcceptanceData>>;

pub type AcceptanceData = Vec<MergesetBlockAcceptanceData>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergesetBlockAcceptanceData {
    pub block_hash: Hash,
    pub accepted_transactions: Vec<AcceptedTxEntry>,
    pub accepting_blue_score: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptedTxEntry {
    pub transaction_id: TransactionId,
    pub index_within_block: u32,
}
