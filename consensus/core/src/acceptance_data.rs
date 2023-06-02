use std::sync::Arc;

use kaspa_hashes::Hash;
use serde::{Deserialize, Serialize};

use crate::{tx::TransactionOccurrence, BlockIndexMap};

pub type BlockAcceptanceData = BlockIndexMap<Arc<AcceptanceData>>;

pub type AcceptanceData = Vec<MergesetBlockAcceptanceData>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergesetBlockAcceptanceData {
    pub accepted_blue_score: u64,
    pub merged_block_hash: Hash,
    pub accepted_transactions: Vec<TransactionOccurrence>,
}