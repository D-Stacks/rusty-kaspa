use kaspa_hashes::Hash;
use serde::{Deserialize, Serialize};

use crate::{
    tx::{TransactionId, TransactionIndexType},
    BlockHashMap,
};

pub type AcceptanceData = Vec<MergesetBlockAcceptanceData>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergesetBlockAcceptanceData {
    pub block_hash: Hash,
    pub accepted_transactions: Vec<TxEntry>,
    pub unaccepted_transactions: Vec<TxEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxEntry {
    pub transaction_id: TransactionId,
    pub index_within_block: TransactionIndexType,
}
