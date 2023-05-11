use std::{collections::HashSet, ops::Range};

use kaspa_consensus_core::{BlockHashMap, tx::{Transaction, TransactionIndexType}};
use kaspa_hashes::Hash;
use serde::{Deserialize, Serialize};

pub type MergeAcceptanceByBlock = BlockHashMap<MergeAcceptance>;

/// Holds the acceptance data of a merged block. 
#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash)]
pub struct MergeAcceptance {
    /// The block hash of a block which includes the transaction.
    pub accepting_block: Hash,
}
