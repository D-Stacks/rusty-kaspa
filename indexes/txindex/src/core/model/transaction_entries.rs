use kaspa_consensus_core::{tx::{TransactionIndexType, TransactionId}, BlockHashMap, HashMapCustomHasher};
use kaspa_hashes::Hash;
use serde::{Deserialize, Serialize};
use std::{vec::Vec, collections::HashMap};

pub type TransactionIdsByBlockHash = BlockHashMap<TransactionId>;
pub type TransactionEntriesById= HashMap<TransactionId, TransactionEntry>;


pub struct TransactionEntry {
    pub offset: Option<TransactionOffset>,
    pub accepting_block: Option<Hash>,
}

/// Holds the reference to a transaction for a block transaction store. 
#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash)]
pub struct TransactionOffset {
    pub including_block: Hash,
    pub transaction_index: TransactionIndexType,
}


pub type TransactionOffsets = BlockHashMap<Vec<TransactionIndexType>>;
