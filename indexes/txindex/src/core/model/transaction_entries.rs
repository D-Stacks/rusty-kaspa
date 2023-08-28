use kaspa_consensus_core::{tx::{TransactionIndexType, TransactionId}, BlockHashMap, HashMapCustomHasher};
use kaspa_hashes::Hash;
use serde::{Deserialize, Serialize};
use std::{vec::Vec, collections::HashMap};

pub type TransactionIdsByBlockHash = BlockHashMap<TransactionId>;
pub type TransactionEntriesById= HashMap<TransactionId, TransactionEntry>;


pub struct TransactionEntry {
    pub offset: TransactionOffset,
    pub acceptance_data: Option<TransactionAcceptenceData>, // `None` indicates transaction is not accepted, only included.  
}

impl TransactionEntry {
    pub fn new(offset: Option<TransactionOffset>, acceptance_data: Option<Hash>) -> Self{
        Self { offset, acceptance_data}
    }

    pub fn clear_acceptance_data(&mut self) {
        self.acceptance_data = None
    }
}

/// Holds the reference to a transaction for a block transaction store. 
#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash)]
pub struct TransactionOffset {
    including_block: Hash,
    transaction_index: TransactionIndexType,
}

impl TransactionOffset {
    pub fn new(including_block: Hash, transaction_index: TransactionIndexType) -> Self { 
        Self{ including_block, transaction_index }
    }

    pub fn including_block(&self) {
        self.including_block
    }

    pub fn transaction_index(&self) {
        self.transaction_index
    }
}

struct TransactionAcceptanceData{
    accepting_block: Hash,
    accepting_blue_score: u64,
}

impl  TransactionAcceptanceData {
    pub fn new(accepting_block: Hash, accepting_blue_score: u64) -> Self {
        Self{ accepting_block, accepting_blue_score }
    }
    pub fn gaccepting_block(&self) -> Hash {
        self.accepting_block
    }

    pub fn accepting_blue_score(&self) -> u64 {
        self.accepting_blue_score
    }
}

pub type TransactionOffsets = BlockHashMap<Vec<TransactionIndexType>>;
