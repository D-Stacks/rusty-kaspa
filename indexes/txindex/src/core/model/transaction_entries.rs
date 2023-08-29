use kaspa_consensus::model::stores::headers::CompactHeaderData;
use kaspa_consensus_core::{tx::{TransactionIndexType, TransactionId, Transaction}, BlockHashMap};
use kaspa_hashes::Hash;
use serde::{Deserialize, Serialize};
use std::{vec::Vec, collections::HashMap};

pub type TxEntriesById = HashMap<TransactionId, TxEntry>;
pub type TxCompactEntriesById = HashMap<TransactionId, TxCompactEntry>;
pub type TxAcceptanceDataByBlockHash = BlockHashMap<TxAcceptanceData>;

#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash)]
pub struct TxEntry {
    offset: TxOffset,
    tx_acceptance_data: Option<TxAcceptanceData>,
}

impl TxEntry {
    pub fn new(offset: TxOffset, tx_acceptance_data: Option<TxAcceptanceData>) -> Self {
        Self { offset, tx_acceptance_data }
    }

    pub fn offset(&self) -> TxOffset {
        self.offset
    }

    pub fn tx_acceptance_data(&self) -> Option<TxAcceptanceData> {
        self.tx_acceptance_data
    }

    pub fn is_accepted(&self) -> bool {
        self.tx_acceptance_data.is_some()
    }
}

#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash)]
pub struct TxCompactEntry {
    offset: TxOffset,
    is_accepted: bool,
}

impl TxCompactEntry {
    pub fn new(offset: TxOffset, is_accepted: bool) -> Self{
        Self { offset, is_accepted}
    }

    pub fn set_as_unaccepted(&mut self) {
        self.is_accepted = false
    }

    pub fn set_as_accepted(&mut self, is_accepted: bool) {
        self.is_accepted = true
    }

    pub fn is_accepted(&self) -> bool {
        self.is_accepted
    }

    pub fn offset(&self) -> TxOffset {
        self.offset
    }

}

/// Holds the inlcluding_block [`Hash`] and [`TransactionIndexType`] to reference further transaction details. 
#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash)]
pub struct TxOffset {
    including_block: Hash,
    transaction_index: TransactionIndexType,
}

impl TxOffset {
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

#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash)]
pub struct TxAcceptanceData {
    accepting_block_hash: Hash,
    accepting_blue_score: u64
}

impl TxAcceptanceData {
    pub fn new(accepting_block_hash: Hash, accepting_blue_score: u64) -> Self {
        Self { accepting_block_hash, accepting_blue_score }
    }

    fn accepting_block_hash(&self) -> Hash {
        self.accepting_block_hash
    }

    fn accepting_blue_score(&self) -> u64 {
        self.accepting_blue_score
    }
}
