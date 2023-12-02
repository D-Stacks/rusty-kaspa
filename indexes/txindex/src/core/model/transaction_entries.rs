use kaspa_consensus::model::stores::headers::CompactHeaderData;
use kaspa_consensus_core::{
    acceptance_data::{AcceptanceData, AcceptedTxEntry, MergesetBlockAcceptanceData},
    tx::{Transaction, TransactionId, TransactionIndexType},
    BlockHashMap,
};
use kaspa_hashes::Hash;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, vec::Vec};

pub type TxEntriesById = HashMap<TransactionId, TxEntry>;
pub type TxOffsetById = HashMap<TransactionId, TxOffset>;
pub type TxOffsetByIdByBlockHash = BlockHashMap<HashMap<TransactionId, TxOffset>>;
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

/// [`TxOffsetWithDAA`] Holds the inlcluding_block [`Hash`] and [`TransactionIndexType`] as well as an inclusion daa score to reference further transaction details, and find the point of inclusion.
#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash)]
pub struct TxOffsetWithDAA {
    including_block: Hash,
    transaction_index: TransactionIndexType,
    daa_inclusion_score: u64,
}

/// Holds the inlcluding_block [`Hash`] and [`TransactionIndexType`] to reference further transaction details.
#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash)]
pub struct TxOffset {
    including_block: Hash,
    transaction_index: TransactionIndexType,
}

impl TxOffset {
    pub fn new(including_block: Hash, transaction_index: TransactionIndexType) -> Self {
        Self { including_block, transaction_index }
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
    accepting_blue_score: u64,
}

impl TxIndexAcceptanceData {
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
