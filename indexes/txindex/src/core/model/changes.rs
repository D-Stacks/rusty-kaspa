use crate::{
    errors::{TxIndexError, TxIndexResult},
    model::{TxAcceptanceData, TxAcceptanceDataByBlockHash, TxCompactEntriesById, TxCompactEntry, TxOffset, TxOffsetById},
};
use kaspa_consensus::processes::ghostdag::mergeset;
use kaspa_consensus_core::{
    acceptance_data::{AcceptanceData, AcceptedTxEntry, MergesetBlockAcceptanceData},
    block::Block,
    tx::{Transaction, TransactionId},
    BlockHashMap, HashMapCustomHasher, BlockHashSet,
};
use kaspa_hashes::Hash;
use std::sync::Arc;

pub struct TxIndexBlockRemovedChanges {
    pub accepted_offets_removed: Arc<Vec<TransactionId>>,
    pub included_offets_removed: Arc<Vec<TransactionId>>,
    pub block_removed: Hash,
    pub history_root: Hash,
}

impl TxIndexBlockRemovedChanges {
    fn new(accepted_offets_removed: Arc<Vec<TransactionId>>, included_offets_removed: Arc<Vec<TransactionId>>, block_removed: Hash, history_root: Hash) -> Self {
        Self { accepted_offets_removed, included_offets_removed, block_removed, history_root }
    }
}

pub struct TxIndexBlockAddedChanges {
    pub included_offsets_added: Arc<Vec<(TransactionId, TxOffset)>>,
    pub tip_added: Hash,
}

impl  TxIndexBlockAddedChanges {
    fn new(included_offsets_added: Arc<Vec<(TransactionId, TxOffset)>>, tip_added: Hash) -> Self {
        Self { included_offsets_added, tip_added }
    }
}

pub struct TxIndexVSPCCChanges {
    /// the [`TransactionId`]s also corrospond to included offsets which where removed
    pub accepted_offsets_added: Arc<Vec<(TransactionId, TxOffset)>>,
    /// the [`TransactionId`]s also corrospond to accepted offsets which where removed
    pub included_offsets_added: Arc<Vec(TransactionId, TxOffset)>,
    pub sink: Hash,
    pub tips_removed: BlockHashSet<Hash>,
}

impl TxIndexVSPCCChanges {
    fn new(accepted_offsets_added: Arc<Vec<(TransactionId, TxOffset)>>, included_offsets_added: Arc<Vec(TransactionId, TxOffset)>, sink: Hash, tips_removed: BlockHashSet) -> Self {
        Self { accepted_offsets_added, included_offsets_added, sink, tips_removed }
    }
}
