use std::{collections::{HashSet, HashMap}, sync::Arc};

use kaspa_consensus_core::{tx::{TransactionIndexType, TransactionId}, BlockHashMap, BlockHashSet};
use kaspa_hashes::Hash;
use serde::{Deserialize, Serialize};

pub type TransactionHashSet = HashSet<TransactionId>;
pub type TxOffsetByTyId = HashMap<TransactionId, TxOffset>;
pub type BlockAcceptanceOffsetByHash = BlockHashMap<BlockAcceptanceOffset>;
pub type MergeSetIDX = u16;


/// A struct holding tx diffs to be committed to the txindex via `added` and `removed`. 
#[derive(Debug, Clone, Default)]
pub struct TxOffsetDiff {
    pub added: Arc<TxOffsetByTyId>,
    pub removed: Arc<TransactionHashSet>,
}

impl TxOffsetDiff {
    pub fn new(added: Arc<TxOffsetByTyId>, removed: Arc<TransactionHashSet>, ) -> Self {
        Self { added, removed }
    }
}

/// A struct holding block accepted diffs to be committed to the txindex via `added` and `removed`. 
#[derive(Debug, Clone, Default)]
pub struct BlockAcceptanceOffsetDiff {
    pub added: Arc<BlockAcceptanceOffsetByHash>,
    pub removed: Arc<BlockHashSet>,
}

impl BlockAcceptanceOffsetDiff {
    pub fn new(added: Arc<BlockAcceptanceOffsetByHash>, removed: Arc<BlockHashSet>) -> Self {
        Self { added, removed }
    }
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

/// Holds a Block's accepting [`Hash`] and [`MergeSetIDX`] of a block, for reference to the block's [`MergesetBlockAcceptanceData`] of a [`DbAcceptanceDataStore`].
#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash)]
pub struct BlockAcceptanceOffset {
    accepting_block: Hash,
    ordered_mergeset_index: MergeSetIDX,
}

impl BlockAcceptanceOffset {
    pub fn new(accepting_block: Hash, ordered_mergeset_index: MergeSetIDX) -> Self {
        Self { accepting_block, ordered_mergeset_index }
    }

    pub fn accepting_block(&self) -> Hash {
        self.accepting_block
    }

    pub fn ordered_mergeset_index(&self) -> MergeSetIDX {
        self.ordered_mergeset_index
    }
}
