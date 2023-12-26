use crate::model::{BlockAcceptanceOffset, TxOffset};
use kaspa_consensus_core::{
    tx::{TransactionId, TransactionIndexType},
    BlockHashMap, BlockHashSet, HashMapCustomHasher,
};
use kaspa_hashes::Hash;
use kaspa_consensus_notify::notification::{VirtualChainChangedNotification as ConsensusVirtualChainChangedNotification};
use kaspa_utils::arc::ArcExtensions;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

#[derive(Debug, Clone, Default)]
pub struct TxOffsetChanges {
    pub to_add: Arc<HashMap<TransactionId, TxOffset>>,
    pub to_remove: Arc<HashSet<TransactionId>>,
}

impl TxOffsetChanges {
    pub fn new(to_remove: Arc<HashSet<TransactionId>>, to_add: Arc<HashMap<TransactionId, TxOffset>>) -> Self {
        Self { to_remove, to_add }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BlockAcceptanceOffsetsChanges {
    pub to_add: Arc<BlockHashMap<BlockAcceptanceOffset>>,
    pub to_remove: Arc<BlockHashSet>,
}

impl BlockAcceptanceOffsetsChanges {
    pub fn new(to_add: Arc<BlockHashMap<BlockAcceptanceOffset>>, to_remove: Arc<BlockHashSet>) -> Self {
        Self { to_add, to_remove }
    }
}

pub struct TxIndexVSPCCChanges {
    pub block_acceptance_offsets_changes: BlockAcceptanceOffsetsChanges,
    pub tx_offset_changes: TxOffsetChanges,
    pub new_sink: Hash,
}

impl TxIndexVSPCCChanges {
    pub fn new(
        block_acceptance_offsets_changes: BlockAcceptanceOffsetsChanges,
        tx_offset_changes: TxOffsetChanges,
        new_sink: Hash,
    ) -> Self {
        Self { block_acceptance_offsets_changes, tx_offset_changes, new_sink }
    }

    pub fn block_acceptance_offsets_changes(&self) -> BlockAcceptanceOffsetsChanges {
        self.block_acceptance_offsets_changes.clone()
    }

    pub fn tx_offset_changes(&self) -> TxOffsetChanges {
        self.tx_offset_changes.clone()
    }

    pub fn new_sink(&self) -> Hash {
        self.new_sink
    }
}

impl From<ConsensusVirtualChainChangedNotification> for TxIndexVSPCCChanges {
    fn from(vspcc_notification: ConsensusVirtualChainChangedNotification) -> Self {
        let new_sink = match vspcc_notification.added_chain_block_hashes.last() {
            Some(new_sink) => *new_sink,
            None => *vspcc_notification
                .removed_chain_block_hashes
                .last()
                .expect("Expected at least one block to be added or removed, i.e. a none_empty chain path"),
        };

        drop(vspcc_notification.removed_chain_block_hashes); // we do not require this anymore.

        let mut tx_offsets_to_add = HashMap::new();
        let mut tx_offsets_to_remove = HashSet::new();
        let mut block_acceptance_offsets_to_add = BlockHashMap::<BlockAcceptanceOffset>::new();
        let mut block_acceptance_offsets_to_remove = BlockHashSet::new();

        for (accepting_block_hash, acceptance_data) in vspcc_notification
            .added_chain_block_hashes
            .unwrap_or_clone()
            .into_iter()
            .zip(vspcc_notification.added_chain_blocks_acceptance_data.unwrap_or_clone().into_iter())
        {
            for (i, mergeset) in acceptance_data.unwrap_or_clone().into_iter().enumerate() {
                tx_offsets_to_add.extend(
                    mergeset
                        .accepted_transactions
                        .into_iter()
                        .map(|tx_entry| (tx_entry.transaction_id, TxOffset::new(mergeset.block_hash, i as TransactionIndexType))),
                );

                block_acceptance_offsets_to_add
                    .insert(mergeset.block_hash, BlockAcceptanceOffset::new(accepting_block_hash, i as u16));
            }
        }

        for acceptance_data in vspcc_notification.removed_chain_blocks_acceptance_data.unwrap_or_clone().into_iter() {
            for mergeset in acceptance_data.unwrap_or_clone().into_iter() {
                tx_offsets_to_remove.extend(
                    mergeset
                        .accepted_transactions
                        .into_iter()
                        .filter(|tx_entry| tx_offsets_to_add.contains_key(&tx_entry.transaction_id))
                        .map(|tx_entry| tx_entry.transaction_id),
                );

                if !block_acceptance_offsets_to_add.contains_key(&mergeset.block_hash) {
                    block_acceptance_offsets_to_remove.insert(mergeset.block_hash);
                };
            }
        }

        Self {
            new_sink,
            block_acceptance_offsets_changes: BlockAcceptanceOffsetsChanges {
                to_add: Arc::new(block_acceptance_offsets_to_add),
                to_remove: Arc::new(block_acceptance_offsets_to_remove),
            },
            tx_offset_changes: TxOffsetChanges { to_add: Arc::new(tx_offsets_to_add), to_remove: Arc::new(tx_offsets_to_remove) },
        }
    }
}
