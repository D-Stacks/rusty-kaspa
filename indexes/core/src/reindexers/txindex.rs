use crate::models::{
    BlockAcceptanceOffsetDiff, TxOffsetDiff, BlockAcceptanceOffsetByHash, BlockAcceptanceOffset, TxOffsetByTyId, TransactionHashSet, TxOffset, TransactionHashMap,
};
use kaspa_consensus_core::{
    tx::{TransactionId, TransactionIndexType},
    BlockHashMap, BlockHashSet, HashMapCustomHasher,
};
use kaspa_hashes::Hash;
use kaspa_consensus_notify::notification::{VirtualChainChangedNotification as ConsensusVirtualChainChangedNotification, ChainAcceptanceDataPrunedNotification as ConsensusChainAcceptanceDataPrunedNotification, Notification as ConsensusNotification};
use kaspa_utils::arc::ArcExtensions;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Reindexes a [`ConsensusNotification`] to txindex diffs, alongside new source and sink [`Hash`], this includes the calculated [`BlockAcceptanceOffsetDiff`] and [`TxOffsetDiff`]. 
#[derive(Clone, Debug, Default)]
pub struct TxIndexReindexer {
    pub new_sink: Option<Hash>,
    pub source: Option<Hash>,
    pub block_acceptance_offsets_changes: BlockAcceptanceOffsetDiff,
    pub tx_offset_changes: TxOffsetDiff,
}


impl From<ConsensusVirtualChainChangedNotification> for TxIndexReindexer {
    fn from(vspcc_notification: ConsensusVirtualChainChangedNotification) -> Self {
        let new_sink = match vspcc_notification.added_chain_block_hashes.last() {
            Some(new_sink) => *new_sink,
            None => *vspcc_notification
                .removed_chain_block_hashes
                .last()
                .expect("Expected at least one block to be added or removed, i.e. a none_empty chain path"),
        };

        drop(vspcc_notification.removed_chain_block_hashes); // we do not require this anymore.

        let mut tx_offsets_to_add = TxOffsetByTyId::new();
        let mut tx_offsets_to_remove = TransactionHashSet::new();
        let mut block_acceptance_offsets_to_add = BlockAcceptanceOffsetByHash::new();
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
                    .insert(mergeset.block_hash, BlockAcceptanceOffset::new(accepting_block_hash, i as MergeSetIDX));
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
            source: None,
            block_acceptance_offsets_changes: BlockAcceptanceOffsetsChanges::new(
                Arc::new(block_acceptance_offsets_to_add),
                Arc::new(block_acceptance_offsets_to_remove),
            ),
            tx_offset_changes: TxOffsetChanges::new(
                Arc::new(tx_offsets_to_add), 
                Arc::new(tx_offsets_to_remove)
            ),
        }
    }
}

impl From<ConsensusChainAcceptanceDataPrunedNotification> for TxIndexReindexer {
    fn from(notification: ConsensusChainAcceptanceDataPrunedNotification) -> Self {
        let source = notification.new_pruning_point;
        let mut tx_offsets_to_remove = TransactionHashSet::new();
        let mut block_acceptance_offsets_to_remove = BlockHashSet::new();

        for acceptance_data in notification.chain_blocks_acceptance_data.unwrap_or_clone().into_iter() {
            for mergeset in acceptance_data.unwrap_or_clone().into_iter() {
                tx_offsets_to_remove.extend(
                    mergeset
                        .accepted_transactions
                        .into_iter()
                        .map(|tx_entry| tx_entry.transaction_id),
                );

                block_acceptance_offsets_to_remove.insert(mergeset.block_hash);
            }
        }

        Self {
            new_sink: None,
            source,
            block_acceptance_offsets_changes: BlockAcceptanceOffsetsChanges::new(
                Arc::new(BlockHashMap::default()),
                Arc::new(block_acceptance_offsets_to_remove),
            ),
            tx_offset_changes: TxOffsetChanges::new(
                Arc::new(HashMap::default()), 
                Arc::new(tx_offsets_to_remove) 
            ),
        }
    }
}
