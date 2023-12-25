use crate::{
    errors::{TxIndexError, TxIndexResult},
    model::{TxOffset, BlockAcceptanceOffset},
};
use kaspa_consensus::processes::ghostdag::mergeset;
use kaspa_consensus_core::{
    acceptance_data::{AcceptanceData, TxEntry, MergesetBlockAcceptanceData},
    block::Block,
    tx::{Transaction, TransactionId, TransactionIndexType},
    BlockHashMap, HashMapCustomHasher, BlockHashSet, errors::tx,
};
use kaspa_consensus_notify::notification::{VirtualChainChangedNotification, BlockAddedNotification};
use kaspa_hashes::Hash;
use std::{sync::Arc, collections::{HashMap, HashSet}};

use super::BlockAcceptanceOffset;

pub struct TxIndexBlockRemovedChanges {
    pub offsets_removed: Arc<Vec<TransactionId>>,
    pub block_removed: Hash,
    pub history_root: Hash,
}

impl TxIndexBlockRemovedChanges {
    fn new(offsets_removed: Arc<Vec<TransactionId>>, block_removed: Hash, history_root: Hash) -> Self {
        Self { offsets_removed, block_removed, history_root }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TxOffsetChanges {
    pub to_remove: Arc<HashMap<TransactionId, TxOffset>>,
    pub to_add: Arc<HashSet<TransactionId>>,
}

#[derive(Debug, Clone, Default)]
pub struct BlockAcceptanceOffsetsChanges {
    pub to_add: Arc<BlockHashMap<Hash, TxOffset>>,
    pub to_remove: Arc<BlockHashSet>,
}

pub struct TxIndexVSPCCReindexer {
    pub block_acceptance_offsets_changes: BlockAcceptanceOffsetsChanges,
    pub tx_offset_changes: TxOffsetChanges,
    pub new_sink: Hash,
}

impl TxIndexVSPCCChanges {
    pub fn new(block_acceptance_offsets_changes: BlockAcceptanceOffsetsChanges, tx_offset_changes: TxOffsetChanges, new_sink: Hash) -> Self {
        Self { block_acceptance_offsets_changes, tx_offset_changes, new_sink }
    }
}

impl From<VirtualChainChangedNotification> for TxIndexVSPCCChanges {
    fn from(vspcc_notification: VirtualChainChangedNotification) -> Self {
        let tx_offsets_accepted = HashMap::new();
        let tx_offsets_unaccepted = HashSet::new();
        Self { 
            block_acceptance_offsets_added: BlockAcceptanceOffsetsChanges {
            
            to_add: vspcc_notification
                .added_chain_block_hashes
                .into_iter()
                .zip(vspcc_notification.added_chain_blocks_acceptance_data.into_iter())
                .map(|(accepting_block_hash, acceptance_data)| {
                    acceptance_data
                        .into_iter()
                        .enumerate()
                        .map(|(i, mergeset)| {
                            tx_offsets_accepted.extend(
                                mergeset
                                    .accepted_transactions
                                    .into_iter()
                                    .map(|tx_entry| (
                                        tx_entry.transaction_id,
                                        TxOffset::new(mergeset.block_hash, tx_entry.index_within_block),
                                    )),
                            );
                            (mergeset.block_hash, (accepting_block_hash, i))
                        })
                        .collect()
                }), 
            
            to_remove: vspcc_notification
                .removed_chain_block_hashes
                .into_iter()
                .zip(vspcc_notification.removed_chain_blocks_acceptance_data.into_iter())
                .map(|(accepting_block_hash, acceptance_data)| {
                    acceptance_data
                        .into_iter()
                        .map(|mergeset| {
                            tx_offsets_unaccepted.extend(
                                mergeset
                                    .accepted_transactions
                                    .into_iter()
                                    .filter_map(|tx_entry| {
                                        if tx_offsets_accepted.contains_key(&tx_entry.transaction_id) {
                                            None
                                        } else {
                                            Some(tx_id)
                                        }
                                    }),
                            );
                            (mergeset.block_hash, accepting_block_hash)
                        })
                        .collect()
                }),
        },
        
        tx_offset_changes: TxOffsetChanges {
            to_remove: tx_offsets_accepted,
            to_add: tx_offsets_unaccepted,
        },
        
        new_sink: vspcc_notification.added_chain_block_hashes.last().unwrap()
     }
    }
}