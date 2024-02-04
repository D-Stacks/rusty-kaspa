use crate::models::txindex::{
    AcceptanceDataIndexType, BlockAcceptanceOffset, BlockAcceptanceOffsetDiff, TxHashSet, TxOffset, TxOffsetById, TxOffsetDiff,
};
use kaspa_consensus_core::{BlockHashMap, BlockHashSet, HashMapCustomHasher};
use kaspa_consensus_notify::notification::{
    ChainAcceptanceDataPrunedNotification as ConsensusChainAcceptanceDataPrunedNotification,
    VirtualChainChangedNotification as ConsensusVirtualChainChangedNotification,
};
use kaspa_hashes::Hash;
use kaspa_utils::arc::ArcExtensions;

/// Reindexes a [`ConsensusVirtualChainChangedNotification`] to txindex diffs, alongside new source and sink [`Hash`], this includes the calculated [`BlockAcceptanceOffsetDiff`] and [`TxOffsetDiff`].
#[derive(Clone, Debug, Default)]
pub struct TxIndexReindexer {
    pub sink: Option<Hash>,
    pub source: Option<Hash>,
    pub block_acceptance_offsets_changes: BlockAcceptanceOffsetDiff,
    pub tx_offset_changes: TxOffsetDiff,
}

impl From<ConsensusVirtualChainChangedNotification> for TxIndexReindexer {
    fn from(vspcc_notification: ConsensusVirtualChainChangedNotification) -> Self {
        let sink = vspcc_notification.added_chain_block_hashes.last().copied();

        drop(vspcc_notification.removed_chain_block_hashes); // we do not require this anymore.

        let mut tx_offsets_to_add = TxOffsetById::new();
        let mut tx_offsets_to_remove = TxHashSet::new();
        let mut block_acceptance_offsets_to_add =
            BlockHashMap::<BlockAcceptanceOffset>::with_capacity(vspcc_notification.added_chain_blocks_acceptance_data.len());
        let mut block_acceptance_offsets_to_remove =
            BlockHashSet::with_capacity(vspcc_notification.removed_chain_blocks_acceptance_data.len());

        for (accepting_block_hash, acceptance_data) in vspcc_notification
            .added_chain_block_hashes
            .unwrap_or_clone()
            .into_iter()
            .zip(vspcc_notification.added_chain_blocks_acceptance_data.unwrap_or_clone().into_iter())
        {
            for (i, mergeset_block_acceptance_datum) in acceptance_data.unwrap_or_clone().into_iter().enumerate() {
                tx_offsets_to_add.extend(mergeset_block_acceptance_datum.accepted_transactions.into_iter().map(|tx_entry| {
                    (tx_entry.transaction_id, TxOffset::new(mergeset_block_acceptance_datum.block_hash, tx_entry.index_within_block))
                }));

                block_acceptance_offsets_to_add.insert(
                    mergeset_block_acceptance_datum.block_hash,
                    BlockAcceptanceOffset::new(accepting_block_hash, i as AcceptanceDataIndexType),
                );
            }
        }

        for acceptance_data in vspcc_notification.removed_chain_blocks_acceptance_data.unwrap_or_clone().into_iter() {
            for mergeset_block_acceptance_datum in acceptance_data.unwrap_or_clone().into_iter() {
                tx_offsets_to_remove.extend(
                    mergeset_block_acceptance_datum
                        .accepted_transactions
                        .into_iter()
                        .filter(|tx_entry| !tx_offsets_to_add.contains_key(&tx_entry.transaction_id))
                        .map(|tx_entry| tx_entry.transaction_id),
                );

                if !block_acceptance_offsets_to_add.contains_key(&mergeset_block_acceptance_datum.block_hash) {
                    block_acceptance_offsets_to_remove.insert(mergeset_block_acceptance_datum.block_hash);
                };
            }
        }

        Self {
            sink,
            source: None,
            block_acceptance_offsets_changes: BlockAcceptanceOffsetDiff::new(
                block_acceptance_offsets_to_add,
                block_acceptance_offsets_to_remove,
            ),
            tx_offset_changes: TxOffsetDiff::new(tx_offsets_to_add, tx_offsets_to_remove),
        }
    }
}

impl From<ConsensusChainAcceptanceDataPrunedNotification> for TxIndexReindexer {
    fn from(notification: ConsensusChainAcceptanceDataPrunedNotification) -> Self {
        let mut tx_offsets_to_remove = TxHashSet::new();
        let mut block_acceptance_offsets_to_remove =
            BlockHashSet::with_capacity(notification.mergeset_block_acceptance_data_pruned.mergeset.len());

        for mergeset_block_acceptance_datum in notification.mergeset_block_acceptance_data_pruned.unwrap_or_clone().into_iter() {
            tx_offsets_to_remove
                .extend(mergeset_block_acceptance_datum.accepted_transactions.into_iter().map(|tx_entry| tx_entry.transaction_id));
            block_acceptance_offsets_to_remove.insert(mergeset_block_acceptance_datum.block_hash);
        }

        Self {
            sink: None,
            source: Some(notification.source),
            block_acceptance_offsets_changes: BlockAcceptanceOffsetDiff::new(BlockHashMap::new(), block_acceptance_offsets_to_remove),
            tx_offset_changes: TxOffsetDiff::new(TxOffsetById::new(), tx_offsets_to_remove),
        }
    }
}

#[cfg(test)]
pub mod test {

    use kaspa_consensus_core::BlockHashSet;

    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::models::txindex::AcceptanceDataIndexType;
    use crate::reindexers::txindex::TxIndexReindexer;

    use kaspa_consensus_core::acceptance_data::{MergesetBlockAcceptanceData, TxEntry};
    use kaspa_consensus_core::tx::TransactionId;
    use kaspa_consensus_notify::notification::{ChainAcceptanceDataPrunedNotification, VirtualChainChangedNotification};
    use kaspa_hashes::Hash;

    #[test]
    fn test_txindex_reindexer_from_virtual_chain_changed_notification() {
        // Define the block hashes:

        // Blocks removed (i.e. unaccepted):
        let block_a = Hash::from_u64_word(1);
        let block_b = Hash::from_u64_word(2);

        // Blocks ReAdded (i.e. reaccepted):
        let block_aa @ block_hh = Hash::from_u64_word(3);

        // Blocks Added (i.e. newly reaccepted):
        let block_h = Hash::from_u64_word(4);
        let block_i @ sink = Hash::from_u64_word(5);

        // Define the tx ids;

        // Txs removed (i.e. unaccepted)):
        let tx_a_1 = TransactionId::from_u64_word(6); // accepted in block a, not reaccepted
        let tx_aa_2 = TransactionId::from_u64_word(7); // accepted in block aa, not reaccepted
        let tx_b_3 = TransactionId::from_u64_word(8); // accepted in block bb, not reaccepted

        // Txs ReAdded (i.e. reaccepted)):
        let tx_a_2 @ tx_h_1 = TransactionId::from_u64_word(9); // accepted in block a, reaccepted in block h
        let tx_a_3 @ tx_i_4 = TransactionId::from_u64_word(10); // accepted in block a, reaccepted in block i
        let tx_a_4 @ tx_hh_3 = TransactionId::from_u64_word(11); // accepted in block a, reaccepted in block hh
        let tx_aa_1 @ tx_h_2 = TransactionId::from_u64_word(12); // accepted in block aa, reaccepted in block_h
        let tx_aa_3 @ tx_i_1 = TransactionId::from_u64_word(13); // accepted in block aa, reaccepted in block_i
        let tx_aa_4 @ tx_hh_4 = TransactionId::from_u64_word(14); // accepted in block aa, reaccepted in block_hh
        let tx_b_1 @ tx_h_3 = TransactionId::from_u64_word(15); // accepted in block b, reaccepted in block_h
        let tx_b_2 @ tx_i_2 = TransactionId::from_u64_word(16); // accepted in block b, reaccepted in block_i
        let tx_b_4 @ tx_hh_1 = TransactionId::from_u64_word(17); // accepted in block b, reaccepted in block_hh

        // Txs added (i.e. newly accepted)):
        let tx_h_4 = TransactionId::from_u64_word(18); // not originally accepted, accepted in block h.
        let tx_hh_2 = TransactionId::from_u64_word(19); // not originally accepted, accepted in block hh.
        let tx_i_3 = TransactionId::from_u64_word(20); // not originally accepted, accepted in block i.

        // Define sets accordingly:

        // Define the block hashes into unaccepted / accepted / reaccepted sets:
        let unaccepted_blocks = BlockHashSet::from_iter([block_a, block_b]);
        let reaccepted_blocks = BlockHashSet::from_iter([block_aa, block_hh]);
        let accepted_blocks = BlockHashSet::from_iter([block_h, block_i]);

        // Define the tx hashes into block sets:
        let block_a_transactions = HashSet::<TransactionId>::from([tx_a_1, tx_a_2, tx_a_3, tx_a_4]);
        let block_aa_transactions = HashSet::<TransactionId>::from([tx_aa_1, tx_aa_2, tx_aa_3, tx_aa_4]);
        let block_b_transactions = HashSet::<TransactionId>::from([tx_b_1, tx_b_2, tx_b_3, tx_b_4]);
        let block_h_transactions = HashSet::<TransactionId>::from([tx_h_1, tx_h_2, tx_h_3, tx_h_4]);
        let block_hh_transactions = HashSet::<TransactionId>::from([tx_hh_1, tx_hh_2, tx_hh_3, tx_hh_4]);
        let block_i_transactions = HashSet::<TransactionId>::from([tx_i_1, tx_i_2, tx_i_3, tx_i_4]);

        // Define the tx hashes into unaccepted / accepted / reaccepted sets:
        let unaccepted_transactions = HashSet::<TransactionId>::from_iter(
            block_a_transactions
                .iter()
                .cloned()
                .chain(block_aa_transactions.iter().cloned())
                .chain(block_b_transactions.iter().cloned())
                .filter(|tx_id| {
                    !(block_h_transactions.contains(tx_id)
                        || block_hh_transactions.contains(tx_id)
                        || block_i_transactions.contains(tx_id))
                }),
        );
        let reaccepted_transactions = HashSet::<TransactionId>::from_iter(
            block_h_transactions
                .iter()
                .cloned()
                .chain(block_hh_transactions.iter().cloned())
                .chain(block_i_transactions.iter().cloned())
                .filter(|tx_id| !unaccepted_transactions.contains(tx_id)),
        );
        let accepted_transactions = HashSet::<TransactionId>::from_iter(
            block_h_transactions
                .into_iter()
                .chain(block_hh_transactions.iter().cloned())
                .chain(block_i_transactions.iter().cloned())
                .filter(|tx_id| !reaccepted_transactions.contains(tx_id)),
        );

        // Define the notification:
        let test_vspcc_notification = VirtualChainChangedNotification {
            added_chain_block_hashes: Arc::new(vec![block_h, block_i]),
            added_chain_blocks_acceptance_data: Arc::new(vec![
                Arc::new(vec![
                    MergesetBlockAcceptanceData {
                        block_hash: block_h,
                        accepted_transactions: vec![
                            TxEntry { transaction_id: tx_h_1, index_within_block: 0 },
                            TxEntry { transaction_id: tx_h_2, index_within_block: 1 },
                            TxEntry { transaction_id: tx_h_3, index_within_block: 2 },
                            TxEntry { transaction_id: tx_h_4, index_within_block: 4 },
                        ],
                    },
                    MergesetBlockAcceptanceData {
                        block_hash: block_hh,
                        accepted_transactions: vec![
                            TxEntry { transaction_id: tx_hh_1, index_within_block: 0 },
                            TxEntry { transaction_id: tx_hh_2, index_within_block: 1 },
                            TxEntry { transaction_id: tx_hh_3, index_within_block: 2 },
                            TxEntry { transaction_id: tx_hh_4, index_within_block: 3 },
                        ],
                    },
                ]),
                Arc::new(vec![MergesetBlockAcceptanceData {
                    block_hash: block_i,
                    accepted_transactions: vec![
                        TxEntry { transaction_id: tx_i_1, index_within_block: 0 },
                        TxEntry { transaction_id: tx_i_2, index_within_block: 1 },
                        TxEntry { transaction_id: tx_i_3, index_within_block: 2 },
                        TxEntry { transaction_id: tx_i_4, index_within_block: 3 },
                    ],
                }]),
            ]),
            removed_chain_block_hashes: Arc::new(vec![block_a, block_b]),
            removed_chain_blocks_acceptance_data: Arc::new(vec![
                Arc::new(vec![
                    MergesetBlockAcceptanceData {
                        block_hash: block_a,
                        accepted_transactions: vec![
                            TxEntry { transaction_id: tx_a_1, index_within_block: 0 },
                            TxEntry { transaction_id: tx_a_2, index_within_block: 1 },
                            TxEntry { transaction_id: tx_a_3, index_within_block: 2 },
                            TxEntry { transaction_id: tx_a_4, index_within_block: 3 },
                        ],
                    },
                    MergesetBlockAcceptanceData {
                        block_hash: block_aa,
                        accepted_transactions: vec![
                            TxEntry { transaction_id: tx_aa_1, index_within_block: 0 },
                            TxEntry { transaction_id: tx_aa_2, index_within_block: 1 },
                            TxEntry { transaction_id: tx_aa_3, index_within_block: 2 },
                            TxEntry { transaction_id: tx_aa_4, index_within_block: 3 },
                        ],
                    },
                ]),
                Arc::new(vec![MergesetBlockAcceptanceData {
                    block_hash: block_b,
                    accepted_transactions: vec![
                        TxEntry { transaction_id: tx_b_1, index_within_block: 0 },
                        TxEntry { transaction_id: tx_b_2, index_within_block: 1 },
                        TxEntry { transaction_id: tx_b_3, index_within_block: 2 },
                        TxEntry { transaction_id: tx_b_4, index_within_block: 3 },
                    ],
                }]),
            ]),
        };

        // Reindex
        let reindexer = TxIndexReindexer::from(test_vspcc_notification.clone());

        // Check the new_sink and source:
        assert_eq!(reindexer.sink.unwrap(), sink);
        assert!(reindexer.source.is_none());

        // Check the added offsets (i.e. accepted & reaccepted):
        let mut block_acceptance_offsets_added_count = 0;
        let mut tx_offsets_added_count = 0;
        for (accepting_block_hash, acceptance_data) in test_vspcc_notification
            .added_chain_block_hashes
            .iter()
            .cloned()
            .zip(test_vspcc_notification.added_chain_blocks_acceptance_data.iter().cloned())
        {
            for (mergeset_idx, mergeset) in acceptance_data.iter().enumerate() {
                assert!((accepted_blocks.contains(&mergeset.block_hash) || reaccepted_blocks.contains(&mergeset.block_hash)));
                assert!(!unaccepted_blocks.contains(&mergeset.block_hash));
                assert!(!reindexer.block_acceptance_offsets_changes.removed.contains(&mergeset.block_hash));
                let block_acceptance_offset = reindexer.block_acceptance_offsets_changes.added.get(&mergeset.block_hash).unwrap();
                assert_eq!(block_acceptance_offset.accepting_block, accepting_block_hash);
                assert_eq!(block_acceptance_offset.acceptance_data_index, mergeset_idx as AcceptanceDataIndexType);
                block_acceptance_offsets_added_count += 1;
                tx_offsets_added_count += mergeset.accepted_transactions.len();
                for accepted_tx_entry in mergeset.accepted_transactions.iter() {
                    assert!(
                        accepted_transactions.contains(&accepted_tx_entry.transaction_id)
                            || reaccepted_transactions.contains(&accepted_tx_entry.transaction_id)
                    );
                    assert!(!unaccepted_transactions.contains(&accepted_tx_entry.transaction_id));
                    assert!(!reindexer.tx_offset_changes.removed.contains(&accepted_tx_entry.transaction_id));
                    let tx_offset = reindexer.tx_offset_changes.added.get(&accepted_tx_entry.transaction_id).unwrap();
                    assert_eq!(mergeset.block_hash, tx_offset.including_block);
                    assert_eq!(accepted_tx_entry.index_within_block, tx_offset.transaction_index);
                }
            }
        }
        assert_eq!(block_acceptance_offsets_added_count, reindexer.block_acceptance_offsets_changes.added.len());
        assert_eq!(tx_offsets_added_count, reindexer.tx_offset_changes.added.len());

        // Check removed offsets (i.e. unaccepted):
        let mut tx_offsets_removed_count = 0;
        let mut block_acceptance_offsets_removed_count = 0;
        for acceptance_data in test_vspcc_notification.removed_chain_blocks_acceptance_data.iter() {
            for mergeset_block_acceptance_datum in acceptance_data.iter() {
                if unaccepted_blocks.contains(&mergeset_block_acceptance_datum.block_hash)
                    || reaccepted_blocks.contains(&mergeset_block_acceptance_datum.block_hash)
                {
                    assert!(!accepted_blocks.contains(&mergeset_block_acceptance_datum.block_hash));
                    if reaccepted_blocks.contains(&mergeset_block_acceptance_datum.block_hash) {
                        assert!(!reindexer
                            .block_acceptance_offsets_changes
                            .removed
                            .contains(&mergeset_block_acceptance_datum.block_hash));
                    } else if unaccepted_blocks.contains(&mergeset_block_acceptance_datum.block_hash) {
                        assert!(reindexer
                            .block_acceptance_offsets_changes
                            .removed
                            .contains(&mergeset_block_acceptance_datum.block_hash));
                        block_acceptance_offsets_removed_count += 1;
                    };
                    for accepted_tx_entry in mergeset_block_acceptance_datum.accepted_transactions.iter() {
                        if unaccepted_transactions.contains(&accepted_tx_entry.transaction_id) {
                            assert!(
                                !(accepted_transactions.contains(&accepted_tx_entry.transaction_id)
                                    || reaccepted_transactions.contains(&accepted_tx_entry.transaction_id))
                            );
                            assert!(reindexer.tx_offset_changes.removed.contains(&accepted_tx_entry.transaction_id));
                            tx_offsets_removed_count += 1;
                        }
                    }
                }
            }
        }
        assert_eq!(block_acceptance_offsets_removed_count, reindexer.block_acceptance_offsets_changes.removed.len());
        assert_eq!(tx_offsets_removed_count, reindexer.tx_offset_changes.removed.len());
    }

    #[test]
    fn test_txindex_reindexer_from_chain_acceptance_data_pruned() {
        // Define the block hashes:
        let chain_block_a_pruned = Hash::from_u64_word(1);
        let mergeset_block_b_pruned = Hash::from_u64_word(2);
        let mergeset_block_c_pruned = Hash::from_u64_word(3);

        let source = Hash::from_u64_word(4);

        // Define the tx ids;
        let tx_a_1 = TransactionId::from_u64_word(5);
        let tx_a_2 = TransactionId::from_u64_word(6);
        let tx_b_1 = TransactionId::from_u64_word(7);
        let tx_b_2 = TransactionId::from_u64_word(8);
        let tx_c_1 = TransactionId::from_u64_word(9);
        let tx_c_2 = TransactionId::from_u64_word(10);

        // Define the notification:
        let test_chain_acceptance_data_pruned_notification = ChainAcceptanceDataPrunedNotification {
            chain_hash_pruned: chain_block_a_pruned,
            mergeset_block_acceptance_data_pruned: Arc::new(vec![
                MergesetBlockAcceptanceData {
                    block_hash: chain_block_a_pruned,
                    accepted_transactions: vec![
                        TxEntry { transaction_id: tx_a_1, index_within_block: 0 },
                        TxEntry { transaction_id: tx_a_2, index_within_block: 1 },
                    ],
                },
                MergesetBlockAcceptanceData {
                    block_hash: mergeset_block_b_pruned,
                    accepted_transactions: vec![
                        TxEntry { transaction_id: tx_b_1, index_within_block: 0 },
                        TxEntry { transaction_id: tx_b_2, index_within_block: 1 },
                    ],
                },
                MergesetBlockAcceptanceData {
                    block_hash: mergeset_block_c_pruned,
                    accepted_transactions: vec![
                        TxEntry { transaction_id: tx_c_1, index_within_block: 0 },
                        TxEntry { transaction_id: tx_c_2, index_within_block: 1 },
                    ],
                },
            ]),
            source,
        };

        // Reindex
        let reindexer = TxIndexReindexer::from(test_chain_acceptance_data_pruned_notification.clone());

        // Check the sink and source:
        assert!(reindexer.sink.is_none());
        assert_eq!(reindexer.source.unwrap(), source);

        // Check the added offsets:
        assert!(reindexer.block_acceptance_offsets_changes.added.is_empty());
        assert!(reindexer.tx_offset_changes.added.is_empty());

        // Check removed offsets:
        let mut tx_offsets_removed_count = 0;
        let mut block_acceptance_offsets_removed_count = 0;
        for mergeset_block_acceptance_datum in
            test_chain_acceptance_data_pruned_notification.mergeset_block_acceptance_data_pruned.iter().cloned()
        {
            assert!(reindexer.block_acceptance_offsets_changes.removed.contains(&mergeset_block_acceptance_datum.block_hash));
            block_acceptance_offsets_removed_count += 1;
            tx_offsets_removed_count += mergeset_block_acceptance_datum.accepted_transactions.len();
            for accepted_tx_entry in mergeset_block_acceptance_datum.accepted_transactions.iter() {
                assert!(reindexer.tx_offset_changes.removed.contains(&accepted_tx_entry.transaction_id));
            }
        }
        assert_eq!(block_acceptance_offsets_removed_count, reindexer.block_acceptance_offsets_changes.removed.len());
        assert_eq!(tx_offsets_removed_count, reindexer.tx_offset_changes.removed.len());
    }
}
