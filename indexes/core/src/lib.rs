pub mod models;
pub mod notify;
pub mod reindexers;

#[cfg(test)]
mod test {
    
    use kaspa_consensus_core::BlockHashSet;
    
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::models::txindex::MergeSetIDX;
    use crate::models::utxoindex::{CirculatingSupply, CirculatingSupplyDiff};
    use crate::reindexers;
    use crate::reindexers::txindex::TxIndexReindexer;
    
    use kaspa_consensus_core::tx::TransactionId;
    use kaspa_consensus_core::{
        acceptance_data::{MergesetBlockAcceptanceData, TxEntry},
        config::params::Params,
        constants::MAX_SOMPI,
        network::NetworkType,
    };
    use kaspa_consensus_notify::notification::{VirtualChainChangedNotification, ChainAcceptanceDataPrunedNotification};
    use kaspa_hashes::Hash;

    #[test]
    fn test_mergest_idx_max() {
        NetworkType::iter().for_each(|network_type| {
            assert!(Params::from(network_type).mergeset_size_limit <= MergeSetIDX::MAX as u64);
        });
    }

    #[test]
    fn test_circulating_supply_max() {
        assert!(MAX_SOMPI <= CirculatingSupply::MAX);
    }

    #[test]
    fn test_circulating_supply_diff_max() {
        assert!(MAX_SOMPI <= CirculatingSupplyDiff::MAX as u64);
    }

    #[test]
    fn test_txindex_reindexer_from_vspcc_notification() {
        let block_a = Hash::from_u64_word(1); // unaccpeted
        let block_aa @ block_hh = Hash::from_u64_word(2); // remerged
        let block_b = Hash::from_u64_word(3); // unaccpeted

        let block_h = Hash::from_u64_word(4); // new accepted
                                              // block hh is already defined above, with block aa.
        let block_i @ sink = Hash::from_u64_word(5); // new accepted

        // Txs removed:
        let tx_a_1 = Hash::from_u64_word(6); // accepted in block a, not reaccepted
        let tx_aa_2 = Hash::from_u64_word(7); // accepted in block aa, not reaccepted
        let tx_b_3 = Hash::from_u64_word(8); // accepted in block bb, not reaccepted

        // Txs ReAdded:

        let tx_a_2 @ tx_h_1 = Hash::from_u64_word(9); // accepted in block a, reaccepted in block h
        let tx_a_3 @ tx_i_4 = Hash::from_u64_word(10); // accepted in block a, reaccepted in block i
        let tx_a_4 @ tx_hh_3 = Hash::from_u64_word(11); // accepted in block a, reaccepted in block hh
        let tx_aa_1 @ tx_h_2 = Hash::from_u64_word(12); // accepted in block aa, reaccepted in block_h
        let tx_aa_3 @ tx_i_1 = Hash::from_u64_word(13); // accepted in block aa, reaccepted in block_i
        let tx_aa_4 @ tx_hh_4 = Hash::from_u64_word(14); // accepted in block aa, reaccepted in block_hh
        let tx_b_1 @ tx_h_3 = Hash::from_u64_word(15); // accepted in block b, reaccepted in block_h
        let tx_b_2 @ tx_i_2 = Hash::from_u64_word(16); // accepted in block b, reaccepted in block_i
        let tx_b_4 @ tx_hh_1 = Hash::from_u64_word(17); // accepted in block b, reaccepted in block_hh

        // Txs added:
        let tx_h_4 = Hash::from_u64_word(18); // not accepted, accepted in block h.
        let tx_hh_2 = Hash::from_u64_word(19); // not accepted, accepted in block hh.
        let tx_i_3 = Hash::from_u64_word(20); // not accepted, accepted in block i.

        // Define sets accordingly:

        let unaccepted_blocks = BlockHashSet::from_iter([block_a, block_b].into_iter());
        let reaccepted_blocks = BlockHashSet::from_iter([block_aa, block_hh].into_iter());
        let accepted_blocks = BlockHashSet::from_iter([block_h, block_i].into_iter());

        let block_a_transactions = HashSet::<TransactionId>::from([tx_a_1, tx_a_2, tx_a_3, tx_a_4]);
        let block_aa_transactions = HashSet::<TransactionId>::from([tx_aa_1, tx_aa_2, tx_aa_3, tx_aa_4]);
        let block_b_transactions = HashSet::<TransactionId>::from([tx_b_1, tx_b_2, tx_b_3, tx_b_4]);
        let block_h_transactions = HashSet::<TransactionId>::from([tx_h_1, tx_h_2, tx_h_3, tx_h_4]);
        let block_hh_transactions = HashSet::<TransactionId>::from([tx_hh_1, tx_hh_2, tx_hh_3, tx_hh_4]);
        let block_i_transactions = HashSet::<TransactionId>::from([tx_i_1, tx_i_2, tx_i_3, tx_i_4]);

        let unaccepted_transactions = HashSet::<TransactionId>::from_iter(
            block_a_transactions.iter().cloned().chain(block_aa_transactions.iter().cloned()).chain(block_b_transactions.iter().cloned()).filter(|tx_id| {
                !(block_h_transactions.contains(tx_id)
                    || block_hh_transactions.contains(tx_id)
                    || block_i_transactions.contains(tx_id))
            })
        );
        let reaccepted_transactions = HashSet::<TransactionId>::from_iter(
            block_h_transactions
                .iter().cloned()
                .chain(block_hh_transactions.iter().cloned())
                .chain(block_i_transactions.iter().cloned())
                .filter(|tx_id| !unaccepted_transactions.contains(tx_id))
        );
        let accepted_transactions = HashSet::<TransactionId>::from_iter(
            block_h_transactions
                .into_iter()
                .chain(block_hh_transactions.iter().cloned())
                .chain(block_i_transactions.iter().cloned())
                .filter(|tx_id| !reaccepted_transactions.contains(tx_id))
        );

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
        assert_eq!(reindexer.new_sink.unwrap(), sink);
        assert!(reindexer.source.is_none());

        // Check the accepted and reaccepted Offsets:
        let mut block_acceptance_offsets_added_count = 0;
        let mut tx_offsets_added_count = 0;
        for (accepting_block_hash,acceptance_data) in test_vspcc_notification.added_chain_block_hashes.iter().cloned().zip(test_vspcc_notification.added_chain_blocks_acceptance_data.iter().cloned()) {
            for (mergeset_idx, mergeset) in acceptance_data.iter().enumerate() {
                assert!((accepted_blocks.contains(&mergeset.block_hash) || reaccepted_blocks.contains(&mergeset.block_hash)));
                assert!(!unaccepted_blocks.contains(&mergeset.block_hash));
                assert!(!reindexer.block_acceptance_offsets_changes.removed.contains(&mergeset.block_hash));
                let block_acceptance_offset = reindexer.block_acceptance_offsets_changes.added.get(&mergeset.block_hash).unwrap();
                assert_eq!(block_acceptance_offset.accepting_block(), accepting_block_hash);
                assert_eq!(block_acceptance_offset.ordered_mergeset_index(), mergeset_idx as MergeSetIDX);
                block_acceptance_offsets_added_count += 1;
                tx_offsets_added_count += mergeset.accepted_transactions.len();
                for accepted_tx_entry in mergeset.accepted_transactions.iter() {
                    assert!(accepted_transactions.contains(&accepted_tx_entry.transaction_id) || reaccepted_transactions.contains(&accepted_tx_entry.transaction_id));
                    assert!(!unaccepted_transactions.contains(&accepted_tx_entry.transaction_id));
                    assert!(!reindexer.tx_offset_changes.removed.contains(&accepted_tx_entry.transaction_id));
                    let tx_offset = reindexer.tx_offset_changes.added.get(&accepted_tx_entry.transaction_id).unwrap();
                    assert_eq!(mergeset.block_hash, tx_offset.including_block());
                    assert_eq!(accepted_tx_entry.index_within_block, tx_offset.transaction_index());
                }
            }
        }
        assert_eq!(block_acceptance_offsets_added_count, reindexer.block_acceptance_offsets_changes.added.len());
        assert_eq!(tx_offsets_added_count, reindexer.tx_offset_changes.added.len());

        // Test Unaccepted Txs:
        let mut tx_offsets_removed_count = 0;
        let mut block_acceptance_offsets_removed_count = 0;
        for (_, acceptance_data) in test_vspcc_notification.removed_chain_block_hashes.iter().zip(test_vspcc_notification.removed_chain_blocks_acceptance_data.iter()) {
            for (_, mergeset) in acceptance_data.iter().enumerate() {
                if unaccepted_blocks.contains(&mergeset.block_hash) || reaccepted_blocks.contains(&mergeset.block_hash) {
                    assert!(!accepted_blocks.contains(&mergeset.block_hash));
                    if reaccepted_blocks.contains(&mergeset.block_hash) {
                        assert!(!reindexer.block_acceptance_offsets_changes.removed.contains(&mergeset.block_hash));
                    } else if unaccepted_blocks.contains(&mergeset.block_hash) {
                        assert!(reindexer.block_acceptance_offsets_changes.removed.contains(&mergeset.block_hash));
                        block_acceptance_offsets_removed_count += 1;
                    };
                    for accepted_tx_entry in mergeset.accepted_transactions.iter() {
                        if unaccepted_transactions.contains(&accepted_tx_entry.transaction_id) {
                            assert!(!(accepted_transactions.contains(&accepted_tx_entry.transaction_id) || reaccepted_transactions.contains(&accepted_tx_entry.transaction_id)));
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

        let chain_block_a_pruned = Hash::from_u64_word(1);
        let mergeset_block_b_pruned = Hash::from_u64_word(2);
        let mergeset_block_c_pruned = Hash::from_u64_word(2);

        let history_root = Hash::from_u64_word(3);

        let tx_a_1 = Hash::from_u64_word(4);
        let tx_a_2 = Hash::from_u64_word(5);
        let tx_b_1 = Hash::from_u64_word(6);
        let tx_b_2 = Hash::from_u64_word(7);
        let tx_c_1 = Hash::from_u64_word(8);
        let tx_c_2 = Hash::from_u64_word(9);

        let test_chain_acceptance_data_pruned_notification = ChainAcceptanceDataPrunedNotification {
            chain_hash_pruned: chain_block_a_pruned,
            mergeset_block_acceptance_data_pruned: Arc::new(vec![
                MergesetBlockAcceptanceData {
                    block_hash: mergeset_block_b_pruned,
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
            history_root: Hash::from_u64_word(9),
        };

        let reindexer = TxIndexReindexer::from(test_chain_acceptance_data_pruned_notification.clone());

        assert!(reindexer.block_acceptance_offsets_changes.added.is_empty());
        assert!(reindexer.tx_offset_changes.added.is_empty());
        
        
        for mergeset in test_chain_acceptance_data_pruned_notification.mergeset_block_acceptance_data_pruned.iter().cloned() {
            assert!(reindexer.block_acceptance_offsets_changes.removed.contains(&mergeset.block_hash));
            for accepted_tx_entry in mergeset.accepted_transactions.iter() {
                assert!(reindexer.tx_offset_changes.removed.contains(&accepted_tx_entry.transaction_id));
            }
        }

    }
}
