use std::{fmt::Debug, sync::Arc};

use kaspa_consensus_core::tx::TransactionId;
use kaspa_consensus_notify::notification::{
    ChainAcceptanceDataPrunedNotification as ConsensusChainAcceptanceDataPrunedNotification,
    VirtualChainChangedNotification as ConsensusVirtualChainChangedNotification,
};
use kaspa_consensusmanager::{ConsensusManager, ConsensusSessionBlocking};
use kaspa_core::{error, info, trace};
use kaspa_database::prelude::DB;
use kaspa_hashes::Hash;
use kaspa_index_core::{
    models::txindex::{BlockAcceptanceOffset, TxOffset},
    reindexers::txindex::TxIndexReindexer,
};
use parking_lot::RwLock;
use rocksdb::WriteBatch;

use crate::{
    config::Config as TxIndexConfig,
    core::api::TxIndexApi,
    core::errors::TxIndexError,
    core::errors::TxIndexResult,
    stores::{
        TxIndexAcceptedTxOffsetsReader, TxIndexAcceptedTxOffsetsStore, TxIndexMergedBlockAcceptanceReader,
        TxIndexMergedBlockAcceptanceStore, TxIndexSinkReader, TxIndexSinkStore, TxIndexSourceReader, TxIndexSourceStore,
        TxIndexStores,
    },
};
pub struct TxIndex {
    stores: TxIndexStores,
    consensus_manager: Arc<ConsensusManager>,
    config: Arc<TxIndexConfig>, // move into config, once txindex is configurable.
}

impl TxIndex {
    pub fn new(consensus_manager: Arc<ConsensusManager>, db: Arc<DB>, config: Arc<TxIndexConfig>) -> TxIndexResult<Arc<RwLock<Self>>> {
        let mut txindex = Self { stores: TxIndexStores::new(db, &config)?, consensus_manager: consensus_manager.clone(), config };

        if !txindex.is_synced()? {
            match txindex.resync() {
                Ok(_) => {
                    info!("[{0:?}] Resync Successful", txindex);
                }
                Err(e) => {
                    error!("[{0:?}] Failed to resync: {1}", txindex, e);
                    txindex.stores.delete_all()?; // we try and delete all, in order to remove any partial data that may have been written.
                    return Err(e);
                }
            };
        };

        let txindex = Arc::new(RwLock::new(txindex));
        //consensus_manager.register_consensus_reset_handler(Arc::new(TxIndexConsensusResetHandler::new(Arc::downgrade(&txindex))));
        Ok(txindex)
    }

    // For internal usage only
    fn _resync_segement(
        &mut self,
        mut start_hash: Hash,
        end_hash: Hash,
        session: &ConsensusSessionBlocking<'_>,
        // Setting below to true will remove data added along the start_hash -> end_hash path.
        unsync_segment: bool,
    ) -> TxIndexResult<()> {
        info!("[{0:?}] Resyncing from {1} to {2}", self, start_hash, end_hash,);
        let split_hash = session.find_highest_common_chain_block(start_hash, end_hash)?;
        let split_daa_score = session.get_header(split_hash)?.daa_score;
        let total_blocks_to_remove = session.get_header(start_hash)?.daa_score - split_daa_score; // start_daa_score - split_daa_score;
        let total_blocks_to_add = session.get_header(end_hash)?.daa_score - split_daa_score; // end_daa_score - split_daa_score;

        let mut total_blocks_removed: u64 = 0u64;
        let mut total_blocks_added: u64 = 0u64;
        let mut removed_processed_in_batch = 0u64;
        let mut added_processed_in_batch = 0u64;

        // As the `session.get_virtual_chain_from_block` method is exclusive to the start-hash, under the added condition, we commit this separately..
        if session.is_chain_block(start_hash)? {
            self.update_via_virtual_chain_changed(ConsensusVirtualChainChangedNotification::new(
                Arc::new(vec![start_hash]),
                Arc::new(vec![]),
                Arc::new(vec![session.get_block_acceptance_data(start_hash)?]),
                Arc::new(vec![]),
            ))?;
        };
        while start_hash != end_hash {
            let mut chain_path =
                session.get_virtual_chain_from_block(start_hash, Some(end_hash), self.config.perf.resync_chunksize as usize)?;

            // We switch added to removed, and clear removed, as we have no use for the removed data.
            if unsync_segment {
                chain_path.removed = chain_path.added;
                chain_path.added = Arc::new(vec![]);
            }

            let removed_chain_blocks_acceptance_data = if !chain_path.removed.is_empty() {
                session.get_blocks_acceptance_data(chain_path.added.clone())?
            } else {
                Arc::new(vec![])
            };
            removed_processed_in_batch += chain_path.added.len() as u64;

            let added_chain_blocks_acceptance_data = if !chain_path.added.is_empty() {
                session.get_blocks_acceptance_data(chain_path.removed.clone())?
            } else {
                Arc::new(vec![])
            };

            removed_processed_in_batch += chain_path.added.len() as u64;
            added_processed_in_batch += chain_path.removed.len() as u64;

            start_hash = chain_path.checkpoint_hash();

            let vspcc_notification = ConsensusVirtualChainChangedNotification::new(
                chain_path.added,
                chain_path.removed,
                added_chain_blocks_acceptance_data,
                removed_chain_blocks_acceptance_data,
            );

            self.update_via_virtual_chain_changed(vspcc_notification)?;

            if removed_processed_in_batch != 0 {
                total_blocks_removed += removed_processed_in_batch;
                // Log progress
                info!(
                    "[{0:?}] Removed {1}, {2} blocks out of {3}, {4:.2} completed",
                    self,
                    removed_processed_in_batch,
                    total_blocks_removed,
                    total_blocks_to_remove,
                    total_blocks_removed as f64 / total_blocks_to_remove as f64 * 100.0,
                );
                removed_processed_in_batch = 0;
            }

            if added_processed_in_batch != 0 {
                total_blocks_added += added_processed_in_batch;
                // Log progress
                info!(
                    "[{0:?}] Added {1}, {2} blocks out of {3}, {4:.2} completed",
                    self,
                    added_processed_in_batch,
                    total_blocks_removed,
                    total_blocks_to_add,
                    total_blocks_added as f64 / total_blocks_to_add as f64 * 100.0,
                );
                added_processed_in_batch = 0;
            }
        }

        Ok(())
    }
}

impl TxIndexApi for TxIndex {
    // Resync methods.
    fn resync(&mut self) -> TxIndexResult<()> {
        info!("[{0:?}] Started Resyncing", self);

        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session_blocking());
        // Gather the necessary block hashes
        let txindex_source = self.stores.source_store.get()?;
        let consensus_history_root = session.get_history_root();
        let txindex_sink = self.stores.sink_store.get()?;
        let consensus_sink = session.get_sink();

        let start_hash = txindex_source
        .filter(|txindex_source| *txindex_source == consensus_history_root)
        .unwrap_or({
            info!(
                "[{0:?}] txindex source is not synced with consensus history root. txindex source: {1:?}, consensus history root: {2:?} - Resetting the DB",
                self,
                txindex_source,
                consensus_history_root,
            );
            self.stores.delete_all()?; // we reset the txindex
            // We can set the source anew after clearing db        
            self.stores.source_store.set(consensus_history_root)?;
            consensus_history_root
        });

        let end_hash = txindex_sink.map_or_else(
            || Ok::<Hash, TxIndexError>(consensus_sink),
            |txindex_sink| {
                if txindex_sink != consensus_sink {
                    info!(
                        "[{0:?}] txindex sink is not synced with consensus sink. txindex sink: {1:?}, consensus sink: {2}",
                        self, txindex_sink, consensus_sink,
                    );
                    // If txindex sink is not synced with consensus sink, and not a chain block, we must first unsync from the highest common chain block.
                    if !session.is_chain_block(txindex_sink)? {
                        let hccb = session.find_highest_common_chain_block(txindex_sink, consensus_sink)?;
                        info!(
                            "[{0:?}] txindex sink is not a chain block, unsyncing from {1} to the highest common chain block: {2}",
                            self, txindex_sink, hccb,
                        );
                        self._resync_segement(txindex_sink, hccb, &session, true)?;
                    }
                }
                Ok(consensus_sink)
            },
        )?;

        self._resync_segement(start_hash, end_hash, &session, false)?;
        Ok(())
    }

    // Sync state methods
    fn is_synced(&self) -> TxIndexResult<bool> {
        trace!("[{0:?}] checking sync status...", self);

        let consensus = self.consensus_manager.consensus();
        let session = futures::executor::block_on(consensus.session_blocking());

        if let Some(txindex_sink) = self.stores.sink_store.get()? {
            if txindex_sink == session.get_sink() {
                if let Some(txindex_source) = self.stores.source_store.get()? {
                    if txindex_source == session.get_source() {
                        return Ok(true);
                    }
                }
            }
        };

        Ok(false)
    }

    fn get_merged_block_acceptance_offset(&self, hashes: Vec<Hash>) -> TxIndexResult<Arc<Vec<Option<BlockAcceptanceOffset>>>> {
        trace!("[{0:?}] Getting merged block acceptance offsets for {1} blocks", self, hashes.len());

        Ok(Arc::new(
            hashes
                .iter()
                .map(move |hash| self.stores.merged_block_acceptance_store.get(*hash))
                .collect::<Result<Vec<Option<BlockAcceptanceOffset>>, _>>()?,
        ))
    }

    fn get_tx_offsets(&self, tx_ids: Vec<TransactionId>) -> TxIndexResult<Arc<Vec<Option<TxOffset>>>> {
        trace!("[{0:?}] Getting tx offsets for {1} txs", self, tx_ids.len());

        Ok(Arc::new(
            tx_ids
                .iter()
                .map(move |tx_id| self.stores.accepted_tx_offsets_store.get(*tx_id))
                .collect::<Result<Vec<Option<TxOffset>>, _>>()?,
        ))
    }

    // Update methods
    fn update_via_virtual_chain_changed(&mut self, vspcc_notification: ConsensusVirtualChainChangedNotification) -> TxIndexResult<()> {
        trace!(
            "[{0:?}] Updating db with {1} added chain blocks and {2} removed chain blocks via virtual chain changed notification",
            self,
            vspcc_notification.added_chain_block_hashes.len(),
            vspcc_notification.removed_chain_blocks_acceptance_data.len()
        );

        if vspcc_notification.added_chain_block_hashes.is_empty() && vspcc_notification.removed_chain_blocks_acceptance_data.is_empty()
        {
            // This shouldn't really happen, but it happens in integration tests 🤷.
            return Ok(());
        }

        let txindex_reindexer = TxIndexReindexer::from(vspcc_notification);

        let mut batch: rocksdb::WriteBatchWithTransaction<false> = WriteBatch::default();

        self.stores.accepted_tx_offsets_store.write_diff_batch(&mut batch, txindex_reindexer.tx_offset_changes)?;
        self.stores.merged_block_acceptance_store.write_diff_batch(&mut batch, txindex_reindexer.block_acceptance_offsets_changes)?;
        self.stores.sink_store.set_via_batch_writer(
            &mut batch,
            txindex_reindexer.new_sink.expect("expected a new sink with each new VCC notification"),
        )?;

        self.stores.write_batch(batch)
    }

    fn update_via_chain_acceptance_data_pruned(
        &mut self,
        chain_acceptance_data_pruned: ConsensusChainAcceptanceDataPrunedNotification,
    ) -> TxIndexResult<()> {
        trace!(
            "[{0:?}] Updating db with {1} removed chain blocks via chain acceptance data pruned notification",
            self,
            chain_acceptance_data_pruned.mergeset_block_acceptance_data_pruned.len()
        );
        let txindex_reindexer = TxIndexReindexer::from(chain_acceptance_data_pruned);

        let mut batch: rocksdb::WriteBatchWithTransaction<false> = WriteBatch::default();

        self.stores.accepted_tx_offsets_store.remove_many(&mut batch, txindex_reindexer.tx_offset_changes.removed)?;
        self.stores
            .merged_block_acceptance_store
            .remove_many(&mut batch, txindex_reindexer.block_acceptance_offsets_changes.removed)?;
        self.stores
            .source_store
            .replace_if_new(&mut batch, txindex_reindexer.source.expect("a source with each new CADP notification"))?;

        self.stores.write_batch(batch)
    }

    // This potentially causes a large chunk of processing, so it should only be used only for tests.
    fn count_all_merged_tx_ids(&self) -> TxIndexResult<usize> {
        Ok(self.stores.accepted_tx_offsets_store.count_all_keys()?)
    }

    // This potentially causes a large chunk of processing, so it should only be used only for tests.
    fn count_all_merged_blocks(&self) -> TxIndexResult<usize> {
        Ok(self.stores.merged_block_acceptance_store.count_all_keys()?)
    }

    fn get_sink(&self) -> TxIndexResult<Option<Hash>> {
        Ok(self.stores.sink_store.get()?)
    }

    fn get_source(&self) -> TxIndexResult<Option<Hash>> {
        Ok(self.stores.source_store.get()?)
    }
}

impl Debug for TxIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxIndex").finish()
    }
}

/*
#[cfg(test)]
mod tests {
    use crate::{config::Config as TxIndexConfig, TxIndex, api::TxIndexApi};
    use futures::sink;
    use kaspa_consensus::{
        consensus::test_consensus::TestConsensus,
        model::stores::{
            acceptance_data::{AcceptanceDataStore, AcceptanceDataStoreReader, DbAcceptanceDataStore},
            depth,
            virtual_state::DbVirtualStateStore,
            DB,
        },
        params::{DEVNET_PARAMS, MAINNET_PARAMS},
        testutils::generate::{
            self,
            from_rand::{
                acceptance_data::generate_random_acceptance_data,
                hash::{generate_random_hash, generate_random_hashes},
            },
        },
    };
    use kaspa_consensus_core::{
        acceptance_data::{MergesetBlockAcceptanceData, TxEntry},
        config::Config as ConsensusConfig,
        tx::TransactionId,
        BlockHashSet, BlockHashMap, HashMapCustomHasher,
    };
    use kaspa_consensus_notify::notification::{Notification, VirtualChainChangedNotification};
    use kaspa_consensusmanager::ConsensusManager;
    use kaspa_database::{
        create_temp_db,
        prelude::{BatchDbWriter, ConnBuilder},
        utils::DbLifetime,
    };
    use kaspa_hashes::Hash;
    use kaspa_index_core::models::txindex::{MergeSetIDX, BlockAcceptanceOffset, TxOffset};
    use kaspa_utils::hashmap;
    use parking_lot::RwLock;
    use rand::{distributions::Distribution, rngs::SmallRng, SeedableRng, seq::index};
    use std::{sync::Arc, hash::Hash};
    use std::collections::HashMap;

    struct TestContext {
        pub txindex: Arc<RwLock<TxIndex>>,
        pub consensus: Arc<TestConsensus>,
        db_lifetime: DbLifetime,
    }

    impl TestContext {
        fn new() -> Self {
            let (txindex_db_lifetime, txindex_db) = create_temp_db!(ConnBuilder::default().with_files_limit(10));
            let consensus_config = ConsensusConfig::new(MAINNET_PARAMS);
            let txindex_config = Arc::new(TxIndexConfig::from(&Arc::new(consensus_config.clone())));
            let consensus = Arc::new(TestConsensus::new(&consensus_config));
            let txindex =
                TxIndex::new(Arc::new(ConsensusManager::from_consensus(consensus.consensus_clone())), txindex_db, txindex_config).unwrap();

            Self { txindex, consensus, db_lifetime: txindex_db_lifetime }
        }
    }

    #[test]
    fn test_reorged_virtual_chain_changed() {
        // This test tests the re-orged condition of:
        // A 🡒 B 🡒 D
        // C 🡕
        //
        // To:
        //
        // A 🡒 E 🡒 F
        // C 🡕
        //
        // I.e. an Intial State of:
        // A: Accepting A,
        // B: Accepting B & C,
        // D: Accepting D,
        //
        // TO:
        //
        // A: Accepting A,
        // E: Accepting E & C,
        // F: Accepting F,
        //
        // Thereby:
        //
        // Removing B & D, adding E & F, and remerging C.
        //
        // The Test holds the following acceptance and rejection txs, regarding the transactions:
        //
        // Intial state:
        // A: Accepted
        // A: Unaccepted
        // B: Accepted
        // B: Unaccepted
        // C via B: Accepted x 4
        // C via B: Unaccepted x 4
        // D: Accepted
        // D: Unaccepted
        //
        // Which expands to the following flow with the re-org notification:
        //
        // C via B: Accepted -> C via E: Accepted
        // C via B: Unaccepted -> C via E: Unaccepted
        // C via B: Accepted -> C via E: Unaccepted
        // C via B: Unaccepted -> C via E: Accepted
        // C via B: Accepted -> C via E: Unaccepted -> F: Unaccepted
        // C via B: Unaccepted -> C via E: Unaccepted -> F: Accepted
        // C via B: Unaccepted -> C via E: Unaccepted -> F: Unaccepted
        // C via B: Accepted -> C via E: Accepted  -> F: Unaccepted
        // E: Accepted
        // E: Unaccepted
        // E: Unaccepted -> F: Accepted
        // E: Unaccepted -> F: Unaccepted
        // E: Accepted -> F: Unaccepted
        // F: Accepted
        // F: Unaccepted
        //
        // Afterward we should be left with the following accepted txs:
        //
        // A: Accepted,
        // C via B: Accepted -> C via E: Accepted,
        // C via B: Unaccepted -> C via E: Accepted,
        // C via B: Unaccepted -> C via E: Unaccepted -> F: Accepted,
        // E: Accepted
        // E: Accepted -> F: Unaccepted
        // E: Unaccepted -> F: Accepted
        // F: Accepted

        let test_context = TestContext::new();
        let rng = SmallRng::seed_from_u64(42u64);

        // Define Relevant Hashes
        let hash_a = Hash::from_u64_word(1);
        let hash_b = Hash::from_u64_word(2);
        let hash_c = Hash::from_u64_word(3);
        let hash_d = Hash::from_u64_word(4);
        let hash_e = Hash::from_u64_word(5);
        let hash_f = Hash::from_u64_word(6);

        // Define Relevant TxIds
        let tx_id_a_accpeted = TransactionId::from_u64(7);
        let tx_id_a_unaccpeted = TransactionId::from_u64(8);
        let tx_id_b_accpeted = TransactionId::from_u64(9);
        let tx_id_b_unaccpeted = TransactionId::from_u64(10);
        let tx_id_c_via_b_accpeted_via_e_accpeted = TransactionId::from_u64(11);
        let tx_id_c_via_b_accpeted_via_e_unaccepted = TransactionId::from_u64(12);
        let tx_id_c_via_b_unaccpeted_via_e_accepted = TransactionId::from_u64(13);
        let tx_id_c_via_b_unaccpeted_via_e_unaccepted = TransactionId::from_u64(14);
        let tx_id_c_via_b_accpeted_via_e_accpeted_via_f_unaccepted = TransactionId::from_u64(15);
        let tx_id_c_via_b_unaccpeted_via_e_accpeted_via_f_unaccepted = TransactionId::from_u64(16);
        let tx_id_c_via_b_unaccpeted_via_e_unaccpeted_via_f_accpeted = TransactionId::from_u64(17);
        let tx_id_c_via_b_unaccpeted_via_e_unaccpeted_via_f_unaccpeted = TransactionId::from_u64(18);
        let tx_id_d_accpeted = TransactionId::from_u64(19);
        let tx_id_d_unaccpeted = TransactionId::from_u64(20);
        let tx_id_e_accpeted = TransactionId::from_u64(21);
        let tx_id_e_unaccpeted = TransactionId::from_u64(22);
        let tx_id_f_accpeted = TransactionId::from_u64(23);
        let tx_id_f_unaccpeted = TransactionId::from_u64(24);
        let tx_id_

        // Define the initial state

        // extract expected block acceptance and tx offset from intial state

        // test expected block acceptance and tx offset from intial state to txindex state

        // define the re-org notification

        // extract expected block acceptance and tx offset from re-org notification

        // test expected block acceptance and tx offset from re-org notification to txindex state

        // test expected intial state residue against txindex state.


        let init_sink_hash = HASH_A;
        let init_sink_accepted_txs = vec![
            TxEntry { transaction_id: TX_ID_A, index_within_block: 0 },
            TxEntry { transaction_id: TX_ID_B, index_within_block: 1 },
        ];
        let init_sink_unaccepted_txs = vec![
            TxEntry { transaction_id: TX_ID_C, index_within_block: 2 },
            TxEntry { transaction_id: TX_ID_D, index_within_block: 3 },
        ];
        let init_sink_merged_hash = HASH_B;

        let init_sink_merged_hash_accepted_txs = vec![
            TxEntry { transaction_id: TX_ID_E, index_within_block: 0 },
            TxEntry { transaction_id: TX_ID_F, index_within_block: 1 },
        ];

        let init_sink_merged_hash_unaccepted_txs = vec![
            TxEntry { transaction_id: TX_ID_G, index_within_block: 2 },
            TxEntry { transaction_id: TX_ID_H, index_within_block: 3 },
        ];

        let init_virtual_chain_changed_notification = VirtualChainChangedNotification::new(
            Arc::new(vec![sink_hash]),
            Arc::new(vec![]),
            Arc::new(vec![Arc::new(vec![MergesetBlockAcceptanceData {
                block_hash: sink_merged_hash,
                accepted_transactions: sink_merged_hash_accepted_txs,
            }, MergesetBlockAcceptanceData {
                block_hash: sink_hash,
                accepted_transactions: sink_accepted_txs,
                unaccepted_transactions: sink_unaccepted_txs,
            }])]),
        Arc::new(vec![]),
        );

        let init_block_acceptance_offsets = BlockHashMap::<BlockAcceptanceOffset>::new();
        let init_tx_acceptance_offsets = HashMap::<TransactionId, TxOffset>::new();
        for accepting_block, mergesets in init_virtual_chain_changed_notification.added_chain_block_hashes.iter().copied().zip(init_virtual_chain_changed_notification.added_chain_blocks_acceptance_data.iter().cloned()) {
            for (i, mergeset) in mergesets.into_iter().enumerate() {
                init_block_acceptance_offsets.insert(mergeset.block_hash, BlockAcceptanceOffset::new(accepting_block, i as MergeSetIDX));
                init_tx_acceptance_offsets.extend(mergeset.accepted_transactions.into_iter().map(|tx_entry| (tx_entry.transaction_id, TxOffset::new(mergeset.block_hash, tx_entry.index_within_block))));
            };
        };

        test_context.txindex.write().update_via_virtual_chain_changed(virtual_chain_changed_notification).unwrap();

        }

    }
*/
