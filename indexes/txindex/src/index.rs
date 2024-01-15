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
        if unsync_segment {
            self.update_via_virtual_chain_changed(ConsensusVirtualChainChangedNotification::new(
                Arc::new(vec![start_hash]),
                Arc::new(vec![]),
                Arc::new(vec![session.get_block_acceptance_data(start_hash)?]),
                Arc::new(vec![]),
            ))?;
        } else {
            self.update_via_virtual_chain_changed(ConsensusVirtualChainChangedNotification::new(
                Arc::new(vec![]),
                Arc::new(vec![start_hash]),
                Arc::new(vec![]),
                Arc::new(vec![session.get_block_acceptance_data(start_hash)?]),
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
                    if txindex_source == session.get_history_root() {
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

        // This shouldn't really happen, but it happens in integration tests, so we handle it 🤷.
        if vspcc_notification.added_chain_block_hashes.is_empty() && vspcc_notification.removed_chain_blocks_acceptance_data.is_empty()
        {
            return Ok(());
        }

        let txindex_reindexer = TxIndexReindexer::from(vspcc_notification);

        let mut batch  = WriteBatch::default();

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

        let mut batch = WriteBatch::default();

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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, collections::HashSet, io::Write};

    use kaspa_consensus::{consensus::test_consensus::{TestConsensus, TestConsensusFactory}, params::MAINNET_PARAMS, model::stores::{virtual_state::VirtualState, acceptance_data::{AcceptanceDataStoreReader, DbAcceptanceDataStore, AcceptanceDataStore}, headers::HeaderStore, statuses::StatusesStore}, testutils::generate::from_rand::acceptance_data::{generate_random_acceptance_data, generate_random_acceptance_data_vec}, processes::relations::RelationsStoreExtensions};
    use kaspa_consensus_core::{config::Config as ConsensusConfig, acceptance_data::{AcceptanceData, MergesetBlockAcceptanceData, TxEntry}, tx::{TransactionId, TransactionIndexType}, ChainPath, BlockHashSet, HashMapCustomHasher, api::ConsensusApi, header::Header, blockstatus::BlockStatus};
    use kaspa_consensus_notify::notification::{VirtualChainChangedNotification, ChainAcceptanceDataPrunedNotification};
    use kaspa_consensusmanager::{ConsensusManager, ConsensusCtl};
    use kaspa_core::{log::init_logger, info};
    use kaspa_database::{create_temp_db, prelude::ConnBuilder};
    use kaspa_hashes::Hash;
    use kaspa_index_core::models::txindex::TxOffset;
    use parking_lot::RwLock;
    use rand::{rngs::SmallRng, SeedableRng};
    use rocksdb::WriteBatch;

    use crate::{TxIndex, config::Config as TxIndexConfig, api::TxIndexApi};

    fn assert_equal_along_virtual_chain(virtual_chain: &ChainPath, test_consensus: Arc<TestConsensus>, txindex: Arc<RwLock<TxIndex>>) {
        assert!(txindex.write().is_synced().unwrap());
        assert_eq!(txindex.write().get_sink().unwrap().unwrap(), test_consensus.get_sink());
        assert_eq!(txindex.write().get_source().unwrap().unwrap(), test_consensus.get_history_root());

        // check intial state
        for (accepting_block_hash, acceptance_data) in virtual_chain.added.iter().map(|hash| (*hash, test_consensus.get_block_acceptance_data(*hash).unwrap())) {
            for (i, mergeset_block_acceptance_data) in acceptance_data.iter().cloned().enumerate()  {
                let block_acceptance_offsets = txindex.write().get_merged_block_acceptance_offset(vec![mergeset_block_acceptance_data.block_hash]).unwrap();
                assert_eq!(block_acceptance_offsets.len(), 1);
                let block_acceptance_offset = block_acceptance_offsets.get(0).unwrap().unwrap();
                assert_eq!(block_acceptance_offset.accepting_block(), accepting_block_hash);
                assert_eq!(block_acceptance_offset.ordered_mergeset_index(), i as u16);
                for tx_entry in mergeset_block_acceptance_data.accepted_transactions {
                    let tx_offsets = txindex.write().get_tx_offsets(vec![tx_entry.transaction_id]).unwrap();
                    assert_eq!(tx_offsets.len(), 1);
                    let tx_offset = tx_offsets.get(0).unwrap().unwrap();
                    assert_eq!(tx_offset.including_block(), mergeset_block_acceptance_data.block_hash);
                    assert_eq!(tx_offset.transaction_index(), tx_entry.index_within_block);
                }
            }
        }

        for (accepting_block, acceptance_data) in virtual_chain.removed.iter().map(|hash| (*hash, test_consensus.get_block_acceptance_data(*hash).unwrap())) {
            for mergeset_block_acceptance_data in acceptance_data.iter().cloned()  {
                let res = txindex.write().get_merged_block_acceptance_offset(vec![mergeset_block_acceptance_data.block_hash]).unwrap();
                assert_eq!(res.len(), 1);
                let res = res.first().unwrap();
                if let Some(block_acceptance) = res {
                    assert_ne!(block_acceptance.accepting_block(), accepting_block); 
                } else { continue; }
        }
    }
}

    #[test]
    fn test_txindex_updates() {
        kaspa_core::log::try_init_logger("TRACE");

        // Note: this test closely mirrors the test `test_txindex_reindexer_from_virtual_chain_changed_notification` 
        // If both fail, check for problems within the reindexer. 

        // Set-up:
        let (_txindex_db_lt, txindex_db) = create_temp_db!(ConnBuilder::default().with_files_limit(10));
        let (_tc_db_lt, tc_db) = create_temp_db!(ConnBuilder::default().with_files_limit(10));
        
        let tc_config = ConsensusConfig::new(MAINNET_PARAMS);
        let txindex_config = Arc::new(TxIndexConfig::from(&Arc::new(tc_config.clone())));
        
        let tc = Arc::new(TestConsensus::with_db(tc_db.clone(), &tc_config));
        let tcm = Arc::new(ConsensusManager::new(Arc::new(TestConsensusFactory::new(tc.clone()))));
        let txindex = TxIndex::new(tcm, txindex_db, txindex_config.into()).unwrap();
        
        // Define the block hashes:

        // Blocks removed (i.e. unaccepted):
        let block_a = Hash::from_u64_word(1);
        let block_b = Hash::from_u64_word(2);

        // Blocks ReAdded (i.e. reaccepted):
        let block_aa @ block_hh = Hash::from_u64_word(3);

        // Blocks Added (i.e. newly reaccepted):
        let block_h = Hash::from_u64_word(4);
        let block_i = Hash::from_u64_word(5);

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

        let acceptance_data_a = Arc::new(vec![
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
        ]);

        let acceptance_data_b =  Arc::new(vec![
        MergesetBlockAcceptanceData { 
            block_hash: block_b,
            accepted_transactions: vec![
                TxEntry { transaction_id: tx_b_1, index_within_block: 0 },
                TxEntry { transaction_id: tx_b_2, index_within_block: 1 },
                TxEntry { transaction_id: tx_b_3, index_within_block: 2 },
                TxEntry { transaction_id: tx_b_4, index_within_block: 3 },
            ],
        },]);

        let virtual_chain = ChainPath::new(Arc::new(vec![block_a, block_b]), Arc::new(Vec::new()));

        let mut batch = WriteBatch::default();
        tc.acceptance_data_store.insert_batch(&mut batch, block_a, acceptance_data_a.clone()).unwrap();
        tc.acceptance_data_store.insert_batch(&mut batch, block_b, acceptance_data_b.clone()).unwrap();
        let mut state = VirtualState::default();
        state.ghostdag_data.selected_parent = block_b;
        tc.virtual_stores.write().state.set_batch(&mut batch, Arc::new(state)).unwrap();
        tc_db.write(batch).unwrap();

        let init_virtual_chain_changed_notification = VirtualChainChangedNotification {
            added_chain_block_hashes: virtual_chain.clone().added.clone(),
            removed_chain_block_hashes: virtual_chain.clone().removed.clone(),
            added_chain_blocks_acceptance_data: Arc::new(vec![
                acceptance_data_a.clone(),
                acceptance_data_b.clone(),
            ],),
            removed_chain_blocks_acceptance_data: Arc::new(Vec::new()),
        };

        txindex.write().update_via_virtual_chain_changed(init_virtual_chain_changed_notification).unwrap();

        assert_equal_along_virtual_chain(&virtual_chain, tc.clone(), txindex.clone());
        assert_eq!(txindex.write().count_all_merged_blocks().unwrap(), 3);
        assert_eq!(txindex.write().count_all_merged_tx_ids().unwrap(), 12);

        let acceptance_data_h = Arc::new(vec![
            MergesetBlockAcceptanceData { 
                block_hash: block_h, 
                accepted_transactions: vec![
                    TxEntry { transaction_id: tx_h_1, index_within_block: 0 },
                    TxEntry { transaction_id: tx_h_2, index_within_block: 1 },
                    TxEntry { transaction_id: tx_h_3, index_within_block: 2 },
                    TxEntry { transaction_id: tx_h_4, index_within_block: 3 },
                ]
            },
            MergesetBlockAcceptanceData { 
                block_hash: block_hh, 
                accepted_transactions: vec![
                    TxEntry { transaction_id: tx_hh_1, index_within_block: 0 },
                    TxEntry { transaction_id: tx_hh_2, index_within_block: 1 },
                    TxEntry { transaction_id: tx_hh_3, index_within_block: 2 },
                    TxEntry { transaction_id: tx_hh_4, index_within_block: 3 },
                ]
            }
        ]);

        let acceptance_data_i= Arc::new(vec![
            MergesetBlockAcceptanceData { 
                block_hash: block_i, 
                accepted_transactions: vec![
                    TxEntry { transaction_id: tx_i_1, index_within_block: 0 },
                    TxEntry { transaction_id: tx_i_2, index_within_block: 1 },
                    TxEntry { transaction_id: tx_i_3, index_within_block: 2 },
                    TxEntry { transaction_id: tx_i_4, index_within_block: 3 },
                ]
            }
        ]);

        let virtual_chain = ChainPath::new(Arc::new(vec![block_h, block_i]), Arc::new(vec![block_a, block_b]));

        // Define the notification:
        let test_vspcc_change_notification = VirtualChainChangedNotification {
            added_chain_block_hashes: virtual_chain.added.clone(),
            added_chain_blocks_acceptance_data: Arc::new(vec![
                acceptance_data_h.clone(),
                acceptance_data_i.clone()
            ]),
            removed_chain_block_hashes: virtual_chain.removed.clone(),
            removed_chain_blocks_acceptance_data: Arc::new(vec![
                acceptance_data_a.clone(),
                acceptance_data_b.clone(),
            ]),
        };

        let mut batch = WriteBatch::default();
        tc.acceptance_data_store.insert_batch(&mut batch, block_h, acceptance_data_h.clone()).unwrap();
        tc.acceptance_data_store.insert_batch(&mut batch, block_i, acceptance_data_i.clone()).unwrap();
        let mut state = VirtualState::default();
        state.ghostdag_data.selected_parent = block_i;
        tc.virtual_stores.write().state.set_batch(&mut batch, Arc::new(state)).unwrap();
        tc_db.write(batch).unwrap();

        txindex.write().update_via_virtual_chain_changed(test_vspcc_change_notification).unwrap();

        assert_equal_along_virtual_chain(&virtual_chain, tc.clone(), txindex.clone());
        assert_eq!(txindex.write().count_all_merged_blocks().unwrap(), 3);
        assert_eq!(txindex.write().count_all_merged_tx_ids().unwrap(), 12);

        let prune_notification = ChainAcceptanceDataPrunedNotification {
            chain_hash_pruned: block_h,
            mergeset_block_acceptance_data_pruned: acceptance_data_h.clone(),
            history_root: block_i,
        };

        let virtual_chain = ChainPath::new(Arc::new(vec![block_i]), Arc::new(vec![]));

        let mut batch = WriteBatch::default();
        tc.acceptance_data_store.delete_batch(&mut batch, block_h).unwrap();
        tc.pruning_point_store.write().set_history_root(&mut batch, block_i).unwrap();
        tc_db.write(batch).unwrap();

        txindex.write().update_via_chain_acceptance_data_pruned(prune_notification).unwrap();
        
        assert_equal_along_virtual_chain(&virtual_chain, tc, txindex.clone());
        assert_eq!(txindex.write().count_all_merged_blocks().unwrap(), 1);
        assert_eq!(txindex.write().count_all_merged_tx_ids().unwrap(), 4);
    }
