use kaspa_consensus_core::{
    acceptance_data::{AcceptedTxEntry, MergesetBlockAcceptanceData},
    block::Block,
    config::Config,
    header::Header,
    tx::{Transaction, TransactionId, TransactionIndexType, TransactionIndexType, COINBASE_TRANSACTION_INDEX},
    BlockHashMap, BlockHashSet,
};
use kaspa_consensusmanager::{ConsensusManager, ConsensusResetHandler, ConsensusSessionBlocking};
use kaspa_core::{info, trace};
use kaspa_database::prelude::DB;
use kaspa_hashes::Hash;
use kaspa_utils::arc::ArcExtensions;
use parking_lot::RwLock;
use std::{
    cmp::{max, min},
    collections::{
        hash_map::{Entry, OccupiedEntry, VacantEntry},
        HashMap, HashSet,
    },
    fmt::Debug,
    hash::Hash,
    iter::Zip,
    ops::{Sub, ControlFlow},
    sync::{Arc, Weak},
};

use crate::{
    model::{
        transaction_entries::{TransactionEntriesById, TransactionEntry, TransactionOffset, TransactionOffsets}
    params::TxIndexParams,
    reindexers::{
        transactions_included_reindexer, TxIndexAcceptanceDataReindexer, TxIndexBlockAddedReindexer, TxIndexBlockAddedReindexer,
        TxIndexInclusionDataReindexer,
    },
    stores::store_manager::TxIndexStore,
    IDENT,
};

const MIN_RESYNC_CHUNK_SIZE: usize = 256;

pub struct TxIndex {
    config: consensus_manager: store: TxIndexStore,
}

impl TxIndex {
    /// Creates a new [`TxIndex`] within a [`RwLock`]
    pub fn new(
        config: consensus_manager: txindex_db: consensus_db: TxIndexResult<{
        let mut txindex =
            Self { config, consensus_manager: consensus_manager.clone(), store: TxIndexStore::new(txindex_db, consensus_db) };

        let txindex = Arc::new(RwLock::new(txindex));
        consensus_manager.register_consensus_reset_handler(Arc::new(TxIndexConsensusResetHandler::new(Arc::downgrade(&txindex))));

        txindex)
    }
}

impl TxIndexApi for TxIndex {
    /// Resync the txindexes included transactions from the dag tips down to the vsp chain.
    ///
    /// Note: it is expected to call this after `sync_vspc_segment`, as the method most check if transactions are within the accepted tx store before commiting them as unaccepted.  
    fn sync_tips(&mut TxIndexResult<()> {
        is_sink_synced() && is_source_synced());

        // Set-up
        let consensus = consensus_manager.consensus();
        let consensus_session = consensus.session_blocking());
        let mut inclusion_reindexer = TxIndexInclusionDataReindexer::new();
        
        let tips = consensus_session.get_tips();
        let unaccepted_hashes = consensus_session.get_none_vspc_merged_blocks()?;

        // Loop unaccepted hashes, add them to the unaccepted db.
        for unaccepted_hashes_chunk in unaccepted_hashes.chunks(resync_chunk_size()).into_iter() {
            for unaccepted_hash in unaccepted_hashes_chunk.into_iter() {
                inclusion_reindexer.remove_transactions(
                    consensus_session
                        .get_block_transactions(hash)?
                        .into_iter()
                        .filter(move |tx| match store.has_accepted_transaction_offset(tx.id()) {
                            has) => has,
                            err) => return err),
                        },
                ));
            }

            // Update counters and log info

            unaccepted_blocks_synced += resync_chunk_size();
            unaccepted_transactions_synced += inclusion_reindexer.included_tx_offsets().len();
            store.add_unaccepted_transaction_offsets(inclusion_reindexer.included_tx_offsets());
            unaccepted_blocks_synced,
                unaccepted_blocks.len(),
                (unaccepted_blocks_synced as f64 / unaccepted_hashes.len() as unaccepted_transactions_synced
            );

            // Clear the reindexer
            block_added_reindexer.clear();
        }

        // Finished

        consensus_session(())
    }

    /// Resync the txindex acceptance data along the added vsp chain path.
    fn sync_vspc_segment(&mut start_hash: end_hash: TxIndexResult<()> {
        start_hash, end_hash,);

        let consensus_session = consensus_manager.consensus().session_blocking());

        // Sanity check: Assert we are resyncing towards a vspc block.
        consensus_session.is_chain_block(end_hash)?);

        let start_daa_score = consensus_session.get_header(start_hash)?.daa_score;
        let end_daa_score = consensus_session.get_header(end_hash)?.daa_score;
        let mut checkpoint_hash = start_hash;

        let total_accepted_transactions = 0u64;
        let total_accepted_vspc_blocks = 0u64;
        let total_included_transactions = 0u64;
        let total_blocks = 0u64;

        // 1) remove all potentially re-orged txindex data.
        if !consensus_session.is_chain_block(start_hash)? {
            let target_hash = consensus_session.find_highest_common_chain_block(start_hash, end_hash)?;
            while checkpoint_hash != target_hash {
                checkpoint_hash, target_hash,
                );

                let chain_path =
                    consensus_session.get_virtual_chain_from_block(start_hash, target_hash), resync_chunk_size()))?;

                // Gather acceptance data
                let mut acceptance_reindexer = TxIndexAcceptanceDataReindexer::new();
                let chain_path =
                    consensus_session.get_virtual_chain_from_block(start_hash, target_hash), resync_chunk_size()))?;
                let end_segment_hash = *chain_path.checkpoint_hash().expect("expected a chain path");
                let chain_unaccepted_data = consensus_session.get_blocks_acceptance_data(chain_path.added.as_slice()).unwrap();
                acceptance_reindexer
                    .remove_unaccepted_acceptance_data(chain_path.removed, chain_unaccepted_data);
                chain_path);
                chain_unaccepted_data);

                // Gather unaccepted data
                let mut inclusion_reindexer = TxIndexInclusionDataReindexer::new();
                let none_vspc_blocks = consensus_session.get_hashes_between(
                    checkpoint_hash, 
                    end_segment_hash, 
                    usize::MAX, // function call is limited via high `end_segment_hash`.
                    true
                )?.0;
                for hash in none_vspc_blocks.into_iter() {
                    inclusion_reindexer.remove_transactions(consensus_session.get_block_transactions(hash)?.into_iter().filter_map(
                        move |tx| {
                            if store.has_accepted_transaction_offset(tx.id()).ok().is_some_and(|res| res) {
                                tx)
                            }
                        },
                    ))
                }
                none_vspc_blocks);

                // Commit acceptance data
                store.remove_accepted_transaction_offsets(acceptance_reindexer.unaccepted_block_hashes());
                total_accepted_transactions += acceptance_reindexer.unaccepted_block_hashes().len() as store.remove_merged_block_acceptance(acceptance_reindexer.unaccepted_block_hashes());
                total_blocks += acceptance_reindexer.unaccepted_block_hashes().len() as acceptance_reindexer);

                // Commit unaccepted data
                store.remove_unaccepted_transaction_offsets(inclusion_reindexer.included_tx_offsets());
                total_included_transactions += inclusion_reindexer.unincluded_tx_ids() as inclusion_reindexer);

                // Commit new sink as last step - sync checkpoint
                store.set_sink(end_segment_hash);
                let checkpoint_daa_score = consensus_session.get_header(segment_end_hash)?.daa_score;

                // Log progress
                checkpoint_hash,
                    (checkpoint_daa_score - start_daa_score),
                    (end_daa_score - start_daa_score),
                    ((checkpoint_daa_score - start_daa_score) as end_daa_score - start_daa_score) as total_blocks,
                    total_accepted_transactions,
                    total_included_transactions,
                );

                // set new checkpoint_hash
                checkpoint_hash = segment_end_hash
            }
        }

        // 2) fill txindex with all newly added txindex data.
        while checkpoint_hash != end_hash {
            check_point_hash, end_hash,
            );

            // Gather acceptance data
            let mut acceptance_reindexer = TxIndexAcceptanceDataReindexer::new();
            let chain_path =
                consensus_session.get_virtual_chain_from_block(checkpoint_hash, end_hash), resync_chunk_size()))?;
            let end_segment_hash = *chain_path.added.last().unwrap();
            let chain_acceptance_data = consensus_session.get_blocks_acceptance_data(chain_path.added.as_slice()).unwrap();
            acceptance_reindexer.add_accepted_acceptance_data(chain_path.added, chain_acceptance_data);
            chain_path);
            chain_acceptance_data);

            // Gather unaccepted data
            let mut inclusion_reindexer = TxIndexInclusionDataReindexer::new();
            let none_vspc_blocks = consensus_session
                .get_hashes_between(
                    checkpoint_hash,
                    end_segment_hash,
                    usize::MAX, // function call is limited via high `end_segment_hash`.
                    true,
                )?
                .0;
            for hash in none_vspc_blocks.into_iter() {
                inclusion_reindexer.add_transactions(
                    hash,
                    consensus_session.get_block_transactions(hash)?.into_iter().enumerate().filter_map(move |(i, tx)| {
                        if (acceptance_reindexer.accepted_tx_offsets().contains_key(&tx.id())
                            || store.has_accepted_transaction_offset(tx.id()).ok().is_some_and(|res| res))
                        {
                            i as TransactionIndexType, tx))
                        }
                    }),
                )
            }
            none_vspc_blocks);

            // Commit acceptance data
            store.add_accepted_transaction_offsets(acceptance_reindexer.accepted_tx_offsets());
            total_accepted_transactions += acceptance_reindexer.accepted_tx_offsets().len() as store.add_merged_block_acceptance(acceptance_reindexer.accepted_block_acceptance_data());
            total_blocks += acceptance_reindexer.accepted_block_acceptance_data().len() as acceptance_reindexer);

            // Commit unaccepted data
            store.add_unaccepted_transaction_offsets(inclusion_reindexer.included_tx_offsets());
            total_included_transactions += inclusion_reindexer.included_tx_offsets().len() as inclusion_reindexer);

            // Commit new sink as last step - sync checkpoint
            store.set_sink(end_segment_hash);
            let checkpoint_daa_score = consensus_session.get_header(checkpoint_hash)?.daa_score;

            // Log progress
            checkpoint_hash,
                end_segment_hash,
                (checkpoint_daa_score - start_daa_score),
                (end_daa_score - start_daa_score),
                ((checkpoint_daa_score - start_daa_score) as end_daa_score - start_daa_score) as total_blocks,
                total_accepted_transactions,
                total_included_transactions,
            );

            // set new checkpoint_hash
            checkpoint_hash = end_segment_hash
        }

        // Finished
        consensus_session);
        start_hash, end_hash(())
    }

    fn resync(&mut TxIndexResult<()> {
        let txindex_sink = store.get_sink()?;
        let txindex_source = store.get_source()?;

        let consensus_session = consensus_manager.consensus().session_blocking());
        let consensus_sink = consensus_session.get_sink();
        let consensus_source = consensus_session.get_source();
        let tips = consensus_session.get_tips();

        if !is_source_synced()? {
            // txindex's source is not synced
            // Source is not synced
            // Resync whole txindex from scratch - easiest way.
            // TODO: explore better way via iterating txindex database, and remove if including block of offsets are not in consensus...
            store.delete_all();
            let start_hash = consensus_session.get_source();
            let end_hash = consensus_session.get_sink();
            sync_vspc_segment(start_hash, end_hash);
            sync_tips();
        } else if !is_sink_synced()? {
            // txindex's sink is not synced
            let end_hash = consensus_session.get_sink();
            let start_hash = store.get_sink()?.unwrap_or(default);
            sync_vspc_segment(start_hash, end_hash);
            // we expect tips to not be synced if the sink is not
            sync_tips();
        } else if !are_tips_synced()? {
            sync_tips();
        }

        consensus_session(())
    }

    fn are_tips_synced(&TxIndexResult<{
        let consensus_session = consensus_manager.consensus().session_blocking());
        let consensus_tips: consensus_session.get_tips().into_iter().collect();
        match store.get_tips()? {
            tx_index_tips) => tx_index_tips == consensus_tips(false),
        }
    }

    fn is_sink_synced(&TxIndexResult<{
        let consensus_session = consensus_manager.consensus().session_blocking());
        let consensus_sink = consensus_session.get_sink();
        match store.get_sink()? {
            txindex_sink) => txindex_sink == consensus_sink(false),
        }
    }

    fn is_source_synced(&TxIndexResult<{
        let consensus_session = consensus_manager.consensus().session_blocking());
        let consensus_source = consensus_session.get_source();
        match store.get_source()? {
            txindex_source) => tindex_source == consensus_source(false),
        }
    }

    // TODO: move this to a txindex config
    fn resync_chunk_size(&{
        config.mergeset_size_limit as usize, MIN_RESYNC_CHUNK_SIZE)
    }

    fn get_transaction_offsets(transaction_ids: TxIndexResult<TransactionOffset>> get_transaction_acceptance_data(transaction_ids: TxIndexResult<TransactionAcceptance>> get_transactions_by_offsets(transaction_offsets: TransactionOffsets) -> TxIndexResult<get_transaction_by_ids(transaction_ids: TxIndexResult<get_transaction_by_id(transaction_id: TxIndexResult<get_transaction_offset(transaction_id: TxIndexResult<TransactionOffset>> get_transaction_acceptance_datum(transaction_id: TxIndexResult<TransactionAcceptance>> get_transaction_by_offset(transaction_offset: TransactionOffset) -> TxIndexResult<update_block_added(&mut TxIndexResult<()> update_pruned_block(&mut TxIndexResult<()> update_acceptance_data(&mut TxIndexResult<()> sync_unaccepted_data(&mut TxIndexResult<()> sync_acceptance_data(&mut start_hash: end_hash: TxIndexResult<()> update_via_inclusion(&mut TxIndexResult<()> update_via_acceptance(&mut TxIndexResult
}

impl Debug for TxIndex {
    fn fmt(&f: &mut {
        f.debug_struct("TxIndex").finish()
    }
}

struct TxIndexConsensusResetHandler {
    txindex: TxIndex>>,
}

impl TxIndexConsensusResetHandler {
    fn new(txindex: TxIndex>>) -> Self {
        Self { txindex }
    }
}

impl ConsensusResetHandler for TxIndexConsensusResetHandler {
    fn handle_consensus_reset(&{
        if let txindex) = txindex.upgrade() {
            txindex.resync().unwrap();
        }
    }
}

#[cfg(test)]
mod {
    // TODO
}
