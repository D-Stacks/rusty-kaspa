use std::{cmp::max, mem::size_of, sync::Arc};

use kaspa_consensus_core::{
    acceptance_data::{MergesetBlockAcceptanceData, TxEntry},
    config::Config as ConsensusConfig,
    tx::TransactionId,
    Hash,
};
use kaspa_database::cache_policy_builder::bounded_size;
use kaspa_index_core::models::txindex::{BlockAcceptanceOffset, TxOffset};

use crate::core::config::{constants::DEFAULT_TXINDEX_EXTRA_FD_BUDGET, params::TxIndexParams, };

use super::constants::{DEFAULT_TXINDEX_MEMORY_BUDGET, DEFAULT_TXINDEX_DB_PARALLELISM};

#[derive(Clone, Debug)]
pub struct TxIndexPerfParams {
    pub mem_budget_total: usize,
    pub resync_chunksize: usize,
    pub extra_fd_budget: usize,
    pub db_parallelism: usize,
    unit_ratio_tx_offset_to_block_acceptance_offset: usize,
}

impl TxIndexPerfParams {
    pub fn new(consensus_config: &Arc<ConsensusConfig>, txindex_params: &TxIndexParams) -> Self {
        
        let resync_chunksize = bounded_size(
                txindex_params.max_blocks_in_mergeset_depth as usize, 
                DEFAULT_TXINDEX_EXTRA_FD_BUDGET, 
                max( //per chain block
                (
                    (size_of::<TransactionId>() + size_of::<TxOffset>()
                    ) 
                    * txindex_params.max_default_txs_per_block as usize
                    + (size_of::<BlockAcceptanceOffset>() + size_of::<Hash>())
                ) * consensus_config.params.mergeset_size_limit  as usize, 
                (
                    size_of::<TxEntry>() 
                    * txindex_params.max_default_txs_per_block as usize
                    + size_of::<MergesetBlockAcceptanceData>() 
                    + size_of::<Hash>()
                ) * consensus_config.params.mergeset_size_limit as usize
            )
        );

        Self {
            unit_ratio_tx_offset_to_block_acceptance_offset: txindex_params.max_default_txs_per_block as usize, 
            resync_chunksize,
            mem_budget_total: DEFAULT_TXINDEX_MEMORY_BUDGET,
            extra_fd_budget: DEFAULT_TXINDEX_EXTRA_FD_BUDGET,
            db_parallelism: DEFAULT_TXINDEX_DB_PARALLELISM,
        }
    }

    pub fn mem_size_tx_offset(&self) -> usize {
        size_of::<TransactionId>() + size_of::<TxOffset>()
    }

    pub fn mem_size_block_acceptance_offset(&self) -> usize {
        size_of::<BlockAcceptanceOffset>() + size_of::<Hash>()
    }

    pub fn mem_budget_tx_offset(&self) -> usize {
        self.mem_budget_total - self.mem_budget_block_acceptance_offset()
    }

    pub fn mem_budget_block_acceptance_offset(&self) -> usize {
        self.mem_budget_total / (
            (
                size_of::<TransactionId>() + size_of::<TxOffset>()
            ) * self.unit_ratio_tx_offset_to_block_acceptance_offset 
            / (
                size_of::<Hash>() + size_of::<BlockAcceptanceOffset>() 
            )
        )
    }
}