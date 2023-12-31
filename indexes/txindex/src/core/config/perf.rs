use std::{cmp::max, mem, sync::Arc};

use kaspa_consensus_core::{
    acceptance_data::{MergesetBlockAcceptanceData, TxEntry},
    config::{constants::perf::bounded_cache_size, Config as ConsensusConfig},
    tx::TransactionId,
    Hash,
};
use kaspa_index_core::models::txindex::{BlockAcceptanceOffset, TxOffset};

use crate::{
    core::config::constants::{
        STANDARD_SCHNORR_SCRIPT_PUBLIC_KEY_LEN, STANDARD_TRANSACTION_HEADER_SIZE, STANDARD_TRANSACTION_INPUT_SIZE,
        STANDARD_TRANSACTION_OUTPUT_SIZE,
    },
};

pub fn calculate_approx_std_transaction_size_in_bytes(number_of_inputs: u64, number_of_outputs: u64) -> u64 {
    STANDARD_TRANSACTION_HEADER_SIZE
        + number_of_inputs * STANDARD_TRANSACTION_INPUT_SIZE
        + number_of_outputs * STANDARD_TRANSACTION_OUTPUT_SIZE
}

pub fn calculate_approx_std_transaction_mass(
    consensus_config: &Arc<ConsensusConfig>,
    number_of_inputs: u64,
    number_of_outputs: u64,
) -> u64 {
    let byte_mass =
        calculate_approx_std_transaction_size_in_bytes(number_of_inputs, number_of_outputs) * consensus_config.mass_per_tx_byte;
    let signature_op_mass = number_of_inputs * consensus_config.mass_per_sig_op; // OP_CHECKSIG per input
    let script_public_key_mass = ((number_of_inputs + number_of_outputs) * STANDARD_SCHNORR_SCRIPT_PUBLIC_KEY_LEN)
        * consensus_config.mass_per_script_pub_key_byte;
    byte_mass + signature_op_mass + script_public_key_mass
}

pub const DEFAULT_TXINDEX_MEMORY_BUDGET: u64 = 750_000_000; // 750mb
pub struct TxIndexPerfParams {
    pub offset_cache_size: u64,
    pub block_acceptance_cache_size: u64,
    pub resync_chunksize: u64,
}

impl TxIndexPerfParams {
    pub fn new(consensus_config: &Arc<ConsensusConfig>) -> Self {
        let unit_size_offset = (mem::size_of::<TxOffset>() + mem::size_of::<TransactionId>()) as u64;
        let unit_size_merged_block = (mem::size_of::<BlockAcceptanceOffset>() + mem::size_of::<Hash>() + mem::size_of::<u32>()) as u64;

        let expected_higher_bound_std_transactions_per_block =
            consensus_config.max_block_mass / calculate_approx_std_transaction_mass(consensus_config, 1, 1);
        let ratio_for_merged_block_acceptance = (unit_size_merged_block as f64
            / (unit_size_offset as f64 * expected_higher_bound_std_transactions_per_block as f64))
            .floor() as u64;

        let memory_budget_merged_blocks = DEFAULT_TXINDEX_MEMORY_BUDGET * ratio_for_merged_block_acceptance;
        let memory_budget_offsets = DEFAULT_TXINDEX_MEMORY_BUDGET - memory_budget_merged_blocks;
        Self {
            offset_cache_size: bounded_cache_size(
                expected_higher_bound_std_transactions_per_block * consensus_config.merge_depth,
                memory_budget_offsets,
                unit_size_offset as usize,
            ),
            block_acceptance_cache_size: bounded_cache_size(
                consensus_config.merge_depth,
                memory_budget_merged_blocks,
                unit_size_merged_block as usize,
            ),
            resync_chunksize: bounded_cache_size(
                consensus_config.merge_depth,
                DEFAULT_TXINDEX_MEMORY_BUDGET,
                max(
                    // we load both into mem, we use higher unit size for bounding
                    ((((unit_size_offset * expected_higher_bound_std_transactions_per_block) * consensus_config.mergeset_size_limit)
                        + unit_size_merged_block) as f64)
                        .floor() as u64, // reindexed overhead, under worst case assumptions, per block.
                    ((((mem::size_of::<MergesetBlockAcceptanceData>() as u64
                        + (mem::size_of::<TxEntry>() as u64 * expected_higher_bound_std_transactions_per_block))
                        * consensus_config.mergeset_size_limit)
                        + mem::size_of::<Hash>() as u64) as f64)
                        .floor() as u64, // pre-indexed overhead, under worst case assumptions, per block.
                ) as usize,
            ),
        }
    }
}
