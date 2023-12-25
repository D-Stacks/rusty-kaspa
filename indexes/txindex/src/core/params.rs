use kaspa_consensus::{config::Config as ConsensusConfig, params::Params as ConsensusParams, processes::mass::MassCalculator};
use kaspa_consensus_core::{tx::{Transaction, TransactionInput, TransactionOutput, TransactionOutpoint, ScriptPublicKey, ScriptVec, ScriptPublicKeyVersion, scriptvec, TransactionId}, constants::MAX_SCRIPT_PUBLIC_KEY_VERSION, subnets::SubnetworkId, block::Block, acceptance_data::{AcceptanceData, MergesetBlockAcceptanceData}};
use kaspa_hashes::{ZERO_HASH, Hash};
use std::{cmp::max, mem};

use crate::model::TxOffset;

/// Hard limit on resync chunk size to hold about ~100 mbs worth of transactions. 
const HARD_LIMIT_RESYNC_CHUNKSIZE: u64 = (1_000_000_000.0 / (mem::size_of::<Transaction>() + mem::size_of::<TransactionId>()) as f64).round() as u64;

pub struct TxIndexParams {
    resync_chunksize: u64
}

impl TxIndexParams {
    fn new(consensus_config: &ConsensusConfig) -> Self {
        
        let minimal_possible_tx = &Transaction::new(
            0, 
            vec![TransactionInput::new(
                TransactionOutpoint::new(ZERO_HASH, 0),
                vec![0x01],
                0, 
                0)], 
                
                vec![TransactionOutput::new(
                    0, 
                    ScriptPublicKey::new(MAX_SCRIPT_PUBLIC_KEY_VERSION, scriptvec![0x00]))], 
                0, 
                SubnetworkId::default(), 
                0, 
                vec![]
            );
 
        let minimal_possible_mass = MassCalculator::new(
            consensus_config.mass_per_tx_byte, 
            consensus_config.mass_per_script_pub_key_byte,
            consensus_config.mass_per_sig_op,
        ).calc_tx_mass(minimal_possible_tx);
        println!("{minimal_possible_mass}");
    
        let maximum_amount_of_txs_per_block = (consensus_config.max_block_mass as f64 / minimal_possible_mass as f64).floor();

        let resync_chunksize = 
            1_000_000_000.0 / //Max bytes we are willing to hold ( 100 Mbs)
            ((mem::size_of::<Transaction>() + mem::size_of::<Hash>()) * maximum_amount_of_txs_per_block) + consensus_config.mergeset_size_limit * (mem::size_of::<MergesetBlockAcceptanceData>() + mem::size_of::<Hash>()).floor() as u64; // We expect to hold this much in memory, per block, under worst-case assumptions

        let resync_chunksize = (((consensus_config.max_block_mass as f64 / minimal_possible_mass as f64) * consensus_config.bps() as f64).round() as u64 + consensus_config.bps()) * 60u64;
        println!("{resync_chunksize}");

        Self { 
            // Corrosponds to an expected 1 minute worth of tx throughput, under worst-case (spamming) assumptions. 
            // TODO: fill numbers below after values are analysed
            // xxx on mainnet, xxx on testnet11
            resync_chunksize: max(
                resync_chunksize, 
                consensus_config.mergeset_size_limit // We require at least this chunksize for consensus_hashes_between call. 
            ), 
        }
    }
}

#[cfg(test)]
pub mod test {
    use kaspa_consensus::{consensus::test_consensus::TestConsensus, params::{Params, Testnet11Bps, TESTNET11_PARAMS}};
    use kaspa_consensus_core::config::Config;

    use super::TxIndexConfig;

    #[test]
    fn test_txindex_config() {
        TxIndexConfig::new(&Config::new(TESTNET11_PARAMS));
    }
}