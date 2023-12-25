use kaspa_consensus::{config::Config as ConsensusConfig, params::Params as ConsensusParams, processes::mass::MassCalculator};
use kaspa_consensus_core::{tx::{Transaction, TransactionInput, TransactionOutput, TransactionOutpoint, ScriptPublicKey, ScriptVec, ScriptPublicKeyVersion, scriptvec, TransactionId}, constants::MAX_SCRIPT_PUBLIC_KEY_VERSION, subnets::SubnetworkId, block::Block, acceptance_data::{AcceptanceData, MergesetBlockAcceptanceData, TxEntry}};
use kaspa_hashes::{ZERO_HASH, Hash};
use std::{cmp::{max, min}, mem};

use crate::model::{TxOffset, BlockAcceptanceOffset};

use super::config::TxIndexConfig;

/// Hard limit on resync chunk size to hold about ~100 mbs worth of transactions. 
const HARD_LIMIT_RESYNC_CHUNKSIZE: u64 = (1_000_000_000.0 / (mem::size_of::<Transaction>() + mem::size_of::<TransactionId>()) as f64).round() as u64;

pub struct TxIndexParams {
    resync_chunksize: u64, 
}

impl TxIndexParams {
    fn new(consensus_config: &ConsensusConfig, txindex_config: &TxIndexConfig ) -> Self {

        let maximum_amount_of_txs_per_block = (consensus_config.max_block_mass as f64 / minimal_possible_mass as f64).floor();
        
        let resync_chunksize = min( 
            (104_857_600.0 / (((mem::size_of::<TxOffset>() + mem::size_of::<TransactionId>()) * maximum_amount_of_txs_per_block)  + mem::size_of::<BlockAcceptanceOffset>() + mem::size_of::<Hash>()) as f64).floor() as u64, // reindexed overhead, under worst case assumptions, per block. 
            (104_857_600.0 / ((consensus_config.mergeset_size_limit * (mem::size_of::<MergesetBlockAcceptanceData>() +  (mem::size_of::<TxEntry>() * maximum_amount_of_txs_per_block))) + mem::size_of::<Hash>()) as f64).floor() as u64, // pre-indexed overhead, under worst case assumptions, per block.
        );
        let resync_chunksize = (((consensus_config.max_block_mass as f64 / minimal_possible_mass as f64) * consensus_config.bps() as f64).round() as u64 + consensus_config.bps()) * 60u64;
        println!("{resync_chunksize}");

        Self { 
          self.resync_chunksize = resync_chunksize,
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