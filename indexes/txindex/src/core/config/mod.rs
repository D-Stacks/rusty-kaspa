mod constants;
pub mod perf;
pub mod params;

use std::sync::Arc;

use bincode::de;
use kaspa_consensus::consensus;
use kaspa_consensus_core::config::Config as ConsensusConfig;

use crate::core::config::{perf::TxIndexPerfParams, params::TxIndexParams};

#[derive(Clone, Debug)]
pub struct TxIndexConfig {
    pub txindex_perf_params: TxIndexPerfParams,
    pub txindex_params: TxIndexParams,
}

impl TxIndexConfig {
    pub fn new(consensus_config: &Arc<ConsensusConfig>) -> Self {
        let txindex_params = TxIndexParams::from(consensus_config);
        Self {
            txindex_params: txindex_params.clone(),
            txindex_perf_params: TxIndexPerfParams::new(consensus_config, &txindex_params),
        }
    }
}