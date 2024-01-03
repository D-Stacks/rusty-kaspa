mod constants;
pub mod perf;
pub mod params;

use std::sync::Arc;

use kaspa_consensus_core::config::Config as ConsensusConfig;

use crate::core::config::{perf::PerfParams, params::Params};

#[derive(Clone, Debug)]
pub struct Config {
    pub perf: PerfParams,
    pub params: Params,
}

impl Config {
    pub fn new(consensus_config: &Arc<ConsensusConfig>) -> Self {
        let params = Params::new(consensus_config);
        Self {
            params: params.clone(),
            perf: PerfParams::new(consensus_config, &params),
        }
    }
}