pub mod models;
pub mod notify;
pub mod reindexers;

#[cfg(test)]
mod test {
    use crate::models::txindex::MergeSetIDX;
    use crate::models::utxoindex::{CirculatingSupply, CirculatingSupplyDiff};
    use kaspa_consensus_core::{config::params::Params, constants::MAX_SOMPI, network::NetworkType};

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
}
