pub mod core; //all things visible to the outside
mod index;
mod stores;

pub use crate::core::*; //Expose all things intended for external usage.
pub use crate::index::TxIndex; //we expose this separately to initiate the index.

const IDENT: &str = "txindex";

#[cfg(test)]
mod tests {
    use crate::{
        core::*,
        model::transaction_entries::{TransactionEntry, TransactionOffset},
    };
    use kaspa_consensus::test_helpers::generate_random_hash;
    use kaspa_core::info;
    use rand::rngs::SmallRng;

    /// TODO: use proper Simnet when implemented.
    #[test]
    fn test_playground() {
        kaspa_core::log::try_init_logger("INFO");
        let rng = &mut SmallRng::seed_from_u64(43);
        let test_entry = TransactionEntry {
            offset: Some(TransactionOffset { including_block: generate_random_hash(rng), transaction_index: rng.gen::<u32>() }),
            accepting_block: Some(generate_random_hash(rng)),
        };
        info!(bincode::serialized_size(rng));
    }
}
