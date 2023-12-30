use kaspa_consensus_core::{
    block::Block,
    header::Header,
    subnets::SubnetworkId,
    tx::{ScriptPublicKey, ScriptVec, Transaction, TransactionInput, TransactionOutpoint, TransactionOutput, UtxoEntry},
    utxo::utxo_collection::UtxoCollection,
    acceptance_data::{AcceptanceData, MergesetBlockAcceptanceData, TxEntry},
};
use kaspa_hashes::{Hash, HASH_SIZE};
use rand::{rngs::SmallRng, seq::SliceRandom, Rng};
use rand_distr::{Distribution, Normal};

use crate::testutils::generate::from_rand::{hash::generate_random_hash};

pub fn generate_random_acceptance_data(rng: &mut SmallRng, len: usize, len_std: f64,  txs_per_block_mean: TransactionIndexType, txs_per_block_std: usize, unaccepted_tx_ratio: f64) -> AcceptanceData {
    let len = min(Normal::new(len as f64, len_std).unwrap().sample(rng) as usize, 1);
    let mut acceptance_data = AcceptanceData::with_capacity(len);
    for _ in 0..len {
        acceptance_data.push(
            generate_random_mergeset_block_acceptance(
                rng, 
                Normal::new(mean_txs_per_block as f64, std_txs_per_block as f64).unwrap().sample(rng) as usize,
                Normal::new(mean_txs_per_block as f64 * unaccepted_tx_ratio, unaccpted_tx_std).unwrap().sample(rng)));
    }
    acceptance_data
}

pub fn generate_random_mergeset_block_acceptance(rng: &mut SmallRng, tx_amount: usize, unaccepted_ratio: f64) -> MergesetBlockAcceptance {
    let unaccepted_amount = (tx_amount as f64 * unaccepted_ratio) as usize;
    let accepted_amount = tx_amount - unaccepted_amount;
    MergesetBlockAcceptanceData {
        block_hash: generate_random_hash(rng),
        accepted_transactions: generate_random_tx_entries(rng, accepted_amount),
        unaccepted_transactions: generate_random_tx_entries(rng, unaccepted_amount),
        
    }
}

pub fn generate_random_tx_entries(rng: &mut SmallRng, amount: usize) -> Vec<TxEntry> {
    let mut tx_entries = Vec::with_capacity(amount);
    for _ in 0..amount {
        tx_entries.push(generate_random_tx_entry(rng));
    }
    tx_entries
}

pub fn generate_random_tx_entry(rng: &mut SmallRng) -> TxEntry {
    TxEntry::new(generate_random_hash(rng), rng.gen())
}