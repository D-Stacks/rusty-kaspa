use itertools::Itertools;
use kaspa_consensus_core::{
    block::Block,
    header::Header,
    subnets::SubnetworkId,
    tx::{ScriptPublicKey, ScriptVec, Transaction, TransactionInput, TransactionOutpoint, TransactionOutput, UtxoEntry},
    utxo::utxo_collection::UtxoCollection,
    acceptance_data::{AcceptanceData, MergesetBlockAcceptanceData, TxEntry},
};
use rand::{rngs::SmallRng, seq::SliceRandom};

use crate::testutils::generate::from_rand::{hash::generate_random_hash};

pub fn generate_random_acceptance_data(rng: &mut SmallRng, len: usize, txs_per_block: TransactionIndexType, unaccepted_tx_ratio: f64) -> AcceptanceData {
    let mut acceptance_data = AcceptanceData::with_capacity(len);
    for _ in 0..(len - 1) {
        acceptance_data.push(
            generate_random_mergeset_block_acceptance(
                rng, 
                txs_per_block_mean,
                unaccepted_tx_ratio;
            )
        );
    };
    acceptance_data
}

pub fn generate_random_mergeset_block_acceptance(rng: &mut SmallRng, tx_amount: usize, unaccepted_ratio: f64) -> MergesetBlockAcceptance {
    let indexes = (0..tx_amount).collect_vec();
    indexes.shuffle(rng);
    let unaccepted_amount = (tx_amount as f64 * unaccepted_ratio) as usize;
    let unaccepted_indexes = &indexes[..unaccepted_amount];
    let accepted_indexes = &indexes[unaccepted_amount..];
    MergesetBlockAcceptanceData {
        block_hash: generate_random_hash(rng),
        accepted_transactions: generate_random_tx_entries(rng, &accepted_indexes),
        unaccepted_transactions: generate_random_tx_entries(rng, &unaccepted_indexes),
    }
}

pub fn generate_random_tx_entries(rng: &mut SmallRng, indexes: &[usize]) -> Vec<TxEntry> {
    let mut tx_entries = Vec::with_capacity(indexes.len());
    for i in indexes.into_iter() {
        tx_entries.push(generate_random_tx_entry_with_index(rng, *i));
    }
    tx_entries
}

pub fn generate_random_tx_entry_with_index(rng: &mut SmallRng, index: usize) -> TxEntry {
    TxEntry { transaction_id: generate_random_hash(rng), index_within_block: index as TransactionIndexType }
}