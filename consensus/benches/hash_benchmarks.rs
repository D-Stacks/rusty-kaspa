use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::{collections::hash_map::DefaultHasher, str::FromStr};
use kaspa_consensus_core::{TxHasher, tx::TransactionId, BlockHasher};
use kaspa_hashes::Hash as KHash;
use std::hash::{Hash, Hasher};

/// Placeholder for actual benchmarks
pub fn hash_benchmark(c: &mut Criterion) {
    c.bench_function("Hash::from_str", |b| {
        let hash_str = "8e40af02265360d59f4ecf9ae9ebf8f00a3118408f5a9cdcbcc9c0f93642f3af";
        b.iter(|| KHash::from_str(black_box(hash_str)))
    });
}

/// bench [`DefaultHasher`] for [`hashes::Hash`],
pub fn default_hasher_hash_benchmark(c: &mut Criterion) {
    c.bench_function("hash.hash (DefaultHasher)", |b| {
        let mut hasher = DefaultHasher::new();
        let hash = KHash::from_str("8e40af02265360d59f4ecf9ae9ebf8f00a3118408f5a9cdcbcc9c0f93642f3af").unwrap();
        b.iter(|| hash.hash(black_box(&mut hasher)));
    });
}

/// bench [`BlockHasher`] for [`hashes::Hash`],
pub fn block_hasher_hash_benchmark(c: &mut Criterion) {
    c.bench_function("hash.hash (BlockHasher)", |b| {
        let mut hasher = BlockHasher::new();
        let hash = KHash::from_str("8e40af02265360d59f4ecf9ae9ebf8f00a3118408f5a9cdcbcc9c0f93642f3af").unwrap();
        b.iter(|| hash.hash(black_box(&mut hasher)));
    });
}

/// bench [`DefaultHasher`] for [`TransactionId`],
pub fn default_hasher_transaction_id(c: &mut Criterion) {
    c.bench_function("tx_id.hash(DefaultHasher)", |b| {
        let mut hasher = DefaultHasher::new();
        let tx_id = TransactionId::from_str("8e40af02265360d59f4ecf9ae9ebf8f00a3118408f5a9cdcbcc9c0f93642f3af").unwrap();
        b.iter(|| tx_id.hash(black_box(&mut hasher)));
    });
}

/// bench [`TxHasher`] for [`TransactionId`],
pub fn tx_hasher_transaction_id(c: &mut Criterion) {
    c.bench_function("tx_id.hash(TxHasher)", |b| {
        let associated_block_hash: KHash = KHash::from_str("ab1ce86bf09864c5c24c5dcc279c22d461cf0d4be6cbdbe5ed7d53a36862c3f1").unwrap();
        let mut hasher = TxHasher::new(associated_block_hash);
        let tx_id = TransactionId::from_str("8e40af02265360d59f4ecf9ae9ebf8f00a3118408f5a9cdcbcc9c0f93642f3af").unwrap();
        b.iter(|| tx_id.hash(black_box(&mut hasher)));
    });
}

criterion_group!(
    benches,
    default_hasher_transaction_id,
    tx_hasher_transaction_id,
    hash_benchmark,
    default_hasher_hash_benchmark,
    block_hasher_hash_benchmark,
);

criterion_main!(benches);

