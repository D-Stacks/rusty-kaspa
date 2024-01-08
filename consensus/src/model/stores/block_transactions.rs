use std::collections::HashSet;
use std::sync::Arc;

use kaspa_consensus_core::{BlockHashMap, HashMapCustomHasher};
use kaspa_consensus_core::tx::TransactionId;
use kaspa_consensus_core::{tx::Transaction, BlockHasher};
use kaspa_database::prelude::CachePolicy;
use kaspa_database::prelude::StoreError;
use kaspa_database::prelude::DB;
use kaspa_database::prelude::{BatchDbWriter, CachedDbAccess, DirectDbWriter};
use kaspa_database::registry::DatabaseStorePrefixes;
use kaspa_hashes::Hash;
use kaspa_utils::{arc::ArcExtensions, vec::VecExtensions};
use rocksdb::WriteBatch;

pub trait BlockTransactionsStoreReader {
    fn get(&self, hash: Hash) -> Result<Arc<Vec<Transaction>>, StoreError>;
    fn get_at_indices(&self, hash: Hash, indices: &mut [usize]) -> Result<Arc<Vec<Transaction>>, StoreError>;
    fn get_all_blocks_and_txs(&self) -> Result<BlockHashMap<HashSet<TransactionId>>, StoreError>;
}

pub trait BlockTransactionsStore: BlockTransactionsStoreReader {
    // This is append only
    fn insert(&self, hash: Hash, transactions: Arc<Vec<Transaction>>) -> Result<(), StoreError>;
    fn delete(&self, hash: Hash) -> Result<(), StoreError>;
}

/// A DB + cache implementation of `BlockTransactionsStore` trait, with concurrency support.
#[derive(Clone)]
pub struct DbBlockTransactionsStore {
    db: Arc<DB>,
    access: CachedDbAccess<Hash, Arc<Vec<Transaction>>, BlockHasher>,
}

impl DbBlockTransactionsStore {
    pub fn new(db: Arc<DB>, cache_policy: CachePolicy) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_policy, DatabaseStorePrefixes::BlockTransactions.into()) }
    }

    pub fn clone_with_new_cache(&self, cache_policy: CachePolicy) -> Self {
        Self::new(Arc::clone(&self.db), cache_policy)
    }

    pub fn has(&self, hash: Hash) -> Result<bool, StoreError> {
        self.access.has(hash)
    }

    pub fn insert_batch(&self, batch: &mut WriteBatch, hash: Hash, transactions: Arc<Vec<Transaction>>) -> Result<(), StoreError> {
        if self.access.has(hash)? {
            return Err(StoreError::HashAlreadyExists(hash));
        }
        self.access.write(BatchDbWriter::new(batch), hash, transactions)?;
        Ok(())
    }

    pub fn delete_batch(&self, batch: &mut WriteBatch, hash: Hash) -> Result<(), StoreError> {
        self.access.delete(BatchDbWriter::new(batch), hash)
    }
}

impl BlockTransactionsStoreReader for DbBlockTransactionsStore {
    fn get(&self, hash: Hash) -> Result<Arc<Vec<Transaction>>, StoreError> {
        self.access.read(hash)
    }

    fn get_at_indices(&self, hash: Hash, indices: &mut [usize]) -> Result<Arc<Vec<Transaction>>, StoreError> {
        let mut txs = self.access.read(hash)?.unwrap_or_clone();
        txs.retain_indices(indices)?;
        Ok(Arc::new(txs))
    }

    fn get_all_blocks_and_txs(&self) -> Result<BlockHashMap<HashSet<TransactionId>>, StoreError> {
        let mut all_blocks_and_txs = BlockHashMap::new();
        for res in self.access.iterator() {
            let res = res.unwrap();
            let (block_hash, txs) = (Hash::from_slice(&res.0), res.1);
            let tx_ids = txs.iter().map(|tx| tx.id()).collect::<HashSet<_>>();
            all_blocks_and_txs.insert(block_hash, tx_ids);
        }
        Ok(all_blocks_and_txs)
    }
}

impl BlockTransactionsStore for DbBlockTransactionsStore {
    fn insert(&self, hash: Hash, transactions: Arc<Vec<Transaction>>) -> Result<(), StoreError> {
        if self.access.has(hash)? {
            return Err(StoreError::HashAlreadyExists(hash));
        }
        self.access.write(DirectDbWriter::new(&self.db), hash, transactions)?;
        Ok(())
    }

    fn delete(&self, hash: Hash) -> Result<(), StoreError> {
        self.access.delete(DirectDbWriter::new(&self.db), hash)
    }
}
