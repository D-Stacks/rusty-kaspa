use kaspa_consensus_core::tx::TransactionId;
use kaspa_consensus_core::BlockHasher;
use kaspa_database::prelude::CachePolicy;
use kaspa_database::prelude::StoreError;
use kaspa_database::prelude::DB;
use kaspa_database::prelude::{BatchDbWriter, CachedDbAccess, DirectDbWriter};
use kaspa_database::registry::DatabaseStorePrefixes;
use kaspa_hashes::Hash;
use kaspa_utils::mem_size::MemSizeEstimator;
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub trait BlockTransactionsStoreReader {
    fn get(&self, hash: Hash) -> Result<Arc<Vec<TransactionId>>, StoreError>;
}

pub trait BlockTransactionsStore: BlockTransactionsStoreReader {
    // This is append only
    fn insert(&self, hash: Hash, transactions: Arc<Vec<TransactionId>>) -> Result<(), StoreError>;
    fn delete(&self, hash: Hash) -> Result<(), StoreError>;
}

#[derive(Clone, Serialize, Deserialize)]
struct BlockBody(Arc<Vec<TransactionId>>);

impl MemSizeEstimator for BlockBody {
    fn estimate_mem_bytes(&self) -> usize {
        self.0.len() * std::mem::size_of::<TransactionId>() + std::mem::size_of::<Arc<Vec<TransactionId>>>()
    }
}

/// A DB + cache implementation of `BlockTransactionsStore` trait, with concurrency support.
#[derive(Clone)]
pub struct DbBlockTransactionsStore {
    db: Arc<DB>,
    access: CachedDbAccess<Hash, BlockBody, BlockHasher>,
}

impl DbBlockTransactionsStore {
    pub fn new(db: Arc<DB>, cache_policy: CachePolicy) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_policy, DatabaseStorePrefixes::BlockTransactions.into()) }
    }

    pub fn clone_with_new_cache(&self, cache_policy: CachePolicy) -> Self {
        Self::new(Arc::clone(&self.db), cache_policy)
    }

    pub fn has(&self, tx_id: TransactionId) -> Result<bool, StoreError> {
        self.access.has(tx_id)
    }

    pub fn insert_batch(&self, batch: &mut WriteBatch, hash: Hash, transactions: Arc<Vec<TransactionId>>) -> Result<(), StoreError> {
        if self.access.has(hash)? {
            return Err(StoreError::HashAlreadyExists(hash));
        }
        self.access.write(BatchDbWriter::new(batch), hash, BlockBody(transactions))?;
        Ok(())
    }

    pub fn delete_batch(&self, batch: &mut WriteBatch, hash: Hash) -> Result<(), StoreError> {
        self.access.delete(BatchDbWriter::new(batch), hash)
    }
}

impl BlockTransactionsStoreReader for DbBlockTransactionsStore {
    fn get(&self, hash: Hash) -> Result<Arc<Vec<TransactionId>>, StoreError> {
        Ok(self.access.read(hash)?.0)
    }
}

impl BlockTransactionsStore for DbBlockTransactionsStore {
    fn insert(&self, hash: Hash, transaction_ids: Arc<Vec<TransactionId>>) -> Result<(), StoreError> {
        if self.access.has(hash)? {
            return Err(StoreError::HashAlreadyExists(hash));
        }
        self.access.write(DirectDbWriter::new(&self.db), hash, BlockBody(transaction_ids))?;
        Ok(())
    }

    fn delete(&self, hash: Hash) -> Result<(), StoreError> {
        self.access.delete(DirectDbWriter::new(&self.db), hash)
    }
}
