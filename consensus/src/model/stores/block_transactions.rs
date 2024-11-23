use kaspa_consensus_core::tx::Transaction;
use kaspa_consensus_core::tx::TransactionIndexType;
use kaspa_consensus_core::tx::{TransactionInput, TransactionOutput};
use kaspa_database::prelude::Cache;
use kaspa_database::prelude::CachePolicy;
use kaspa_database::prelude::StoreError;
use kaspa_database::prelude::DB;
use kaspa_database::prelude::{BatchDbWriter, CachedDbAccess, DirectDbWriter};
use kaspa_database::registry::DatabaseStorePrefixes;
use kaspa_hashes::Hash;
use kaspa_utils::mem_size::MemSizeEstimator;
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};
use std::fmt::Display;
use std::sync::Arc;

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
struct BlockTransactionFullAccessKey(#[serde_as(as = "Bytes")] [u8; 36]);

impl Display for BlockTransactionFullAccessKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl BlockTransactionFullAccessKey {
    pub fn new(block_hash: &Hash, index: TransactionIndexType) -> Self {
        let block_hash_bytes = block_hash.as_bytes();
        let index_bytes = index.to_be_bytes();
        let mut key = std::mem::MaybeUninit::uninit();
        let dest = key.as_mut_ptr() as *mut u8;
        Self(
            // unsafe, but avoids initializing array with zeros
            unsafe {
                std::ptr::copy_nonoverlapping(block_hash_bytes.as_ptr(), dest, block_hash_bytes.len());
                std::ptr::copy_nonoverlapping(index_bytes.as_ptr(), dest.add(block_hash_bytes.len()), index_bytes.len());
                key.assume_init()
            },
        )
    }
}

impl AsRef<[u8]> for BlockTransactionFullAccessKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BlockBody(Arc<Vec<Arc<Transaction>>>);

pub trait BlockTransactionsStoreReader {
    fn get(&self, block_hash: Hash) -> Result<Arc<Vec<Arc<Transaction>>>, StoreError>;
    fn get_at_index(&self, block_hash: Hash, index: TransactionIndexType) -> Result<Arc<Transaction>, StoreError>;
}

pub trait BlockTransactionsStore: BlockTransactionsStoreReader {
    // This is append only
    fn insert(&self, hash: Hash, transactions: Arc<Vec<Arc<Transaction>>>) -> Result<(), StoreError>;
    fn delete(&self, hash: Hash) -> Result<(), StoreError>;
}

impl MemSizeEstimator for BlockBody {
    fn estimate_mem_bytes(&self) -> usize {
        const NORMAL_SIG_SIZE: usize = 66;
        let (inputs, outputs) = self.0.iter().fold((0, 0), |(ins, outs), tx| (ins + tx.inputs.len(), outs + tx.outputs.len()));
        // TODO: consider tracking transactions by bytes accurately (preferably by saving the size in a field)
        // We avoid zooming in another level and counting exact bytes for sigs and scripts for performance reasons.
        // Outliers with longer signatures are rare enough and their size is eventually bounded by mempool standards
        // or in the worst case by max block mass.
        // A similar argument holds for spk within outputs, but in this case the constant is already counted through the SmallVec used within.
        inputs * (size_of::<TransactionInput>() + NORMAL_SIG_SIZE)
            + outputs * size_of::<TransactionOutput>()
            + self.0.len() * size_of::<Transaction>()
            + size_of::<Vec<Transaction>>()
            + size_of::<Self>()
    }
}

/// A DB + cache implementation of `BlockTransactionsStore` trait, with concurrency support.
#[derive(Clone)]
pub struct DbBlockTransactionsStore {
    db: Arc<DB>,
    access: CachedDbAccess<BlockTransactionFullAccessKey, Arc<Transaction>>,
    cache: Cache<Hash, BlockBody>,
}

impl DbBlockTransactionsStore {
    pub fn new(db: Arc<DB>, cache_policy: CachePolicy) -> Self {
        Self {
            db: Arc::clone(&db),
            access: CachedDbAccess::new(Arc::clone(&db), CachePolicy::Empty, DatabaseStorePrefixes::BlockTransactions.into()),
            cache: Cache::new(cache_policy),
        }
    }

    pub fn clone_with_new_cache(&self, cache_policy: CachePolicy) -> Self {
        Self::new(Arc::clone(&self.db), cache_policy)
    }

    pub fn has(&self, hash: Hash) -> Result<bool, StoreError> {
        Ok(self.cache.contains_key(&hash) || self.access.has_bucket(hash.as_bytes().as_ref())?)
    }

    pub fn insert_batch(
        &self,
        batch: &mut WriteBatch,
        hash: Hash,
        transactions: Arc<Vec<Arc<Transaction>>>,
    ) -> Result<(), StoreError> {
        if self.cache.contains_key(&hash) || self.access.has_bucket(hash.as_bytes().as_ref())? {
            return Err(StoreError::HashAlreadyExists(hash));
        }
        let writer = BatchDbWriter::new(batch);
        self.cache.insert(hash, BlockBody(transactions.clone()));
        self.access.write_many_without_cache(
            writer,
            &mut transactions
                .iter()
                .enumerate()
                .map(|(index, tx)| (BlockTransactionFullAccessKey::new(&hash, index as TransactionIndexType), tx.clone())),
        )?;
        Ok(())
    }

    pub fn delete_batch(&self, batch: &mut WriteBatch, hash: Hash) -> Result<(), StoreError> {
        self.cache.remove(&hash);
        self.access.delete_bucket(BatchDbWriter::new(batch), hash.as_bytes().as_ref())
    }
}

impl BlockTransactionsStoreReader for DbBlockTransactionsStore {
    fn get(&self, hash: Hash) -> Result<Arc<Vec<Arc<Transaction>>>, StoreError> {
        self.cache
            .get(&hash)
            .map(|block_transactions| block_transactions.0.clone())
            .ok_or_else(|| StoreError::BucketNotFound(hash.to_string()));
        if self.cache.contains_key(&hash) {
            Ok(self.cache.get(&hash).unwrap().0.clone())
        } else {
            Ok(Arc::new(self.access.read_bucket(hash.as_bytes().as_ref())?))
        }
    }

    fn get_at_index(&self, block_hash: Hash, index: TransactionIndexType) -> Result<Arc<Transaction>, StoreError> {
        Ok(if let Some(block_transactions) = self.cache.get(&block_hash) {
            block_transactions.0[index as usize].clone()
        } else {
            self.access.read(BlockTransactionFullAccessKey::new(&block_hash, index))?
        })
    }
}

impl BlockTransactionsStore for DbBlockTransactionsStore {
    fn insert(&self, hash: Hash, transactions: Arc<Vec<Arc<Transaction>>>) -> Result<(), StoreError> {
        if self.access.has_bucket(hash.as_bytes().as_ref())? {
            return Err(StoreError::HashAlreadyExists(hash));
        }
        self.cache.insert(hash, BlockBody(transactions.clone()));
        self.access.write_many(
            DirectDbWriter::new(&self.db),
            &mut transactions
                .iter()
                .enumerate()
                .map(|(index, tx)| (BlockTransactionFullAccessKey::new(&hash, index as TransactionIndexType), tx.clone())),
        )
    }

    fn delete(&self, hash: Hash) -> Result<(), StoreError> {
        self.cache.remove(&hash);
        self.access.delete_bucket(DirectDbWriter::new(&self.db), hash.as_bytes().as_ref())
    }
}
