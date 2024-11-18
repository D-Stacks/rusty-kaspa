use std::sync::Arc;

use kaspa_consensus_core::tx::{Transaction, TransactionId};
use kaspa_database::prelude::CachePolicy;
use kaspa_database::prelude::StoreError;
use kaspa_database::prelude::DB;
use kaspa_database::prelude::{BatchDbWriter, CachedDbAccess, DirectDbWriter};
use kaspa_database::registry::DatabaseStorePrefixes;
use rocksdb::WriteBatch;

/// Store for holding the UTXO difference (delta) of a block relative to its selected parent.
/// Note that this data is lazy-computed only for blocks which are candidates to being chain
/// blocks. However, once the diff is computed, it is permanent. This store has a relation to
/// block status, such that if a block has status `StatusUTXOValid` then it is expected to have
/// utxo diff data as well as utxo multiset data and acceptance data.

pub trait TransactionsStoreReader {
    fn get(&self, tx_id: TransactionId) -> Result<Arc<Transaction>, StoreError>;
}

pub trait TransactionsStore: TransactionsStoreReader {
    fn insert(&self, tx_id: TransactionId, transaction: Arc<Transaction>) -> Result<(), StoreError>;
    fn insert_many(&self, tx_ids: &[(TransactionId, Arc<Transaction>)]) -> Result<(), StoreError>;
    fn delete_many(&self, tx_ids: &[(TransactionId, Arc<Transaction>)]) -> Result<(), StoreError>;
}

/// A DB + cache implementation of `UtxoDifferencesStore` trait, with concurrency support.
#[derive(Clone)]
pub struct DbTransactionsStore {
    db: Arc<DB>,
    access: CachedDbAccess<TransactionId, Arc<Transaction>>,
}

impl DbTransactionsStore {
    pub fn new(db: Arc<DB>, cache_policy: CachePolicy) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_policy, DatabaseStorePrefixes::Transactions.into()) }
    }

    pub fn clone_with_new_cache(&self, cache_policy: CachePolicy) -> Self {
        Self::new(Arc::clone(&self.db), cache_policy)
    }

    pub fn insert_batch(&self, batch: &mut WriteBatch, tx_id: TransactionId, transaction: Arc<Transaction>) -> Result<(), StoreError> {
        if self.access.has(tx_id)? {
            return Ok(());
        }
        self.access.write(BatchDbWriter::new(batch), tx_id, transaction)?;
        Ok(())
    }

    pub fn delete_batch(&self, batch: &mut WriteBatch, tx_id: TransactionId) -> Result<(), StoreError> {
        self.access.delete(BatchDbWriter::new(batch), tx_id)
    }
}

impl TransactionsStoreReader for DbTransactionsStore {
    fn get(&self, tx_id: TransactionId) -> Result<Arc<Transaction>, StoreError> {
        self.access.read(tx_id)
    }
}

impl TransactionsStore for DbTransactionsStore {
    fn insert(&self, tx_id: TransactionId, transaction: Arc<Transaction>) -> Result<(), StoreError> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.write(&mut writer, tx_id, transaction)?;
        Ok(())
    }
    fn insert_many(&self, tx_ids: &[(TransactionId, Arc<Transaction>)]) -> Result<(), StoreError> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.write_many(&mut writer, &mut tx_ids.iter().map(|(tx_id, tx)| (*tx_id, Arc::clone(tx))))?;
        Ok(())
    }

    fn delete_many(&self, tx_ids: &[(TransactionId, Arc<Transaction>)]) -> Result<(), StoreError> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.delete_many(&mut writer, &mut tx_ids.iter().map(|(tx_id, _)| *tx_id))?;
        Ok(())
    }
}
