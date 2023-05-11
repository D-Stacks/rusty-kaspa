use crate::model::{transaction_offsets::TransactionOffset, params::TxIndexParams};

use kaspa_consensus_core::{tx::{TransactionId, TransactionEntry}, block::Block};
use kaspa_database::prelude::{CachedDbAccess, DirectDbWriter, StoreResult, DB}; 
use kaspa_hashes::Hash;
use std::sync::Arc;

// Prefixes:

/// Prefixes the [`TransactionId`] indexed [`TransactionOffset`] store.
pub const STORE_PREFIX: &[u8] = b"txindex-included-offsets";

// Traits:

pub trait IncludedTxOffsetStoreReader {
    /// Get [`TransactionOffset`] queried by [`TransactionId`],
    fn get(&self, transaction_id: TransactionId) -> StoreResult<TransactionOffset>;
    fn has(&self, transaction_id: TransactionId) -> StoreResult<bool>;
}

pub trait IncludedTxOffsetStore: IncludedTxOffsetStoreReader {
    fn remove_many(&mut self, transaction_ids: Vec<TransactionId>) -> StoreResult<()>;
    fn insert_many_from_block(&mut self, including_block: Hash, transaction_entries: Vec<TransactionEntry>) -> StoreResult<()>;
     
    fn delete_all(&mut self) -> StoreResult<()>;
}

// Implementations:

#[derive(Clone)]
pub struct DbIncludedTxOffsetStore {
    db: Arc<DB>,
    access: CachedDbAccess<TransactionId, TransactionOffset>,
}

impl DbIncludedTxOffsetStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_size, STORE_PREFIX) }
    }
}

impl IncludedTxOffsetStoreReader for DbIncludedTxOffsetStore {   
    fn get(&self, transaction_id: TransactionId) -> StoreResult<TransactionOffset> {
        self.access.read(transaction_id)
    }

    fn has(&self, transaction_id: TransactionId) -> StoreResult<bool> {
        self.access.has(transaction_id)
    }
}

impl IncludedTxOffsetStore for DbIncludedTxOffsetStore {
    fn remove_many(&mut self, mut transaction_ids: Vec<TransactionId>) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        self.access.delete_many(writer, &mut transaction_ids.into_iter())
    }

    fn insert_many_from_block(&mut self, block: Block) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        let mut to_add = block.transactions
        .into_iter()
        .enumerate()
        .filter_map(move |(i,transaction)| {
            if self.has(transaction.id()) { // no need to re-write
                Some((
                transaction.id(),
                TransactionOffset {
                    including_block: block.hash(),
                    transaction_index: i as TransactionIndexType,
                }
            ))
            } else {
                    None
            }
        });
        

        self.access.write_many(writer, &mut to_add)

    }

    /// Removes all Offset in the cache and db, besides prefixes themselves.
    fn delete_all(&mut self) -> StoreResult<()> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.delete_all(&mut writer)
    }
}
