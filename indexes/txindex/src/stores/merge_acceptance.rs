use crate::model::{transaction_offsets::TransactionOffset, merge_acceptance::MergeAcceptance, MergeAcceptanceByBlock};

use kaspa_consensus_core::{tx::TransactionId, acceptance_data::{MergesetBlockAcceptanceData, BlockAcceptanceData, AcceptanceData}, BlockHashMap};
use kaspa_database::prelude::{CachedDbAccess, DirectDbWriter, StoreResult, DB};
use kaspa_hashes::Hash;
use std::{sync::Arc, collections::btree_set::Iter};

// Prefixes:

/// Prefixes the [`TransactionId`] indexed [`TransactionOffset`] store.
pub const STORE_PREFIX: &[u8] = b"txindex-merge-acceptance";

// Traits:

pub trait MergeAcceptanceStoreReader {
    /// Get [`TransactionOffset`] queried by [`TransactionId`],
    fn get(&self, hash: Hash) -> StoreResult<MergeAcceptance>;
    fn has(&self, hash: Hash) -> StoreResult<bool>;
}

pub trait MergeAcceptanceStore: MergeAcceptanceStoreReader {
    fn remove_many(&mut self, merged_hashes: Vec<Hash>) -> StoreResult<()>;
    fn insert_many(&mut self, merge_acceptance_by_block: Vec<(Hash, MergeAcceptance)>) -> StoreResult<()>;
     
    fn delete_all(&mut self) -> StoreResult<()>;
}

// Implementations:

#[derive(Clone)]
pub struct DbMergeAcceptanceStore {
    db: Arc<DB>,
    access: CachedDbAccess<Hash, MergeAcceptance, BlockHasher>,
}

impl DbMergeAcceptanceStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_size, STORE_PREFIX.to_vec()) }
    }
}

impl MergeAcceptanceStoreReader for DbMergeAcceptanceStore {   
    fn get(&self, hash: Hash) -> StoreResult<MergeAcceptance> {
        self.access.read(hash)
    }

    fn has(&self, hash: Hash) -> StoreResult<bool> {
        self.access.has(transaction_id)
    }
}

impl MergeAcceptanceStore for DbMergeAcceptanceStore {
    fn remove_many(&mut self, merged_hashes: Vec<Hash>) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        self.access.delete_many(writer, &mut merged_hashes.into_iter())
    }

    fn insert_many(&mut self, merge_acceptance_by_block: Vec<(Hash, MergeAcceptance)>) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);
        
        self.access.write_many(writer, &mut merge_acceptance_by_block.into_iter())
    }

    /// Removes all Offset in the cache and db, besides prefixes themselves.
    fn delete_all(&mut self) -> StoreResult<()> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.delete_all(&mut writer)
    }
}
