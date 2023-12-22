use kaspa_consensus_core::BlockHasher;
use kaspa_database::{
    prelude::{CachedDbAccess, DirectDbWriter, StoreResult, DB},
    registry::DatabaseStorePrefixes,
};
use kaspa_hashes::Hash;
use std::sync::Arc;

// Traits:

pub trait TxIndexMergedBlockAcceptanceReader {
    /// Get [`TransactionOffset`] queried by [`TransactionId`],
    fn get(&self, block_hash: Hash) -> StoreResult<Hash>;
    fn has(&self, block_hash: Hash) -> StoreResult<bool>;
}

pub trait TxIndexMergedBlockAcceptanceStore: TxIndexMergedBlockAcceptanceReader {
    fn remove_many(&mut self, block_hashes: Vec<Hash>) -> StoreResult<()>;
    fn insert_many(&mut self, accepting_block_hash: Hash, accepted_hashes: Vec<Hash>) -> StoreResult<()>;
    fn delete_all(&mut self) -> StoreResult<()>;
}

// Implementations:

#[derive(Clone)]
pub struct DbTxIndexMergedBlockAcceptanceStore {
    db: Arc<DB>,
    access: CachedDbAccess<Hash, Hash, BlockHasher>,
}

impl DbTxIndexMergedBlockAcceptanceStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self {
            db: Arc::clone(&db),
            access: CachedDbAccess::new(db, cache_size, DatabaseStorePrefixes::TxIndexMergedBlockAcceptance.into()),
        }
    }
}

impl TxIndexMergedBlockAcceptanceReader for DbTxIndexMergedBlockAcceptanceStore {
    fn get(&self, block_hash: Hash) -> StoreResult<Hash> {
        self.access.read(block_hash)
    }

    fn has(&self, block_hash: Hash) -> StoreResult<bool> {
        self.access.has(block_hash)
    }
}

impl TxIndexMergedBlockAcceptanceStore for DbTxIndexMergedBlockAcceptanceStore {
    fn remove_many(&mut self, block_hashes: Vec<Hash>) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        self.access.delete_many(writer, &mut block_hashes.into_iter())
    }

    fn insert_many(&mut self, accepting_block_hash: Hash, accepted_hashes: Vec<Hash>) -> StoreResult<()> {
        let mut writer: DirectDbWriter = DirectDbWriter::new(&self.db);

        self.access.write_many(
            writer,
            &mut accepted_hashes.iter().map(|accepted_hash| (*accepted_hash, accepting_block_hash.into())),
        )
    }

    /// Removes all Offset in the cache and db, besides prefixes themselves.
    fn delete_all(&mut self) -> StoreResult<()> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.delete_all(&mut writer)
    }
}
