use std::sync::Arc;

use kaspa_consensus_core::BlockHasher;
use kaspa_database::prelude::StoreError;
use kaspa_database::prelude::DB;
use kaspa_database::prelude::{BatchDbWriter, CachedDbAccess, DirectDbWriter};
use kaspa_hashes::Hash;
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};

pub trait DepthStoreReader {
    fn merge_depth_root(&self, hash: Hash) -> Result<Hash, StoreError>;
    fn finality_point(&self, hash: Hash) -> Result<Hash, StoreError>;
}

pub trait DepthStore: DepthStoreReader {
    // This is append only
    fn insert(&self, hash: Hash, merge_depth_root: Hash, finality_point: Hash) -> Result<(), StoreError>;
    fn delete(&self, hash: Hash) -> Result<(), StoreError>;
}

const STORE_PREFIX: &[u8] = b"block-at-depth";

#[derive(Clone, Copy, Serialize, Deserialize)]
struct BlockDepthInfo {
    merge_depth_root: Hash,
    finality_point: Hash,
}

/// A DB + cache implementation of `DepthStore` trait, with concurrency support.
#[derive(Clone)]
pub struct DbDepthStore {
    db: Arc<DB>,
    access: CachedDbAccess<Hash, BlockDepthInfo, BlockHasher>,
}

impl DbDepthStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_size, STORE_PREFIX.to_vec()) }
    }

    pub fn clone_with_new_cache(&self, cache_size: u64) -> Self {
        Self::new(Arc::clone(&self.db), cache_size)
    }

    pub fn insert_batch(
        &self,
        batch: &mut WriteBatch,
        hash: Hash,
        merge_depth_root: Hash,
        finality_point: Hash,
    ) -> Result<(), StoreError> {
        if self.access.has(hash)? {
            return Err(StoreError::HashAlreadyExists(hash));
        }
        self.access.write(BatchDbWriter::new(batch), hash, BlockDepthInfo { merge_depth_root, finality_point })?;
        Ok(())
    }

    pub fn delete_batch(&self, batch: &mut WriteBatch, hash: Hash) -> Result<(), StoreError> {
        self.access.delete(BatchDbWriter::new(batch), hash)
    }
}

impl DepthStoreReader for DbDepthStore {
    fn merge_depth_root(&self, hash: Hash) -> Result<Hash, StoreError> {
        Ok(self.access.read(hash)?.merge_depth_root)
    }

    fn finality_point(&self, hash: Hash) -> Result<Hash, StoreError> {
        Ok(self.access.read(hash)?.finality_point)
    }
}

impl DepthStore for DbDepthStore {
    fn insert(&self, hash: Hash, merge_depth_root: Hash, finality_point: Hash) -> Result<(), StoreError> {
        if self.access.has(hash)? {
            return Err(StoreError::HashAlreadyExists(hash));
        }
        self.access.write(DirectDbWriter::new(&self.db), hash, BlockDepthInfo { merge_depth_root, finality_point })?;
        Ok(())
    }

    fn delete(&self, hash: Hash) -> Result<(), StoreError> {
        self.access.delete(DirectDbWriter::new(&self.db), hash)
    }
}
