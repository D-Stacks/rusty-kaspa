use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;

pub use conn_builder::ConnBuilder;
use kaspa_utils::fd_budget::FDGuard;

use crate::prelude::StoreResult;

mod conn_builder;

/// The DB type used for Kaspad stores
pub struct DB {
    inner: DBWithThreadMode<MultiThreaded>,
    _fd_guard: FDGuard,
}

impl DB {
    pub fn new(inner: DBWithThreadMode<MultiThreaded>, fd_guard: FDGuard) -> Self {
        Self { inner, _fd_guard: fd_guard }
    }

    // Useful for testing if a key exists, and if it doesn't perform initialization logic
    // Such as in cases when a new store is created for the node.
    pub fn has_key(&self, key: &[u8]) -> StoreResult<bool> {
        Ok(self.inner.get_pinned(key)?.is_some())
    }
}

impl DerefMut for DB {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Deref for DB {
    type Target = DBWithThreadMode<MultiThreaded>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Deletes an existing DB if it exists
pub fn delete_db(db_dir: PathBuf) {
    if !db_dir.exists() {
        return;
    }
    let options = rocksdb::Options::default();
    let path = db_dir.to_str().unwrap();
    <DBWithThreadMode<MultiThreaded>>::destroy(&options, path).expect("DB is expected to be deletable");
}
