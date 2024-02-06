use crate::{
    stores::accepting_blue_score::{ConfIndexAcceptingBlueScoreStore, DbConfIndexAcceptingBlueScoreStore},
    ConfIndexResult, IDENT,
};
use kaspa_core::trace;
use kaspa_database::prelude::{CachePolicy, DB};
use rocksdb::WriteBatch;
use std::sync::Arc;

pub struct StoreManager {
    pub accepting_blue_score_store: DbConfIndexAcceptingBlueScoreStore,
    db: Arc<DB>,
}

impl StoreManager {
    pub fn new(confindex_db: Arc<DB>) -> Self {
        Self {
            accepting_blue_score_store: DbConfIndexAcceptingBlueScoreStore::new(
                confindex_db.clone(),
                CachePolicy::Empty, // this db should only read from the rocks-db, due to working with ranges this should be more efficient then extensive hashing of u64, and as such shouldn't be cached.
            ),
            db: confindex_db,
        }
    }

    pub fn delete_all(&mut self) -> ConfIndexResult<()> {
        let mut batch = WriteBatch::default();
        trace!("[{0}] attempting to clear confindex database...", IDENT);
        self.accepting_blue_score_store.delete_all(&mut batch)?;
        trace!("[{0}] clearing utxoindex database - success!", IDENT);
        Ok(())
    }

    pub fn write_batch(&self, batch: WriteBatch) -> ConfIndexResult<()> {
        Ok(self.db.write(batch)?)
    }
}