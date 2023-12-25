use kaspa_consensus_core::{tx::{Transaction, TransactionId}, acceptance_data::MergesetBlockAcceptanceData};
use kaspa_consensus_notify::notification::{BlockAddedNotification, ChainAcceptanceDataPrunedNotification};
use kaspa_hashes::Hash;
use parking_lot::RwLock;
use std::{fmt::Debug, sync::Arc};

use crate::{
    errors::TxIndexResult,
    model::{TxInclusionVerboseData, TxAcceptanceVerboseData,
    },
};
pub enum TxIndexTxQuery {
    Unaccepted,
    Accepted,
    AcceptedAndUnaccepted,
}

pub trait TxIndexApi: Send + Sync + Debug {
    // Resync methods. 
    fn resync(&mut self) -> TxIndexResult<()>;

    // Sync state methods
    fn is_synced(&self) -> TxIndexResult<bool>;

    // Retrieval Methods
    fn get_tx_verbose_inclusion_data(&self, tx_ids: &[TransactionId]) -> TxIndexResult<Arc<[(TransactionId, TxInclusionVerboseData)]>>;

    fn get_tx_verbose_acceptance_data(&self, tx_ids: &[TransactionId]) -> TxIndexResult<Arc<[(TransactionId, TxAcceptanceVerboseData)]>>;

    fn get_txs(&self, tx_ids: &[TransactionId], query: TxIndexTxQuery) -> TxIndexResult<Arc<[Transaction]>>;

    fn update_via_vspcc_added(
        &mut self,
        vspcc_notification: VirtualChainChangedNotification,
    ) -> TxIndexResult<()>;

    fn update_via_block_block_body_pruned(&mut self, chain_acceptance_data_pruned: ChainAcceptanceDataPrunedNotification) -> TxIndexResult<()>;
}

/// Async proxy for the UTXO index
#[derive(Debug, Clone)]
pub struct TxIndexProxy {
    inner: Arc<RwLock<dyn TxIndexApi>>,
}

pub type DynTxIndexApi = Option<Arc<RwLock<dyn TxIndexApi>>>;

impl TxIndexProxy {
    pub fn new(inner: Arc<RwLock<dyn TxIndexApi>>) -> Self {
        Self { inner }
    }

    pub async fn get_tx_verbose_inclusion_data(self) -> StoreResult<u64> {
        spawn_blocking(move || self.inner.read().get_tx_verbose_acceptance_data(tx_ids)).await.unwrap()
    }

    pub async fn get_tx_verbose_acceptance_data(self, tx_ids: &[TransactionId]) -> StoreResult<Arc<[(TransactionId, TxAcceptanceVerboseData)]>> {
        spawn_blocking(move || self.inner.read().get_tx_verbose_acceptance_data(tx_ids)).await.unwrap()
    }

    pub async fn get_txs(self, tx_ids: &[TransactionId], query: TxIndexTxQuery) -> StoreResult<Arc<[Transaction]>> {
        spawn_blocking(move || self.inner.read().get_txs(tx_ids, query)).await.unwrap()
    }

    pub async fn update_via_vspcc_added(
        self,
        vspcc_notification: VirtualChainChangedNotification,
    ) -> StoreResult<()> {
        spawn_blocking(move || self.inner.write().update_via_vspcc_added(vspcc_notification)).await.unwrap()
    }

    pub async fn update_via_block_block_body_pruned(self, chain_acceptance_data_pruned_notification: ChainAcceptanceDataPrunedNotification) -> StoreResult<()> {
        spawn_blocking(move || self.inner.write().update_via_block_block_body_pruned(chain_acceptance_data_pruned_notification)).await.unwrap()
    }
    
}