use kaspa_consensus_core::tx::TransactionId;
use kaspa_consensusmanager::spawn_blocking;
use kaspa_hashes::Hash;
use kaspa_index_core::notification::{ChainAcceptanceDataPrunedNotification, VirtualChainChangedNotification};
use parking_lot::RwLock;
use std::{fmt::Debug, sync::Arc};

use crate::{
    errors::TxIndexResult,
    model::{BlockAcceptanceOffset, TxOffset},
};

pub trait TxIndexApi: Send + Sync + Debug {
    // Resync methods.
    fn resync(&mut self) -> TxIndexResult<()>;

    // Sync state methods
    fn is_synced(&self) -> TxIndexResult<bool>;

    fn get_merged_block_acceptance_offset(&self, hashes: Vec<Hash>) -> TxIndexResult<Arc<Vec<Option<BlockAcceptanceOffset>>>>;

    fn get_tx_offsets(&self, tx_ids: Vec<TransactionId>) -> TxIndexResult<Arc<Vec<Option<TxOffset>>>>;

    fn update_via_vspcc_added(&mut self, vspcc_notification: VirtualChainChangedNotification) -> TxIndexResult<()>;

    fn update_via_chain_acceptance_data_pruned(
        &mut self,
        chain_acceptance_data_pruned: ChainAcceptanceDataPrunedNotification,
    ) -> TxIndexResult<()>;
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

    pub async fn get_tx_offsets(self, tx_ids: Vec<TransactionId>) -> TxIndexResult<Arc<Vec<Option<TxOffset>>>> {
        spawn_blocking(move || self.inner.read().get_tx_offsets(tx_ids)).await.unwrap()
    }

    pub async fn get_merged_block_acceptance_offset(
        self,
        hashes: Vec<Hash>,
    ) -> TxIndexResult<Arc<Vec<Option<BlockAcceptanceOffset>>>> {
        spawn_blocking(move || self.inner.read().get_merged_block_acceptance_offset(hashes)).await.unwrap()
    }

    pub async fn update_via_vspcc_added(self, vspcc_notification: VirtualChainChangedNotification) -> TxIndexResult<()> {
        spawn_blocking(move || self.inner.write().update_via_vspcc_added(vspcc_notification)).await.unwrap().into()
    }

    pub async fn update_via_chain_acceptance_data_pruned(
        self,
        chain_acceptance_data_pruned_notification: ChainAcceptanceDataPrunedNotification,
    ) -> TxIndexResult<()> {
        spawn_blocking(move || self.inner.write().update_via_chain_acceptance_data_pruned(chain_acceptance_data_pruned_notification))
            .await
            .unwrap()
            .into()
    }
}
impl TxIndexProxy {
    pub fn new(inner: Arc<RwLock<dyn TxIndexApi>>) -> Self {
        Self { inner }
    }

    pub async fn get_tx_verbose_inclusion_data(self) -> StoreResult<u64> {
        spawn_blocking(move || self.inner.read().get_tx_verbose_acceptance_data(tx_ids)).await.unwrap()
    }

    pub async fn get_tx_verbose_acceptance_data(
        self,
        tx_ids: &[TransactionId],
    ) -> StoreResult<Arc<[(TransactionId, TxAcceptanceVerboseData)]>> {
        spawn_blocking(move || self.inner.read().get_tx_verbose_acceptance_data(tx_ids)).await.unwrap()
    }

    pub async fn get_txs(self, tx_ids: &[TransactionId], query: TxIndexTxQuery) -> StoreResult<Arc<[Transaction]>> {
        spawn_blocking(move || self.inner.read().get_txs(tx_ids, query)).await.unwrap()
    }

    pub async fn update_via_vspcc_added(self, vspcc_notification: VirtualChainChangedNotification) -> StoreResult<()> {
        spawn_blocking(move || self.inner.write().update_via_vspcc_added(vspcc_notification)).await.unwrap()
    }

    pub async fn update_via_block_block_body_pruned(
        self,
        chain_acceptance_data_pruned_notification: ChainAcceptanceDataPrunedNotification,
    ) -> StoreResult<()> {
        spawn_blocking(move || self.inner.write().update_via_block_block_body_pruned(chain_acceptance_data_pruned_notification))
            .await
            .unwrap()
    }
}
