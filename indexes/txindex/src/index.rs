use std::sync::Arc;

use kaspa_consensus::model::stores::DB;
use kaspa_consensus_core::config::Config as ConsensusConfig;
use kaspa_consensusmanager::ConsensusManager;

use crate::{
    store_manager::StoreManager,
    core::api::TxIndexApi,
    core::errors::TxIndexResult,
    core::model::BlockAcceptanceOffset, config::TxIndexConfig,
};

pub struct TxIndexSyncState {
    pub is_sink_synced: bool,
    pub is_history_root_synced: bool,
}

pub struct TxIndex {
    store_manager: StoreManager,
    consensus_manager: ConsensusManager, 
    txindex_config: TxIndexConfig, 
    consensus_config: ConsensusConfig
}

impl TxIndex {
    fn new(
        consensus_manager: ConsensusManager, 
        consensus_db: Arc<DB>, 
        txindex_db: Arc<DB>, 
        txindex_config: TxIndexConfig, 
        consensus_config: ConsensusConfig
    ) -> Self {
        Self {
            store_manager: StoreManager::new(consensus_db, txindex_db),
            consensus_manager,
            txindex_config,
            consensus_config
        }
}
}

impl TxIndexApi for TXIndex {
    // Resync methods. 
    fn resync(&mut self) -> TxIndexResult<()> {
        todo!()
    }

    // Sync state methods
    fn is_synced(&self) -> TxIndexResult<TxIndexSyncState> {
        todo!()
    }

    // Retrieval Methods
    fn get_tx_verbose_inclusion_data(
        &self, 
        tx_ids: &[TransactionId]
    ) -> TxIndexResult<Arc<[(TransactionId, TxInclusionVerboseData)]>> 
    {
        todo!()
    }

    fn get_tx_verbose_acceptance_data(
        &self, 
        tx_ids: &[TransactionId]
    ) -> TxIndexResult<Arc<[(TransactionId, TxAcceptanceVerboseData)]>> {
        todo!()
    }

    fn get_txs(
        &self, 
        tx_ids: &[TransactionId], 
        query: TxIndexTxQuery
    ) -> TxIndexResult<Arc<[Transaction]>> {
        todo!()
    }

    // Update methods
    fn update_via_vspcc_added(
        &mut self,
        vspcc_notification: VirtualChainChangedNotification,
    ) -> TxIndexResult<()> {
        todo!()
    }

    fn update_via_chain_acceptance_data_pruned(
        &mut self, 
        chain_acceptance_data_pruned: ChainAcceptanceDataPrunedNotification
    ) -> TxIndexResult<()> {
        todo!()
    }
}

impl Debug for TxIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxIndex").finish()
    }
}

struct TxIndexConsensusResetHandler {
    txindex: Weak<RwLock<TxIndex>>,
}

impl TxIndexConsensusResetHandler {
    fn new(txindex: Weak<RwLock<UtxoIndex>>) -> Self {
        Self { txindex }
    }
}

impl ConsensusResetHandler for TxIndexConsensusResetHandler {
    fn handle_consensus_reset(&self) {
        if let Some(txindex) = self.txindex.upgrade() {
            txindex.write().resync().unwrap();
        }
    }
}