use kaspa_consensus_core::{tx::{Transaction, TransactionId}, header::Header, block::Block, acceptance_data::MergesetBlockAcceptanceData};
use kaspa_consensus_notify::notification::{BlockAddedNotification, VirtualChainChangedNotification};
use kaspa_hashes::Hash;
use parking_lot::RwLock;
use std::{fmt::Debug, sync::Arc};

use crate::{
    errors::TxIndexResult,
    model::{
        transaction_entries::{TxOffset, Tx},
        TxAcceptanceData, TxEntry, TxOffset, TxInclusionVerboseData, TxAcceptanceVerboseData,
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
    fn get_tx_verbose_inclusion_data(&self, tx_ids: Vec<TransactionId>) -> Arc<[(TransactionId, TxInclusionVerboseData)]>;

    fn get_tx_verbose_acceptance_data(&self, tx_ids: Vec<TransactionId>) -> Arc<[(TransactionId, TxAcceptanceVerboseData)]>;

    fn get_txs(&self, tx_ids: Vec<TransactionId>, query: TxIndexTxQuery) -> Arc<[Transaction]>;

    fn get_accepted_txs_from_block(&self, block_hash: Hash) -> Arc<[Transaction]>;

    // Update Methods   
    fn update_via_block_added(&mut self, block_hash: Hash, Transactions: Vec<Transaction>) -> TxIndexResult<()>;

    fn update_via_vspcc(
        &mut self, new_sink: Hash, 
        blocks_added: Vec<Hash>, blocks_removed: Vec<Hash>, 
        mergeset_added: MergesetBlockAcceptanceData, 
        mergeset_removed: MergesetBlockAcceptanceData
    ) -> TxIndexResult<()>;

    fn update_via_block_removed(&mut self, block_hash: Hash, history_root: Hash, Transactions: Vec<Transaction>) -> TxIndexResult<()>;
}

pub type DynTxIndexApi = Option<Arc<RwLock<dyn TxIndexApi>>>;
