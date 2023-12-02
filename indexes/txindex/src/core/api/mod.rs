use kaspa_consensus_core::tx::{Transaction, TransactionId};
use parking_lot::RwLock;
use std::{fmt::Debug, sync::Arc};

use crate::{
    errors::TxIndexResult,
    model::{
        transaction_entries::{TransactionOffset, TransactionOffsets},
        TxAcceptanceData, TxEntry, TxOffset,
    },
};

pub enum TxQuery {
    OffsetOnly = 1,
    AcceptanceOnly,
    Compact,
    FullInclusion,
    FullAcceptance,
    Full,
}

pub trait TxIndexApi: Send + Sync + Debug {
    fn sync_tips(&mut self) -> TxIndexResult<()>;

    fn sync_vspc_segment(&mut self, start_hash: Hash, end_hash: Hash) -> TxIndexResult<()>;

    fn are_tips_synced(&self) -> TxIndexResult<bool>;

    fn is_sink_synced(&self) -> TxIndexResult<bool>;

    fn is_source_synced(&self) -> TxIndexResult<bool>;

    fn sync_unaccepted_data(&mut self) -> TxIndexResult<()>;

    fn sync_acceptance_data(&mut self, start_hash: Hash, end_hash: Hash) -> TxIndexResult<()>;

    fn resync_chunk_size(&self) -> usize;

    fn get_transaction_offsets(self, transaction_ids: Vec<TransactionId>) -> TxIndexResult<Vec<TransactionOffset>>; //None option indicates transaction is not found

    fn get_transaction_acceptance_data(self, transaction_ids: Vec<TransactionId>) -> TxIndexResult<Vec<TransactionAcceptance>>; //None option indicates transaction is not found

    fn get_transactions_by_offsets(self, transaction_offsets: TransactionOffsets) -> TxIndexResult<Vec<Transaction>>; //None option indicates transaction is not found

    /// This is a convenience method which combines `get_transaction_offset` with `get_transaction_by_offset`.
    fn get_transaction_by_ids(self, transaction_ids: Vec<TransactionId>) -> TxIndexResult<Vec<Option<Transaction>>>;

    /// This is a convenience method which combines `get_transaction_offsets` with `get_transaction_by_offsets`.
    fn get_transaction_by_id(self, transaction_id: Vec<TransactionId>) -> TxIndexResult<Vec<Option<Transaction>>>;

    fn get_transaction_offset(self, transaction_id: Vec<TransactionId>) -> TxIndexResult<Option<TransactionOffset>>; //None option indicates transaction is not found

    fn get_transaction_acceptance_datum(self, transaction_id: Vec<TransactionId>) -> TxIndexResult<Option<TransactionAcceptance>>;

    fn get_transaction_by_offset(self, transaction_offset: TransactionOffset) -> TxIndexResult<Option<Transaction>>;

    fn update_via_inclusion(&mut self) -> TxIndexResult<()>;

    fn update_via_acceptance(&mut self) -> TxIndexResult<()>;

    fn update_acceptance_data(&mut self) -> TxIndexResult<()>;

    fn resync(&mut self) -> TxIndexResult<()>;
}

pub type DynTxIndexApi = Option<Arc<RwLock<dyn TxIndexApi>>>;