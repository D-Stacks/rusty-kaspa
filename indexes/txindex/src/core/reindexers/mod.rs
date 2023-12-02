use std::sync::Arc;

use kaspa_consensus_core::tx::{Transaction, TransactionId};
use kaspa_hashes::Hash;

use crate::{
    errors::{TxIndexError, TxIndexResult},
    model::{
        BlockAcceptanceChanges, TxAcceptanceChanges, TxAcceptanceData, TxAcceptanceDataByBlockHash, TxCompactEntriesById,
        TxCompactEntry, TxInclusionChanges, TxOffset, TxOffsetById,
    },
};

pub mod acceptance_data_reindexer;
pub mod unaccepted_data_reindexer;

pub use {acceptance_data_reindexer::*, unaccepted_data_reindexer::*};
