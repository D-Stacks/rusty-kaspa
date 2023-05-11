use std::io;
use thiserror::Error;

use crate::IDENT;
use kaspa_database::prelude::StoreError;

/// Errors originating from the [`UtxoIndex`].
#[derive(Error, Debug)]
pub enum TxIndexError {
    #[error("[{IDENT}]: {0}")]
    StoreAccessError(#[from] StoreError),

    #[error("[{IDENT}]: {0}")]
    DBResetError(#[from] io::Error),

    #[error("[{IDENT}]: Cannot process by both inclusion and acceptance, choose one only")]
    ConflictingProcessingMechanics,
}

/// Results originating from the [`UtxoIndex`].
pub type TxIndexResult<T> = Result<T, TxIndexError>;