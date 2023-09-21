use std::{io, string::FromUtf8Error};
use kaspa_consensus_core::errors::consensus::ConsensusError;
use thiserror::Error;

use crate::IDENT;
use kaspa_database::prelude::StoreError;

/// Errors originating from the [`TxIndex`].
#[derive(Error, Debug)]
pub enum TxIndexError {
    #[error("[{IDENT}]: {0}")]
    StoreAccessError(#[from] StoreError),

    #[error("[{IDENT}]: {0}")]
    DBResetError(#[from] io::Error),

    #[error("[{IDENT}]: {0}")]
    ByteStringLUtf8ConversionError(#[from] FromUtf8Error),

    #[error("[{IDENT}]: Trying to read from uninitialized store: `{0}`")]
    DBReadingFromUninitializedStoreError(String),

    #[error("[{IDENT}]: {0}")]
    ConsensusQueryError(#[from] ConsensusError),

    #[error("[{IDENT}]: {0}")]
    NoProcessingPurposeError(String),

    #[error("[{IDENT}]: {0}")]
    ReindexingError(String),
}

/// Results originating from the [`UtxoIndex`].
pub type TxIndexResult<T> = Result<T, TxIndexError>;