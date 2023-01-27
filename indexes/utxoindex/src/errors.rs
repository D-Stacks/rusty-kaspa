use async_std::channel::SendError;
use consensus::model::stores::errors::StoreError;
use rocksdb::Error as RDBError;
use std::io;

use thiserror::Error;

use crate::notify::UtxoIndexNotification;

#[derive(Error, Debug)]
pub enum UtxoIndexError {
    #[error("utxoindex error: consensus reciever is unreachable")]
    ConsensusRecieverUnreachableError,

    #[error("utxoindex error: shutdown reciever is unreachable")]
    ShutDownRecieverUnreachableError,

    #[error("utxoindex error: {0}")]
    StoreAccessError(#[from] StoreError),

    #[error("utxoindex error: {0}")]
    DBResetError(#[from] io::Error),

    #[error("utxoindex error: {0}")]
    DBDestroyError(#[from] RDBError),

    #[error("utxoindex error: {0}")]
    SendError(#[from] SendError<UtxoIndexNotification>),
}
