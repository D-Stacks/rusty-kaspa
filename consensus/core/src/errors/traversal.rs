use kaspa_hashes::Hash;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum TraversalError {
    #[error("passed max allowed traversal ({0} > {1})")]
    ReachedMaxTraversalAllowed(u64, u64),

    #[error("it is required for hash {0} to be a chain block")]
    NoneChainBlockHash(Hash),
}

pub type TraversalResult<T> = std::result::Result<T, TraversalError>;
