// TODO (when scoped configs are implemented): remove file use txindex config directly, for the params store. 

use serde::{Deserialize, Serialize};

use crate::errors::TxIndexError;

#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash, PartialEq, Eq)]
pub struct TxIndexParams {
    pub process_offsets: bool, 
    pub process_acceptance: bool,
    pub process_offsets_by_inclusion: bool,
    pub transaction_entries_cache_size: usize,
    pub block_transaction_cache_size: usize,
}

impl TxIndexParams {
    pub fn new(
        process_offsets: bool, 
        process_acceptance: bool, 
        process_offsets_by_inclusion: bool, 
        transaction_entries_cache_size: usize,
        block_transaction_cache_size: usize,
    ) -> TxIndexResult<Self> {
        
        if !process_offsets && !process_acceptance {
            return Err(TxIndexError::NoProcessingPurpose(
                "Not processing any relevant txindex data (i.e. accepting block hash, or transaction offsets)"
                .to_string())
            ); //No processing purpose
        } else if process_offsets_by_inclusion && !process_offsets {
            return Err(TxIndexError::NoProcessingPurpose(
                "Not processing transaction offsets, but processing none-accepted transactions serves no purpose"
                .to_string())
            ); //No processing purpose
        }

        Ok(Self {
            process_offsets,
            process_acceptance,
            process_offsets_by_inclusion,
            transaction_entries_cache_size,
            block_transaction_cache_size,
        })
    }

    pub fn process_via_acceptance_data(&self) {
        self.process_acceptance || !self.process_offsets_by_inclusion
    }

    pub fn process_via_block_added(&self) {
        self.process_offsets_by_inclusion
    }
}