pub mod acceptance_data_reindexer;
pub mod block_added_reindexer;
pub mod block_pruned_reindexer;

pub use {
        acceptance_data_reindexer::*, 
        block_added_reindexer::*, 
        block_pruned_reindexer::*
    };

pub struct TxIndexReindexers {
    pub acceptance_data_reindexer: TxIndexAcceptanceDataReindexer,
    pub block_added_reindexer: TxIndexBlockAddedReindexer,
    pub block_pruned_reindexer: TxIndexBlockPrunedReindexer,
}

impl TxIndexReindexers {
    pub fn new() -> Self {
        Self {
            acceptance_data_reindexer: TxIndexAcceptanceDataReindexer::new(),
            block_added_reindexer: TxIndexBlockAddedReindexer::new(),
            block_pruned_reindexer: TxIndexBlockPrunedReindexer::new(),
        }
    }
}
