//! Conversion of Block related types

use std::sync::Arc;

use crate::{RpcBlock, RpcError, RpcRawBlock, RpcResult, RpcTransaction};
use kaspa_consensus_core::block::{Block, BlockTemplate, MutableBlock};

// ----------------------------------------------------------------------------
// consensus_core to rpc_core
// ----------------------------------------------------------------------------

impl From<&Block> for RpcBlock {
    fn from(item: &Block) -> Self {
        Self {
            header: item.header.as_ref().into(),
            transactions: item.transactions.iter().map(|tx| RpcTransaction::from(tx.as_ref())).collect(),
            // TODO: Implement a populating process inspired from kaspad\app\rpc\rpccontext\verbosedata.go
            verbose_data: None,
        }
    }
}

impl From<&Block> for RpcRawBlock {
    fn from(item: &Block) -> Self {
        Self {
            header: item.header.as_ref().into(),
            transactions: item.transactions.iter().map(|tx| RpcTransaction::from(tx.as_ref())).collect(),
        }
    }
}

impl From<BlockTemplate> for RpcRawBlock {
    fn from(item: BlockTemplate) -> Self {
        let transactions = item.transactions().iter().map(|tx| RpcTransaction::from(tx.as_ref())).collect();
        Self { header: item.header.into(), transactions }
    }
}

impl From<&MutableBlock> for RpcBlock {
    fn from(item: &MutableBlock) -> Self {
        Self {
            header: item.header.as_ref().into(),
            transactions: item.transactions.iter().map(RpcTransaction::from).collect(),
            verbose_data: None,
        }
    }
}

impl From<&MutableBlock> for RpcRawBlock {
    fn from(item: &MutableBlock) -> Self {
        Self { header: item.header.as_ref().into(), transactions: item.transactions.iter().map(RpcTransaction::from).collect() }
    }
}

impl From<MutableBlock> for RpcRawBlock {
    fn from(item: MutableBlock) -> Self {
        Self { header: item.header.into(), transactions: item.transactions.iter().map(RpcTransaction::from).collect() }
    }
}

// ----------------------------------------------------------------------------
// rpc_core to consensus_core
// ----------------------------------------------------------------------------

impl TryFrom<RpcBlock> for Block {
    type Error = RpcError;
    fn try_from(item: RpcBlock) -> RpcResult<Self> {
        Ok(Self {
            header: Arc::new(item.header.into()),
            transactions: Arc::new(
                item.transactions
                    .into_iter()
                    .map(kaspa_consensus_core::tx::Transaction::try_from)
                    .collect::<RpcResult<Vec<kaspa_consensus_core::tx::Transaction>>>()?
                    .into_iter()
                    .map(Arc::new)
                    .collect(),
            ),
        })
    }
}

impl TryFrom<RpcRawBlock> for Block {
    type Error = RpcError;
    fn try_from(item: RpcRawBlock) -> RpcResult<Self> {
        Ok(Self {
            header: Arc::new(item.header.into()),
            transactions: Arc::new(
                item.transactions
                    .into_iter()
                    .map(kaspa_consensus_core::tx::Transaction::try_from)
                    .collect::<RpcResult<Vec<kaspa_consensus_core::tx::Transaction>>>()?
                    .into_iter()
                    .map(Arc::new)
                    .collect(),
            ),
        })
    }
}
