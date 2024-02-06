use crate::protowire;
use crate::{from, try_from};
use kaspa_rpc_core::{RpcError, RpcHash};
use std::str::FromStr;

from!(item: &kaspa_rpc_core::RpcConfirmedData, protowire::RpcConfirmedData, {
    Self {
        confirmations: item.confirmations,
        blue_score: item.blue_score,
        daa_score: item.daa_score,
        timestamp: item.timestamp,
        chain_block_hash: item.chain_block_hash.to_string(),
        chain_block: item.chain_block.as_ref().map(protowire::RpcBlock::from),
        merge_set_block_acceptance_data: item.merge_set_block_acceptance_data.iter().map(protowire::RpcMergeSetBlockAcceptanceData::from).collect::<Vec<_>>(),
    }
});

from!(item: &kaspa_rpc_core::RpcMergeSetBlockAcceptanceData, protowire::RpcMergeSetBlockAcceptanceData, {
    Self {
        block_hash: item.block_hash.as_ref().map(|x| x.to_string()).unwrap_or_default(),
        block: item.block.as_ref().map(protowire::RpcBlock::from),
        accepted_transaction_ids: item.accepted_transaction_ids.iter().map(|x| x.to_string()).collect::<Vec<_>>(),
        accepted_transactions: item.accepted_transactions.iter().map(protowire::RpcTransaction::from).collect::<Vec<_>>(),
    }
});

try_from!(item: &protowire::RpcConfirmedData, kaspa_rpc_core::RpcConfirmedData, {
    Self {
        confirmations: item.confirmations,
        blue_score: item.blue_score,
        daa_score: item.daa_score,
        timestamp: item.timestamp,
        chain_block_hash: RpcHash::from_str(&item.chain_block_hash)?,
        chain_block: item.chain_block.as_ref().map(kaspa_rpc_core::RpcBlock::try_from).transpose()?,
        merge_set_block_acceptance_data: item.merge_set_block_acceptance_data.iter().map(kaspa_rpc_core::RpcMergeSetBlockAcceptanceData::try_from).collect::<Result<Vec<_>, _>>()?,
    }
});

try_from!(item: &protowire::RpcMergeSetBlockAcceptanceData, kaspa_rpc_core::RpcMergeSetBlockAcceptanceData, {
    Self {
        block_hash: if item.block_hash == String::default() {None} else { Some(RpcHash::from_str(item.block_hash.as_str())?) },
        block: item.block.as_ref().map(kaspa_rpc_core::RpcBlock::try_from).transpose()?,
        accepted_transaction_ids: item.accepted_transaction_ids.iter().map(|x| RpcHash::from_str(x)).collect::<Result<Vec<_>, _>>()?,
        accepted_transactions: item.accepted_transactions.iter().map(kaspa_rpc_core::RpcTransaction::try_from).collect::<Result<Vec<_>, _>>()?,
    }
});
