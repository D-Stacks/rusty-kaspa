use crate::protowire::{self, RpcBlockHeaderVerbosity, RpcTransactionVerbosity};
use crate::{from, try_from};
use kaspa_rpc_core::{RpcError, RpcHash, RpcMergesetBlockAcceptanceData};
use std::str::FromStr;

// ----------------------------------------------------------------------------
// rpc_core to protowire
// ----------------------------------------------------------------------------

from!(item: &kaspa_rpc_core::RpcAcceptanceData,  protowire::RpcAcceptanceData, {
    Self {
        accepting_chain_header: item.accepting_chain_header.as_ref().map(protowire::RpcBlockHeader::from),
        mergeset_block_acceptance_data: item
            .mergeset_block_acceptance_data
            .iter()
            .map(protowire::RpcMergesetBlockAcceptanceData::from)
            .collect(),
        accepted_transactions: item
            .accepted_transactions
            .iter()
            .map(protowire::RpcTransaction::from)
            .collect(),
    }
});

from!(item: &kaspa_rpc_core::RpcAcceptanceDataVerbosity, protowire::RpcAcceptanceDataVerbosity, {
    Self {
        accepting_chain_header_verbosity: item.accepting_chain_header_verbosity.as_ref().map(RpcBlockHeaderVerbosity::from),
        merged_header_verbosity: item.merged_header_verbosity.as_ref().map(RpcBlockHeaderVerbosity::from),
        accepted_transactions_verbosity: item.accepted_transactions_verbosity.as_ref().map(RpcTransactionVerbosity::from),
    }
});

from!(item: &kaspa_rpc_core::RpcMergesetBlockAcceptanceData, protowire::RpcMergesetBlockAcceptanceData, {
    Self {
        merged_header: item.merged_header.as_ref().map(protowire::RpcBlockHeader::from),
        transaction_ids: item.transaction_ids.iter().map(|x| x.to_string()).collect(),
    }
});

// ----------------------------------------------------------------------------
// protowire to rpc_core
// ----------------------------------------------------------------------------

try_from!(item: &protowire::RpcAcceptanceData, kaspa_rpc_core::RpcAcceptanceData, {
    Self {
        accepting_chain_header: item
            .accepting_chain_header
            .as_ref()
            .map(kaspa_rpc_core::RpcHeader::try_from)
            .transpose()?,
        mergeset_block_acceptance_data: item
        .mergeset_block_acceptance_data
        .iter()
        .map(RpcMergesetBlockAcceptanceData::try_from)
        .collect::<Result<_, _>>()?,
        accepted_transactions: item.accepted_transactions.iter().map(kaspa_rpc_core::RpcTransaction::try_from).collect::<Result<_, _>>()?,
    }
});

try_from!(item: &protowire::RpcAcceptanceDataVerbosity, kaspa_rpc_core::RpcAcceptanceDataVerbosity, {
    Self {
        accepting_chain_header_verbosity: item.accepting_chain_header_verbosity.as_ref().map(kaspa_rpc_core::RpcHeaderVerbosity::try_from).transpose()?,
        merged_header_verbosity: item.merged_header_verbosity.as_ref().map(kaspa_rpc_core::RpcHeaderVerbosity::try_from).transpose()?,
        accepted_transactions_verbosity: item.accepted_transactions_verbosity.as_ref().map(kaspa_rpc_core::RpcTransactionVerbosity::try_from).transpose()?,
    }
});

try_from!(item: &protowire::RpcMergesetBlockAcceptanceData, kaspa_rpc_core::RpcMergesetBlockAcceptanceData, {
    Self {
        merged_header: item.merged_header.as_ref().map(kaspa_rpc_core::RpcHeader::try_from).transpose()?,
        transaction_ids: item
            .transaction_ids
            .iter()
            .map(|x| RpcHash::from_str(x))
            .collect::<Result<Vec<kaspa_rpc_core::RpcHash>, faster_hex::Error>>()?,
    }
});
