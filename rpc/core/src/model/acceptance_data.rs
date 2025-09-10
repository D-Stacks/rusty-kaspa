use serde::{Deserialize, Serialize};
use workflow_serializer::prelude::*;

use super::{RpcHeader, RpcHash, RpcHeaderVerbosity, RpcTransaction, RpcTransactionVerbosity};

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RpcAcceptanceData {
    pub accepting_chain_header: Option<RpcHeader>,
    pub mergeset_block_acceptance_data: Vec<RpcMergesetBlockAcceptanceData>,
    pub accepted_transactions: Vec<RpcTransaction>,
}

impl RpcAcceptanceData {
    pub fn new(
        accepting_chain_header: Option<RpcHeader>,
        mergeset_block_acceptance_data: Vec<RpcMergesetBlockAcceptanceData>,
        accepted_transactions: Vec<RpcTransaction>,
    ) -> Self {
        Self { accepting_chain_header, mergeset_block_acceptance_data, accepted_transactions }
    }
}

impl Serializer for RpcAcceptanceData {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        store!(u8, &1, writer)?;
        store!(Option<RpcHeader>, &self.accepting_chain_header, writer)?;
        serialize!(Vec<RpcMergesetBlockAcceptanceData>, &self.mergeset_block_acceptance_data, writer)?;
        serialize!(Vec<RpcTransaction>, &self.accepted_transactions, writer)?;

        Ok(())
    }
}

impl Deserializer for RpcAcceptanceData {
    fn deserialize<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let _version = load!(u8, reader);
        let accepting_chain_header = load!(Option<RpcHeader>, reader)?;
        let mergeset_block_acceptance_data = deserialize!(Vec<RpcMergesetBlockAcceptanceData>, reader)?;
        let accepted_transactions = deserialize!(Vec<RpcTransaction>, reader)?;

        Ok(Self { accepting_chain_header, mergeset_block_acceptance_data, accepted_transactions })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcMergesetBlockAcceptanceData {
    pub merged_header: Option<RpcHeader>,
    pub transaction_ids: Vec<RpcHash>,
}

impl RpcMergesetBlockAcceptanceData {
    #[inline(always)]
    pub fn new(merged_header: Option<RpcHeader>, transaction_ids: Vec<RpcHash>) -> Self {
        Self { merged_header, transaction_ids }
    }
}

impl Serializer for RpcMergesetBlockAcceptanceData {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        store!(u8, &1, writer)?;

        store!(Option<RpcHeader>, &self.merged_header, writer)?;
        store!(Vec<RpcHash>, &self.transaction_ids, writer)?;

        Ok(())
    }
}

impl Deserializer for RpcMergesetBlockAcceptanceData {
    fn deserialize<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let _version = load!(u8, reader);

        let merged_header = load!(Option<RpcHeader>, reader)?;
        let transaction_ids = load!(Vec<RpcHash>, reader)?;

        Ok(Self { merged_header, transaction_ids })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcAcceptanceDataVerbosity {
    pub accepting_chain_header_verbosity: Option<RpcHeaderVerbosity>,
    pub merged_header_verbosity: Option<RpcHeaderVerbosity>,
    pub accepted_transactions_verbosity: Option<RpcTransactionVerbosity>,
}

impl RpcAcceptanceDataVerbosity {
    pub fn new(
        accepting_chain_header_verbosity: Option<RpcHeaderVerbosity>,
        merged_header_verbosity: Option<RpcHeaderVerbosity>,
        accepted_transactions_verbosity: Option<RpcTransactionVerbosity>,
    ) -> Self {
        Self { accepting_chain_header_verbosity, merged_header_verbosity, accepted_transactions_verbosity }
    }

    pub fn requires_merged_header(&self) -> bool {
        self.merged_header_verbosity.is_some()
            || self.accepted_transactions_verbosity.as_ref().is_some_and(|active| {
                active.verbose_data_verbosity.as_ref().is_some_and(|active| active.include_block_hash.unwrap_or(false))
            })
    }

    pub fn requeires_accepted_header(&self) -> bool {
        self.merged_header_verbosity.as_ref().is_some_and(|active| active.include_hash.unwrap_or(false))
            || self.accepted_transactions_verbosity.as_ref().is_some_and(|active| {
                active.verbose_data_verbosity.as_ref().is_some_and(|active| active.include_block_hash.unwrap_or(false))
            })
    }

    pub fn requires_accepted_transactions(&self) -> bool {
        self.accepted_transactions_verbosity.as_ref().is_some()
    }
}

impl Serializer for RpcAcceptanceDataVerbosity {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        store!(u8, &1, writer)?;
        store!(Option<RpcHeaderVerbosity>, &self.accepting_chain_header_verbosity, writer)?;
        serialize!(Option<RpcHeaderVerbosity>, &self.merged_header_verbosity, writer)?;
        serialize!(Option<RpcTransactionVerbosity>, &self.accepted_transactions_verbosity, writer)?;

        Ok(())
    }
}

impl Deserializer for RpcAcceptanceDataVerbosity {
    fn deserialize<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let _version = load!(u8, reader);
        let accepting_chain_header_verbosity = load!(Option<RpcHeaderVerbosity>, reader)?;
        let merged_header_verbosity = deserialize!(Option<RpcHeaderVerbosity>, reader)?;
        let accepted_transactions_verbosity = deserialize!(Option<RpcTransactionVerbosity>, reader)?;

        Ok(Self { accepting_chain_header_verbosity, merged_header_verbosity, accepted_transactions_verbosity })
    }
}

