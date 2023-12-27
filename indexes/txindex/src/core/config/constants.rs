use kaspa_consensus_core::subnets::SUBNETWORK_ID_SIZE;

pub const STANDARD_SCHNORR_SCRIPT_PUBLIC_KEY_LEN: u64 = 34u64;

/// The approx size of a default standard network transaction header in bytes.
pub const STANDARD_TRANSACTION_HEADER_SIZE: u64 = {
    // HEADER
    2u64 // transaction version
    + 8u64 // number of inputs
    + 8u64 // number of outputs 
    + 4u64 // lock time
    + SUBNETWORK_ID_SIZE as u64 // subnetwork id size
    + 8u64 // gas
    + 32u64 // payload hash
    + 8u64 // payload length
};

/// The approx size of a default standard network transaction input in bytes.
pub const STANDARD_TRANSACTION_INPUT_SIZE: u64 = {
    // INPUT
    32u64 // previous transaction id
    + 4u64 // previous transaction index
    + 8u64 // value
    + 1u64 // signature script length
    + 4u64 // sequence
    + STANDARD_SCHNORR_SCRIPT_PUBLIC_KEY_LEN // script public key len
    + 1 + 64 + 1 // signature -> 1 byte for OP_DATA_65 + 64 (length of signature) + 1 byte for sig hash type
};

/// The approx size of a default standard network transaction output in bytes.
pub const STANDARD_TRANSACTION_OUTPUT_SIZE: u64 = {
    // OUTPUT
    8u64 // value
    + 8u64 // script length
    + STANDARD_SCHNORR_SCRIPT_PUBLIC_KEY_LEN // script public key len
    + 4u64 // script version
    + 8u64 // sequence
};
