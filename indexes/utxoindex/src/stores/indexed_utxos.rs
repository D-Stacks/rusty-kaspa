use crate::core::model::{CompactUtxoCollection, CompactUtxoEntry, UtxoChanges, UtxoSetByScriptPublicKey};

use consensus_core::tx::{
    ScriptPublicKey, ScriptPublicKeys, ScriptVec, TransactionIndexType, TransactionOutpoint, VersionType, SCRIPT_VECTOR_SIZE,
};
use database::prelude::{CachedDbAccess, DirectDbWriter, StoreError, DB};
use hashes::Hash;
use kaspa_utils::serde_big_array::BigArray;
use serde::Deserialize;
use std::collections::hash_map::Entry;
use std::mem::size_of;
use std::sync::Arc;

// Prefixes:

/// Prefixes the [ScriptPublicKey] indexed utxo set.
pub const UTXO_SET_PREFIX: &[u8] = b"utxo-set";

// Buckets:

pub const VERSION_TYPE_SIZE: usize = size_of::<VersionType>(); // Const since we need to re-use this a few times.

/// Size of the [ScriptPublicKeyBucket] in bytes.
pub const SCRIPT_PUBLIC_KEY_BUCKET_SIZE: usize = VERSION_TYPE_SIZE + SCRIPT_VECTOR_SIZE + 1; //plus one to encode length

/// [`ScriptPublicKeyBucket`].
/// Consists of 1 byte u8 encoded length of the script, 2 bytes of little endian [VersionType] bytes, followed by 36 bytes of [ScriptVec] (with right padded 0 bytes if script vec < 36 bytes).
/// the length is encoded separately at the first index, as script public key scripts can have variable byte sizes, this can be used for quick deserialization without needing to parse the script.  
#[derive(Eq, Hash, PartialEq, Debug, Copy, Clone)]
struct ScriptPublicKeyBucket([u8; SCRIPT_PUBLIC_KEY_BUCKET_SIZE]);

impl From<&ScriptPublicKey> for ScriptPublicKeyBucket {
    fn from(script_public_key: &ScriptPublicKey) -> Self {
        let mut bytes = [0; SCRIPT_PUBLIC_KEY_BUCKET_SIZE];

        let length = script_public_key.script().len();
        bytes[0] = length as u8;

        let off_set = VERSION_TYPE_SIZE + 1;
        bytes[1..off_set].copy_from_slice(&script_public_key.version().to_le_bytes());

        bytes[off_set..length + off_set].copy_from_slice(script_public_key.script()); //note: this pads all shorter scripts with 0x00 bytes;

        Self(bytes)
    }
}

impl From<ScriptPublicKeyBucket> for ScriptPublicKey {
    fn from(bucket: ScriptPublicKeyBucket) -> Self {
        let off_set = VERSION_TYPE_SIZE + 1;
        let version =
            VersionType::from_le_bytes(<[u8; VERSION_TYPE_SIZE]>::try_from(&bucket.0[1..off_set]).expect("expected version size"));

        let script_length = bucket.0[0] as usize;
        let script = ScriptVec::from_slice(&bucket.0[off_set..script_length + off_set]);

        Self::new(version, script)
    }
}

impl AsRef<[u8]> for ScriptPublicKeyBucket {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

// Keys:

// TransactionOutpoint:
/// Size of the [TransactionOutpointKey] in bytes.
pub const TRANSACTION_OUTPOINT_KEY_SIZE: usize = hashes::HASH_SIZE + size_of::<TransactionIndexType>();

/// [TransactionOutpoint] key which references the [CompactUtxoEntry] within a [ScriptPublicKeyBucket]
/// Consists of 32 bytes of [TransactionId], followed by 4 bytes of little endian [TransactionIndexType]
#[derive(Eq, Hash, PartialEq, Debug, Copy, Clone)]
struct TransactionOutpointKey([u8; TRANSACTION_OUTPOINT_KEY_SIZE]);

impl From<TransactionOutpointKey> for TransactionOutpoint {
    fn from(key: TransactionOutpointKey) -> Self {
        let transaction_id = Hash::from_slice(&key.0[..hashes::HASH_SIZE]);
        let index = TransactionIndexType::from_le_bytes(
            <[u8; std::mem::size_of::<TransactionIndexType>()]>::try_from(&key.0[hashes::HASH_SIZE..]).expect("expected index size"),
        );
        Self::new(transaction_id, index)
    }
}

impl From<&TransactionOutpoint> for TransactionOutpointKey {
    fn from(outpoint: &TransactionOutpoint) -> Self {
        let mut bytes = [0; TRANSACTION_OUTPOINT_KEY_SIZE];
        bytes[..hashes::HASH_SIZE].copy_from_slice(&outpoint.transaction_id.as_bytes());
        bytes[hashes::HASH_SIZE..].copy_from_slice(&outpoint.index.to_le_bytes());
        Self(bytes)
    }
}

impl AsRef<[u8]> for TransactionOutpointKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Size of the [UtxoEntryFullAccessKey] in bytes.
pub const UTXO_ENTRY_FULL_ACCESS_KEY_SIZE: usize = SCRIPT_PUBLIC_KEY_BUCKET_SIZE + TRANSACTION_OUTPOINT_KEY_SIZE;

/// Full [CompactUtxoEntry] access key.
/// Consists of 39 bytes of [ScriptPublicKeyBucket], and 36 bytes of [TransactionOutpointKey]
#[derive(Eq, Hash, PartialEq, Debug, Copy, Clone, Deserialize)]
struct UtxoEntryFullAccessKey(#[serde(with = "BigArray")] [u8; UTXO_ENTRY_FULL_ACCESS_KEY_SIZE]);

impl UtxoEntryFullAccessKey {
    /// Creates a new [UtxoEntryFullAccessKey] from a [ScriptPublicKeyBucket] and [TransactionOutpointKey].
    pub fn new(script_public_key_bucket: ScriptPublicKeyBucket, transaction_outpoint_key: TransactionOutpointKey) -> Self {
        let mut bytes = [0; UTXO_ENTRY_FULL_ACCESS_KEY_SIZE];
        bytes[..SCRIPT_PUBLIC_KEY_BUCKET_SIZE].copy_from_slice(script_public_key_bucket.as_ref());
        bytes[SCRIPT_PUBLIC_KEY_BUCKET_SIZE..].copy_from_slice(transaction_outpoint_key.as_ref());
        Self(bytes)
    }

    /// Extracts a [`ScriptPublicKey`] of the  [`UtxoEntryFullAccessKey`]
    pub fn extract_script_public_key(&self) -> ScriptPublicKey {
        ScriptPublicKey::from(ScriptPublicKeyBucket(self.0[..SCRIPT_PUBLIC_KEY_BUCKET_SIZE].try_into().expect("expected array")))
    }

    /// Extracts a [`TransactionOutpoint`] of the  [`UtxoEntryFullAccessKey`]
    pub fn extract_transaction_outpoint(&self) -> TransactionOutpoint {
        TransactionOutpoint::from(TransactionOutpointKey(self.0[SCRIPT_PUBLIC_KEY_BUCKET_SIZE..].try_into().expect("expected array")))
    }
}

impl AsRef<[u8]> for UtxoEntryFullAccessKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

// Traits:

pub trait UtxoSetByScriptPublicKeyStoreReader {
    /// Get [UtxoSetByScriptPublicKey] set by queried [ScriptPublicKeys],
    fn get_utxos_from_script_public_keys(&self, script_public_keys: ScriptPublicKeys) -> Result<UtxoSetByScriptPublicKey, StoreError>;

    /// Get the whole indexed [UtxoSetByScriptPublicKey],
    ///
    /// **WARN**: this should only be used for testing purposes.
    fn get_all_utxos(&self) -> Result<UtxoSetByScriptPublicKey, StoreError>;
}

pub trait UtxoSetByScriptPublicKeyStore: UtxoSetByScriptPublicKeyStoreReader {
    /// Updates the store according to the [`UtxoChanges`] -- adding and deleting entries correspondingly.
    /// Note we define `self` as `mut` in order to require write access even though the compiler does not require it.
    /// This is because concurrent readers can interfere with cache consistency.  
    fn write_diff(&mut self, utxo_diff_by_script_public_key: UtxoChanges) -> Result<(), StoreError>;

    /// add [UtxoSetByScriptPublicKey] into the [UtxoSetByScriptPublicKeyStore].
    fn add_utxo_entries(&mut self, utxo_entries: UtxoSetByScriptPublicKey) -> Result<(), StoreError>;

    /// removes all entries in the cache and db, besides prefixes themselves.
    fn delete_all(&mut self) -> Result<(), StoreError>;
}

// Implementations:

pub struct DbUtxoSetByScriptPublicKeyStore {
    db: Arc<DB>,
    access: CachedDbAccess<UtxoEntryFullAccessKey, CompactUtxoEntry>,
}

impl DbUtxoSetByScriptPublicKeyStore {
    pub fn new(db: Arc<DB>, cache_size: u64) -> Self {
        Self { db: Arc::clone(&db), access: CachedDbAccess::new(db, cache_size, UTXO_SET_PREFIX.to_vec()) }
    }
}

impl UtxoSetByScriptPublicKeyStoreReader for DbUtxoSetByScriptPublicKeyStore {
    // compared to go-kaspad this gets transaction outpoints from multiple script public keys at once.
    // TODO: probably ideal way to retrieve is to return a chained iterator which can be used to chunk results and propagate utxo entries
    // to the rpc via pagination, this would alleviate the memory footprint of script public keys with large amount of utxos.
    fn get_utxos_from_script_public_keys(&self, script_public_keys: ScriptPublicKeys) -> Result<UtxoSetByScriptPublicKey, StoreError> {
        let mut utxos_by_script_public_keys = UtxoSetByScriptPublicKey::new();
        for script_public_key in script_public_keys.iter() {
            let script_public_key_bucket = ScriptPublicKeyBucket::from(script_public_key);
            let utxos_by_script_public_keys_inner = CompactUtxoCollection::from_iter(
                self.access
                    .seek_iterator::<TransactionOutpoint, CompactUtxoEntry>(Some(script_public_key_bucket.as_ref()), None, usize::MAX)
                    .into_iter()
                    .map(move |value| {
                        let (k, v) = value.expect("expected `key: TransactionOutpoint`, `value: CompactUtxoEntry`");
                        (k, v)
                    }),
            );
            utxos_by_script_public_keys.insert(ScriptPublicKey::from(script_public_key_bucket), utxos_by_script_public_keys_inner);
        }
        Ok(utxos_by_script_public_keys)
    }

    /// Get the whole indexed [UtxoSetByScriptPublicKey],
    ///
    /// **WARN**: this should only be used for testing purposes.
    fn get_all_utxos(&self) -> Result<UtxoSetByScriptPublicKey, StoreError> {
        let mut utxos_by_script_public_keys = UtxoSetByScriptPublicKey::new();
        for res in self.access.seek_iterator::<UtxoEntryFullAccessKey, CompactUtxoEntry>(None, None, usize::MAX) {
            let (k, v) = res.expect("expected `key: UtxoEntryFullAccessKey`, `value: CompactUtxoEntry`");
            match utxos_by_script_public_keys.entry(k.extract_script_public_key()) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().insert(k.extract_transaction_outpoint(), v);
                }
                Entry::Vacant(entry) => {
                    let mut value = CompactUtxoCollection::with_capacity(1);
                    value.insert(k.extract_transaction_outpoint(), CompactUtxoEntry::new(v.amount, v.block_daa_score, v.is_coinbase));
                    entry.insert(value);
                }
            };
        }
        Ok(utxos_by_script_public_keys)
    }
}

impl UtxoSetByScriptPublicKeyStore for DbUtxoSetByScriptPublicKeyStore {
    fn write_diff(&mut self, utxo_diff_by_script_public_key: UtxoChanges) -> Result<(), StoreError> {
        let mut writer = DirectDbWriter::new(&self.db);

        let mut to_remove =
            utxo_diff_by_script_public_key.removed.iter().flat_map(move |(script_public_key, compact_utxo_collection)| {
                compact_utxo_collection.iter().map(move |(transaction_outpoint, _)| {
                    UtxoEntryFullAccessKey::new(
                        ScriptPublicKeyBucket::from(script_public_key),
                        TransactionOutpointKey::from(transaction_outpoint),
                    )
                })
            });

        let mut to_add = utxo_diff_by_script_public_key.added.iter().flat_map(move |(script_public_key, compact_utxo_collection)| {
            compact_utxo_collection.iter().map(move |(transaction_outpoint, compact_utxo)| {
                (
                    UtxoEntryFullAccessKey::new(
                        ScriptPublicKeyBucket::from(script_public_key),
                        TransactionOutpointKey::from(transaction_outpoint),
                    ),
                    *compact_utxo,
                )
            })
        });

        self.access.delete_many(&mut writer, &mut to_remove)?;
        self.access.write_many(&mut writer, &mut to_add)?;

        Ok(())
    }

    fn add_utxo_entries(&mut self, utxo_entries: UtxoSetByScriptPublicKey) -> Result<(), StoreError> {
        let mut writer = DirectDbWriter::new(&self.db);

        let mut to_add = utxo_entries.iter().flat_map(move |(script_public_key, compact_utxo_collection)| {
            compact_utxo_collection.iter().map(move |(transaction_outpoint, compact_utxo)| {
                (
                    UtxoEntryFullAccessKey::new(
                        ScriptPublicKeyBucket::from(script_public_key),
                        TransactionOutpointKey::from(transaction_outpoint),
                    ),
                    *compact_utxo,
                )
            })
        });

        self.access.write_many(&mut writer, &mut to_add)?;

        Ok(())
    }

    /// Removes all entries in the cache and db, besides prefixes themselves.
    fn delete_all(&mut self) -> Result<(), StoreError> {
        let mut writer = DirectDbWriter::new(&self.db);
        self.access.delete_all(&mut writer)
    }
}
