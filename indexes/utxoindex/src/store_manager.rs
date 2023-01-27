use std::{fs, ops::Deref, sync::Arc};

use consensus::model::stores::{errors::StoreError, DB};
use consensus_core::{tx::ScriptPublicKeys, BlockHashSet};
use log::trace;
use parking_lot::RwLock;
use rocksdb::{DBAccess, Options};

use crate::{
    errors::UtxoIndexError,
    model::{UTXOChanges, UtxoSetByScriptPublicKey},
    stores::{
        circulating_supply_store::{CirculatingSupplyStore, CirculatingSupplyStoreReader, DbCirculatingSupplyStore},
        tips_store::{DbUtxoIndexTipsStore, UtxoIndexTipsStore, UtxoIndexTipsStoreReader},
        utxo_set_store::{DbUtxoSetByScriptPublicKeyStore, UtxoSetByScriptPublicKeyStore, UtxoSetByScriptPublicKeyStoreReader},
    },
};

#[derive(Clone)]
pub struct StoreManager {
    db: Arc<DB>,

    utxoindex_tips_store: Arc<RwLock<DbUtxoIndexTipsStore>>,
    circulating_suppy_store: Arc<RwLock<DbCirculatingSupplyStore>>,
    utxos_by_script_public_key_store: Arc<RwLock<DbUtxoSetByScriptPublicKeyStore>>,
}

impl StoreManager {
    pub fn new(db: Arc<DB>) -> Self {
        Self {
            utxoindex_tips_store: Arc::new(RwLock::new(DbUtxoIndexTipsStore::new(db.clone()))),
            circulating_suppy_store: Arc::new(RwLock::new(DbCirculatingSupplyStore::new(db.clone()))),
            utxos_by_script_public_key_store: Arc::new(RwLock::new(DbUtxoSetByScriptPublicKeyStore::new(db.clone(), 0))),
            db: db.clone(),
        }
    }

    pub fn get_utxos_by_script_public_key(
        &self,
        script_public_keys: ScriptPublicKeys,
    ) -> Result<Arc<UtxoSetByScriptPublicKey>, StoreError> {
        let reader = self.utxos_by_script_public_key_store.read();
        reader.get_utxos_from_script_public_keys(script_public_keys)
    }

    pub fn update_utxo_state(&self, utxo_diff_by_script_public_key: UTXOChanges) -> Result<(), StoreError> {
        let mut writer = self.utxos_by_script_public_key_store.write();
        writer.write_diff(utxo_diff_by_script_public_key)
    }

    pub fn insert_utxo_entries(&self, utxo_set_by_script_public_key: UtxoSetByScriptPublicKey) -> Result<(), StoreError> {
        let mut writer = self.utxos_by_script_public_key_store.write();
        writer.insert_utxo_entries(utxo_set_by_script_public_key)
    }

    pub fn get_circulating_supply(&self) -> Result<u64, StoreError> {
        let reader = self.circulating_suppy_store.read();
        reader.get()
    }

    pub fn update_circulating_supply(&self, circulating_supply_diff: i64) -> Result<u64, StoreError> {
        let mut writer = self.circulating_suppy_store.write();
        writer.add_circulating_supply_diff(circulating_supply_diff)
    }

    pub fn insert_circulating_supply(&self, circulating_supply: u64) -> Result<(), StoreError> {
        let mut writer = self.circulating_suppy_store.write();
        writer.insert(circulating_supply)
    }

    pub fn get_tips(&self) -> Result<Arc<BlockHashSet>, StoreError> {
        let reader = self.utxoindex_tips_store.read();
        Ok(reader.get()?)
    }

    pub fn insert_tips(&self, tips: BlockHashSet) -> Result<(), StoreError> {
        let mut writer = self.utxoindex_tips_store.write();
        writer.add_tips(tips)
    }

    /// Resets the utxoindex database:
    ///
    /// 1) Removes the entire utxoindex database,
    /// 2) Creates a new one in its place,
    /// 3) populates the new db with associated prefixes.
    pub fn delete_all(&self) -> Result<(), UtxoIndexError> {
        trace!("creating new utxoindex database, deleting the old one");
        //hold all individual store locks in-place
        let mut utxoindex_tips_store = self.utxoindex_tips_store.write();
        let mut circulating_suppy_store = self.circulating_suppy_store.write();
        let mut utxos_by_script_public_key_store = self.utxos_by_script_public_key_store.write();
        utxoindex_tips_store.remove()?;
        circulating_suppy_store.remove()?;
        utxos_by_script_public_key_store.delete_all()?;

        Ok(())
    }
}
