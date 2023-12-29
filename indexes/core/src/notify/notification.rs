use crate::models::utxoindex::{UtxoChanges, UtxoSetByScriptPublicKey};
use derive_more::{Display, From};
use kaspa_consensus_core::acceptance_data::AcceptanceData;
use kaspa_consensus_notify::notification::{Notification as ConsensusNotification, VirtualChainChangedNotification as ConsensusVirtualChainChangedNotification, ChainAcceptanceDataPrunedNotification as ConsensusChainAcceptanceDataPrunedNotification, PruningPointUtxoSetOverrideNotification as ConsensusPruningPointUtxoSetOverrideNotification};
use kaspa_hashes::Hash;
use kaspa_notify::{
    events::EventType,
    full_featured,
    notification::Notification as NotificationTrait,
    subscription::{
        single::{OverallSubscription, UtxosChangedSubscription, VirtualChainChangedSubscription},
        Subscription, AsAny,
    },
};
use std::{collections::HashMap, sync::Arc, iter::FromFn};

full_featured! {
#[derive(Clone, Debug, Display)]
pub enum Notification {
    
    // Notifications pertaining to the UTXO index
    #[display(fmt = "UtxosChanged notification")]
    UtxosChanged(UtxosChangedNotification),

    #[display(fmt = "PruningPointUtxoSetOverride notification")]
    PruningPointUtxoSetOverride(PruningPointUtxoSetOverrideNotification),

    // Notifications pertaining to the Tx index
    #[display(fmt = "VirtualChainChanged notification")]
    VirtualChainChanged(VirtualChainChangedNotification),

    #[display(fmt = "ChainAcceptanceDataPruned notification")]
    ChainAcceptanceDataPruned(ChainAcceptanceDataPrunedNotification),
}
}

impl NotificationTrait for Notification {
    fn apply_overall_subscription(&self, subscription: &OverallSubscription) -> Option<Self> {
        match subscription.active() {
            true => Some(self.clone()),
            false => None,
        }
    }

    fn apply_virtual_chain_changed_subscription(&self, _subscription: &VirtualChainChangedSubscription) -> Option<Self> {
        Some(self.clone())
    }

    fn apply_utxos_changed_subscription(&self, subscription: &UtxosChangedSubscription) -> Option<Self> {
        match subscription.active() {
            true => {
                let Self::UtxosChanged(notification) = self else { return None };
                notification.apply_utxos_changed_subscription(subscription).map(Self::UtxosChanged)
            }
            false => None,
        }
    }

    fn event_type(&self) -> EventType {
        self.into()
    }
}

#[derive(Debug, Clone, From)]
pub struct ChainAcceptanceDataPrunedNotification {
    pub chain_hash_pruned: Hash,
    pub mergeset_block_acceptance_data_pruned: AcceptanceData,
    pub history_root: Hash,
}

impl ChainAcceptanceDataPrunedNotification {
    pub fn new(chain_hash_pruned: Hash, mergeset_block_acceptance_data_pruned: AcceptanceData, history_root: Hash) -> Self {
        Self { chain_hash_pruned, mergeset_block_acceptance_data_pruned, history_root }
    }
}

impl From<ConsensusChainAcceptanceDataPrunedNotification> for ChainAcceptanceDataPrunedNotification {
    fn from(value: ConsensusChainAcceptanceDataPrunedNotification) -> Self {
        Self {
            chain_hash_pruned: value.chain_hash_pruned,
            mergeset_block_acceptance_data_pruned: value.mergeset_block_acceptance_data_pruned,
            history_root: value.history_root,
        }
    }  
}

#[derive(Debug, Clone, From)]
pub struct VirtualChainChangedNotification {
    pub added_chain_block_hashes: Arc<Vec<Hash>>,
    pub removed_chain_block_hashes: Arc<Vec<Hash>>,
    pub added_chain_blocks_acceptance_data: Arc<Vec<Arc<AcceptanceData>>>,
    pub removed_chain_blocks_acceptance_data: Arc<Vec<Arc<AcceptanceData>>>,
};

impl VirtualChainChangedNotification {
    pub fn new(
        added_chain_block_hashes: Arc<Vec<Hash>>,
        removed_chain_block_hashes: Arc<Vec<Hash>>,
        added_chain_blocks_acceptance_data: Arc<Vec<Arc<AcceptanceData>>>,
        removed_chain_blocks_acceptance_data: Arc<Vec<Arc<AcceptanceData>>>,
    ) -> Self {
        Self {
            added_chain_block_hashes,
            removed_chain_block_hashes,
            added_chain_blocks_acceptance_data,
            removed_chain_blocks_acceptance_data,
        }
    }
}

impl From<ConsensusVirtualChainChangedNotification> for VirtualChainChangedNotification {
    fn from(value: ConsensusVirtualChainChangedNotification) -> Self {
        Self {
            added_chain_block_hashes: value.added_chain_block_hashes,
            removed_chain_block_hashes:value.removed_chain_block_hashes,
            added_chain_blocks_acceptance_data: value.added_chain_blocks_acceptance_data,
            removed_chain_blocks_acceptance_data: value.removed_chain_blocks_acceptance_data,
        }
    }
}
#[derive(Debug, Clone, From, Default)]
pub struct PruningPointUtxoSetOverrideNotification {}

impl From<ConsensusPruningPointUtxoSetOverrideNotification> for PruningPointUtxoSetOverrideNotification {
    fn from(value: _value) -> Self {
        PruningPointUtxoSetOverrideNotification {}
    }
}

#[derive(Debug, Clone, From)]
pub struct UtxosChangedNotification {
    pub added: Arc<UtxoSetByScriptPublicKey>,
    pub removed: Arc<UtxoSetByScriptPublicKey>,
}

impl From<UtxoChanges> for UtxosChangedNotification {
    fn from(item: UtxoChanges) -> Self {
        Self { added: Arc::new(item.added), removed: Arc::new(item.removed) }
    }
}

impl UtxosChangedNotification {
    pub fn from_utxos_changed(utxos_changed: UtxoChanges) -> Self {
        Self { added: Arc::new(utxos_changed.added), removed: Arc::new(utxos_changed.removed) }
    }

    pub(crate) fn apply_utxos_changed_subscription(&self, subscription: &UtxosChangedSubscription) -> Option<Self> {
        if subscription.to_all() {
            Some(self.clone())
        } else {
            let added = Self::filter_utxo_set(&self.added, subscription);
            let removed = Self::filter_utxo_set(&self.removed, subscription);
            if added.is_empty() && removed.is_empty() {
                None
            } else {
                Some(Self { added: Arc::new(added), removed: Arc::new(removed) })
            }
        }
    }

    fn filter_utxo_set(utxo_set: &UtxoSetByScriptPublicKey, subscription: &UtxosChangedSubscription) -> UtxoSetByScriptPublicKey {
        // As an optimization, we iterate over the smaller set (O(n)) among the two below
        // and check existence over the larger set (O(1))
        let mut result = HashMap::default();
        if utxo_set.len() < subscription.addresses().len() {
            utxo_set.iter().for_each(|(script_public_key, collection)| {
                if subscription.addresses().contains_key(script_public_key) {
                    result.insert(script_public_key.clone(), collection.clone());
                }
            });
        } else {
            subscription.addresses().iter().filter(|(script_public_key, _)| utxo_set.contains_key(script_public_key)).for_each(
                |(script_public_key, _)| {
                    if let Some(collection) = utxo_set.get(script_public_key) {
                        result.insert(script_public_key.clone(), collection.clone());
                    }
                },
            );
        }
        result
    }
}


pub mod test {
    /// Below checks tha we do not add fields to the Consensus notifications without adding them to the Index notifications as well.
    fn check_same 
}

