use kaspa_consensus_core::tx::{TimeLock, TimeLockArg, TimeLockResult, Transaction};

use crate::constants::LOCK_TIME_THRESHOLD;

use super::{
    errors::{TxResult, TxRuleError},
    TransactionValidator,
};

impl TransactionValidator {
    pub fn utxo_free_tx_validation(&self, tx: &Transaction, time_lock_arg: TimeLockArg) -> TxResult<()> {
        self.check_tx_is_finalized(tx, time_lock_arg)
    }

    fn check_tx_is_finalized(&self, tx: &Transaction, time_lock_arg: TimeLockArg) -> TxResult<()> {
        let time_lock = tx.get_time_lock();
        match time_lock.is_finalized(&time_lock_arg) {
            TimeLockResult::Finalized => return Ok(()),
            TimeLockResult::NotFinalized => (),
            TimeLockResult::Invalid => return Err(TxRuleError::InvalidUnlockOp(time_lock_arg, time_lock)),
        };

        // At this point, the transaction's lock time hasn't occurred yet, but
        // the transaction might still be finalized if the sequence number
        // for all transaction inputs is maxed out.
        for (i, input) in tx.inputs.iter().enumerate() {
            if input.sequence != u64::MAX {
                return Err(TxRuleError::NotFinalized(i));
            }
        }

        Ok(())
    }
}
