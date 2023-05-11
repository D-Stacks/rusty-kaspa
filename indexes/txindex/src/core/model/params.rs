// TODO (when scoped configs are implemented): remove file use txindex config directly, for the params store. 

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Deserialize, Serialize, Debug, Hash)]
pub struct TxIndexParams {
    pub process_offsets_by_inclusion: bool, 
    pub process_offsets_by_acceptance: bool,
    pub process_acceptance: bool,
}