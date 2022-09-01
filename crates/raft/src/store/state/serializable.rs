use std::{collections::BTreeMap, fmt::Debug};

use serde::{Deserialize, Serialize};

use crate::openraft_types::types::{EffectiveMembership, LogId};

use super::{ConsensusStateMachine, RaftStateMachine};

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SerializableConsensusStateMachine {
    pub last_applied_log: Option<LogId>,
    pub last_membership: EffectiveMembership,
    /// application data
    pub data: BTreeMap<String, String>,
}

impl From<&ConsensusStateMachine> for SerializableConsensusStateMachine {
    fn from(state: &ConsensusStateMachine) -> Self {
        let mut data = BTreeMap::new();

        for r in state.db.iterator_cf(
            state.db.cf_handle("data").expect("cf_handle"),
            rocksdb::IteratorMode::Start,
        ) {
            if let Ok((key, value)) = r {
                let key: &[u8] = &key;
                let value: &[u8] = &value;
                data.insert(
                    String::from_utf8(key.to_vec()).expect("invalid key"),
                    String::from_utf8(value.to_vec()).expect("invalid data"),
                );
            } else {
                panic!("iterator error");
            }
        }

        Self {
            last_applied_log: state.get_last_applied_log().unwrap(),
            last_membership: state.get_last_membership().unwrap(),
            data,
        }
    }
}
