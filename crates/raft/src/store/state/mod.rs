use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use crate::openraft_types::types::{EffectiveMembership, LogId};

use super::StorageResult;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ConsensusStateMachine {
    pub last_applied_log: Option<LogId>,
    pub last_membership: EffectiveMembership,
}

pub(super) trait RaftStateMachine {
    fn get_last_membership(&self) -> StorageResult<EffectiveMembership>;
    fn set_last_membership(&mut self, membership: EffectiveMembership) -> StorageResult<()>;
    fn get_last_applied_log(&self) -> StorageResult<Option<LogId>>;
    fn set_last_applied_log(&mut self, log_id: LogId) -> StorageResult<()>;
}

impl RaftStateMachine for ConsensusStateMachine {
    fn get_last_membership(&self) -> StorageResult<EffectiveMembership> {
        Ok(self.last_membership.clone())
    }

    fn set_last_membership(&mut self, membership: EffectiveMembership) -> StorageResult<()> {
        self.last_membership = membership;
        Ok(())
    }

    fn get_last_applied_log(&self) -> StorageResult<Option<LogId>> {
        Ok(self.last_applied_log)
    }

    fn set_last_applied_log(&mut self, log_id: LogId) -> StorageResult<()> {
        self.last_applied_log = Some(log_id);
        Ok(())
    }
}
