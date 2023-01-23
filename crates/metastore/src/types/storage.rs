use super::{FromOptionalField, ProtoConvError};
use crate::proto::arrow;
use crate::proto::catalog;
use crate::proto::storage;
use crate::types::catalog::{CatalogEntry, CatalogState};
use proptest_derive::Arbitrary;
use prost_types::Timestamp;
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseState {
    Unlocked,
    Locked,
}

impl TryFrom<storage::lease_information::State> for LeaseState {
    type Error = ProtoConvError;
    fn try_from(value: storage::lease_information::State) -> Result<Self, Self::Error> {
        match value {
            storage::lease_information::State::Unkown => {
                Err(ProtoConvError::ZeroValueEnumVariant("LockState"))
            }
            storage::lease_information::State::Unlocked => Ok(LeaseState::Unlocked),
            storage::lease_information::State::Locked => Ok(LeaseState::Locked),
        }
    }
}

impl TryFrom<i32> for LeaseState {
    type Error = ProtoConvError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        storage::lease_information::State::from_i32(value)
            .ok_or(ProtoConvError::UnknownEnumVariant("LockState", value))
            .and_then(|t| t.try_into())
    }
}

impl From<LeaseState> for storage::lease_information::State {
    fn from(value: LeaseState) -> Self {
        match value {
            LeaseState::Unlocked => storage::lease_information::State::Unlocked,
            LeaseState::Locked => storage::lease_information::State::Locked,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseInformation {
    pub state: LeaseState,
    pub generation: u64,
    pub expires_at: Option<SystemTime>,
    pub held_by: Option<Uuid>,
}

impl TryFrom<storage::LeaseInformation> for LeaseInformation {
    type Error = ProtoConvError;
    fn try_from(value: storage::LeaseInformation) -> Result<Self, Self::Error> {
        let state: LeaseState = value.state.try_into()?;
        if state == LeaseState::Unlocked {
            return Ok(LeaseInformation {
                state,
                generation: value.generation,
                held_by: None,
                expires_at: None,
            });
        }

        let expires_at: Option<SystemTime> = match value.expires_at {
            Some(t) => Some(t.try_into()?),
            None => None,
        };

        let held_by = Uuid::from_slice(&value.held_by)?;

        Ok(LeaseInformation {
            state,
            generation: value.generation,
            expires_at,
            held_by: Some(held_by),
        })
    }
}

impl From<LeaseInformation> for storage::LeaseInformation {
    fn from(value: LeaseInformation) -> Self {
        let state: storage::lease_information::State = value.state.into();
        storage::LeaseInformation {
            state: state as i32,
            generation: value.generation,
            expires_at: value.expires_at.map(|t| t.into()),
            held_by: value
                .held_by
                .map(|t| t.into_bytes().to_vec())
                .unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PersistedCatalog {
    pub db_id: Uuid,
    pub state: CatalogState,
    pub oid_counter: u32,
}

impl TryFrom<storage::PersistedCatalog> for PersistedCatalog {
    type Error = ProtoConvError;
    fn try_from(value: storage::PersistedCatalog) -> Result<Self, Self::Error> {
        Ok(PersistedCatalog {
            db_id: Uuid::from_slice(&value.db_id)?,
            state: value.state.required("state")?,
            oid_counter: value.oid_counter,
        })
    }
}

impl TryFrom<PersistedCatalog> for storage::PersistedCatalog {
    type Error = ProtoConvError;
    fn try_from(value: PersistedCatalog) -> Result<Self, Self::Error> {
        Ok(storage::PersistedCatalog {
            db_id: value.db_id.into_bytes().to_vec(),
            state: Some(value.state.try_into()?),
            oid_counter: value.oid_counter,
        })
    }
}
