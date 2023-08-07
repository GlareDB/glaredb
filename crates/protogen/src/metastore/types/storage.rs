use super::catalog::CatalogState;
use super::{FromOptionalField, ProtoConvError};
use crate::metastore::gen::storage;
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
pub struct CatalogMetadata {
    pub latest_version: u64,
    pub last_written_by: Uuid,
}

impl TryFrom<storage::CatalogMetadata> for CatalogMetadata {
    type Error = ProtoConvError;
    fn try_from(value: storage::CatalogMetadata) -> Result<Self, Self::Error> {
        Ok(CatalogMetadata {
            latest_version: value.latest_version,
            last_written_by: Uuid::from_slice(&value.last_written_by)?,
        })
    }
}

impl From<CatalogMetadata> for storage::CatalogMetadata {
    fn from(value: CatalogMetadata) -> Self {
        storage::CatalogMetadata {
            latest_version: value.latest_version,
            last_written_by: value.last_written_by.into_bytes().to_vec(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PersistedCatalog {
    pub state: CatalogState,
    pub extra: ExtraState,
}

impl TryFrom<storage::PersistedCatalog> for PersistedCatalog {
    type Error = ProtoConvError;
    fn try_from(value: storage::PersistedCatalog) -> Result<Self, Self::Error> {
        Ok(PersistedCatalog {
            state: value.state.required("state".to_string())?,
            extra: value.extra.required("extra".to_string())?,
        })
    }
}

impl TryFrom<PersistedCatalog> for storage::PersistedCatalog {
    type Error = ProtoConvError;
    fn try_from(value: PersistedCatalog) -> Result<Self, Self::Error> {
        Ok(storage::PersistedCatalog {
            state: Some(value.state.try_into()?),
            extra: Some(value.extra.into()),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ExtraState {
    pub oid_counter: u32,
}

impl TryFrom<storage::ExtraState> for ExtraState {
    type Error = ProtoConvError;
    fn try_from(value: storage::ExtraState) -> Result<Self, Self::Error> {
        Ok(ExtraState {
            oid_counter: value.oid_counter,
        })
    }
}

impl From<ExtraState> for storage::ExtraState {
    fn from(value: ExtraState) -> Self {
        storage::ExtraState {
            oid_counter: value.oid_counter,
        }
    }
}
