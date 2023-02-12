use super::ProtoConvError;
use crate::proto::storage;
use crate::types::catalog::{CatalogEntry, DependencyList};
use std::collections::HashMap;
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
    pub version: u64,
    pub entries: HashMap<u32, CatalogEntry>,
    pub oid_counter: u32,
    pub dependency_lists: HashMap<u32, DependencyList>,
}

impl TryFrom<storage::PersistedCatalog> for PersistedCatalog {
    type Error = ProtoConvError;
    fn try_from(value: storage::PersistedCatalog) -> Result<Self, Self::Error> {
        let mut entries = HashMap::with_capacity(value.entries.len());
        for (id, ent) in value.entries {
            entries.insert(id, ent.try_into()?);
        }

        let mut dependency_lists = HashMap::with_capacity(value.dependency_lists.len());
        for (id, deps) in value.dependency_lists {
            dependency_lists.insert(id, deps.try_into()?);
        }

        Ok(PersistedCatalog {
            version: value.version,
            entries,
            oid_counter: value.oid_counter,
            dependency_lists,
        })
    }
}

impl TryFrom<PersistedCatalog> for storage::PersistedCatalog {
    type Error = ProtoConvError;
    fn try_from(value: PersistedCatalog) -> Result<Self, Self::Error> {
        Ok(storage::PersistedCatalog {
            version: value.version,
            entries: value
                .entries
                .into_iter()
                .map(|(id, ent)| match ent.try_into() {
                    Ok(ent) => Ok((id, ent)),
                    Err(e) => Err(e),
                })
                .collect::<Result<_, _>>()?,
            oid_counter: value.oid_counter,
            dependency_lists: value
                .dependency_lists
                .into_iter()
                .map(|(id, deps)| (id, deps.into()))
                .collect(),
        })
    }
}
