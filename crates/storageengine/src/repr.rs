use crate::errors::Result;
use lemur::repr::df::Schema;
use lemur::repr::relation::RelationKey;
use lemur::repr::value::{Row, Value};
use serde::{Deserialize, Serialize};

/// Key representations.
///
/// TODO: Timestamp these to provide transactional semantics.
#[derive(Debug, Serialize, Deserialize)]
pub enum Key {
    /// A primary record.
    Primary(RelationKey, Vec<Value>),
    /// Schema for a table.
    Schema(RelationKey),
}

impl Key {
    pub fn serialize(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn deserialize<B: AsRef<[u8]>>(buf: B) -> Result<Self> {
        Ok(bincode::deserialize(buf.as_ref())?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum InternalValue {
    PrimaryRecord(Row),
    Tombstone,
    Schema(Schema),
}

impl InternalValue {
    pub fn serialize(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn deserialize<B: AsRef<[u8]>>(buf: B) -> Result<Self> {
        Ok(bincode::deserialize(buf.as_ref())?)
    }
}
