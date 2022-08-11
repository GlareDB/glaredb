use anyhow::Result;
use lemur::repr::value::{Row, Value};
use serde::{Deserialize, Serialize};

pub type TableId = String;

pub type PrimaryKeyIndices<'a> = &'a [usize];

pub type PrimaryKey<'a> = &'a [Value];

/// Key representations.
///
/// TODO: Timestamp these.
#[derive(Debug, Serialize, Deserialize)]
pub enum Key {
    /// A primary record.
    Primary(TableId, Vec<Value>),
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
    PrimaryRecord(Row), // TODO: Write intents.
    Tombstone,
}

impl InternalValue {
    pub fn serialize(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn deserialize<B: AsRef<[u8]>>(buf: B) -> Result<Self> {
        Ok(bincode::deserialize(buf.as_ref())?)
    }
}
