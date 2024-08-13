use rayexec_bullet::field::Field;
use rayexec_error::{RayexecError, Result};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use crate::functions::{
    aggregate::AggregateFunction, scalar::ScalarFunction, table::TableFunction,
};

#[derive(Debug, Clone)]
pub enum CatalogEntry {
    Table(TableEntry),
    Function(FunctionEntry),
}

impl CatalogEntry {
    pub fn try_as_function(self) -> Result<FunctionEntry> {
        match self {
            Self::Function(f) => Ok(f),
            _ => Err(RayexecError::new("Not a function")),
        }
    }
}

impl From<FunctionEntry> for CatalogEntry {
    fn from(value: FunctionEntry) -> Self {
        CatalogEntry::Function(value)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableEntry {
    pub name: String,
    pub columns: Vec<Field>,
}

impl ProtoConv for TableEntry {
    type ProtoType = rayexec_proto::generated::execution::TableEntry;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            name: self.name.clone(),
            columns: self
                .columns
                .iter()
                .map(|c| c.to_proto())
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            name: proto.name,
            columns: proto
                .columns
                .into_iter()
                .map(Field::from_proto)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct FunctionEntry {
    pub name: String,
    pub implementation: FunctionImpl,
}

#[derive(Debug, Clone)]
pub enum FunctionImpl {
    Scalar(Box<dyn ScalarFunction>),
    Aggregate(Box<dyn AggregateFunction>),
    Table(Box<dyn TableFunction>),
}
