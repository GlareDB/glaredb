//! Various create messages/structs.
use rayexec_error::{RayexecError, Result};
use rayexec_proto::ProtoConv;

use crate::arrays::field::Field;
use crate::functions::copy::CopyToFunction;
use crate::functions::function_set::{AggregateFunctionSet, ScalarFunctionSet};
use crate::functions::table::TableFunction;

/// Behavior on create conflict.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum OnConflict {
    /// Ignore and return ok.
    ///
    /// CREATE IF NOT EXIST
    Ignore,
    /// Replace the original entry.
    ///
    /// CREATE OR REPLACE
    Replace,
    /// Error on conflict.
    #[default]
    Error,
}

impl ProtoConv for OnConflict {
    type ProtoType = rayexec_proto::generated::execution::OnConflict;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(match self {
            Self::Ignore => Self::ProtoType::Ignore,
            Self::Replace => Self::ProtoType::Replace,
            Self::Error => Self::ProtoType::Error,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(match proto {
            Self::ProtoType::InvalidOnConflict => return Err(RayexecError::new("invalid")),
            Self::ProtoType::Ignore => Self::Ignore,
            Self::ProtoType::Replace => Self::Replace,
            Self::ProtoType::Error => Self::Error,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableInfo {
    pub name: String,
    pub columns: Vec<Field>,
    pub on_conflict: OnConflict,
}

impl ProtoConv for CreateTableInfo {
    type ProtoType = rayexec_proto::generated::execution::CreateTableInfo;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            name: self.name.clone(),
            columns: self
                .columns
                .iter()
                .map(|f| f.to_proto())
                .collect::<Result<Vec<_>>>()?,
            on_conflict: self.on_conflict.to_proto()? as i32,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            on_conflict: OnConflict::from_proto(proto.on_conflict())?,
            name: proto.name,
            columns: proto
                .columns
                .into_iter()
                .map(Field::from_proto)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSchemaInfo {
    pub name: String,
    pub on_conflict: OnConflict,
}

impl ProtoConv for CreateSchemaInfo {
    type ProtoType = rayexec_proto::generated::execution::CreateSchemaInfo;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            name: self.name.clone(),
            on_conflict: self.on_conflict.to_proto()? as i32,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            on_conflict: OnConflict::from_proto(proto.on_conflict())?,
            name: proto.name,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateViewInfo {
    pub name: String,
    pub column_aliases: Option<Vec<String>>,
    pub on_conflict: OnConflict,
    // TODO: Currently just stores the string, would be nice to store something
    // a bit more structured like a parsed or bound state.
    //
    // But that would required they be a bit more stable than they currently are.
    pub query_string: String,
}

#[derive(Debug)]
pub struct CreateScalarFunctionInfo {
    pub name: String,
    pub implementation: ScalarFunctionSet,
    pub on_conflict: OnConflict,
}

#[derive(Debug)]
pub struct CreateAggregateFunctionInfo {
    pub name: String,
    pub implementation: AggregateFunctionSet,
    pub on_conflict: OnConflict,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CreateTableFunctionInfo {
    pub name: String,
    pub implementation: Box<dyn TableFunction>,
    pub on_conflict: OnConflict,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CreateCopyToFunctionInfo {
    pub name: String,
    pub format: String,
    pub implementation: Box<dyn CopyToFunction>,
    pub on_conflict: OnConflict,
}
