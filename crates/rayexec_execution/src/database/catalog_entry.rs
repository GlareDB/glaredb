use std::fmt;
use std::sync::Arc;

use rayexec_error::{OptionExt, RayexecError, Result};
use rayexec_proto::ProtoConv;

use super::DatabaseContext;
use crate::arrays::field::Field;
use crate::functions::aggregate::AggregateFunction;
use crate::functions::copy::CopyToFunction;
use crate::functions::scalar::ScalarFunction;
use crate::functions::table::TableFunction;
use crate::proto::DatabaseProtoConv;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogEntryType {
    Table,
    Schema,
    View,
    ScalarFunction,
    AggregateFunction,
    TableFunction,
    CopyToFunction,
}

impl fmt::Display for CatalogEntryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table => write!(f, "table"),
            Self::Schema => write!(f, "schema"),
            Self::View => write!(f, "view"),
            Self::ScalarFunction => write!(f, "scalar function"),
            Self::AggregateFunction => write!(f, "aggregate function"),
            Self::TableFunction => write!(f, "table function"),
            Self::CopyToFunction => write!(f, "copy to function"),
        }
    }
}

impl ProtoConv for CatalogEntryType {
    type ProtoType = rayexec_proto::generated::catalog::CatalogEntryType;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(match self {
            Self::Table => Self::ProtoType::Table,
            Self::Schema => Self::ProtoType::Schema,
            Self::View => unimplemented!(),
            Self::ScalarFunction => Self::ProtoType::ScalarFunction,
            Self::AggregateFunction => Self::ProtoType::AggregateFunction,
            Self::TableFunction => Self::ProtoType::TableFunction,
            Self::CopyToFunction => Self::ProtoType::CopyToFunction,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(match proto {
            Self::ProtoType::Invalid => return Err(RayexecError::new("inval catalog entry type")),
            Self::ProtoType::Table => Self::Table,
            Self::ProtoType::Schema => Self::Schema,
            Self::ProtoType::ScalarFunction => Self::ScalarFunction,
            Self::ProtoType::AggregateFunction => Self::AggregateFunction,
            Self::ProtoType::TableFunction => Self::TableFunction,
            Self::ProtoType::CopyToFunction => Self::CopyToFunction,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CatalogEntry {
    pub oid: u32,
    pub name: String,
    pub entry: CatalogEntryInner,
    pub child: Option<Arc<CatalogEntry>>,
}

impl DatabaseProtoConv for CatalogEntry {
    type ProtoType = rayexec_proto::generated::catalog::CatalogEntry;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        let child = self
            .child
            .as_ref()
            .map(|c| c.to_proto_ctx(context))
            .transpose()?;

        Ok(Self::ProtoType {
            oid: self.oid,
            name: self.name.clone(),
            entry: Some(self.entry.to_proto_ctx(context)?),
            child: child.map(Box::new),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        let child = proto
            .child
            .map(|c| DatabaseProtoConv::from_proto_ctx(*c, context))
            .transpose()?;

        Ok(Self {
            oid: proto.oid,
            name: proto.name,
            entry: DatabaseProtoConv::from_proto_ctx(proto.entry.required("entry")?, context)?,
            child: child.map(Arc::new),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum CatalogEntryInner {
    Table(TableEntry),
    Schema(SchemaEntry),
    View(ViewEntry),
    ScalarFunction(ScalarFunctionEntry),
    AggregateFunction(AggregateFunctionEntry),
    TableFunction(TableFunctionEntry),
    CopyToFunction(CopyToFunctionEntry),
}

impl DatabaseProtoConv for CatalogEntryInner {
    type ProtoType = rayexec_proto::generated::catalog::CatalogEntryInner;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::catalog::catalog_entry_inner::Value;

        let value = match self {
            Self::Table(ent) => Value::Table(ent.to_proto()?),
            Self::Schema(ent) => Value::Schema(ent.to_proto()?),
            Self::View(_ent) => unimplemented!(),
            Self::ScalarFunction(ent) => Value::ScalarFunction(ent.to_proto_ctx(context)?),
            Self::AggregateFunction(ent) => Value::AggregateFunction(ent.to_proto_ctx(context)?),
            Self::TableFunction(ent) => Value::TableFunction(ent.to_proto_ctx(context)?),
            Self::CopyToFunction(ent) => Value::CopyToFunction(ent.to_proto_ctx(context)?),
        };

        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        use rayexec_proto::generated::catalog::catalog_entry_inner::Value;

        Ok(match proto.value.required("value")? {
            Value::Table(ent) => Self::Table(ProtoConv::from_proto(ent)?),
            Value::Schema(ent) => Self::Schema(ProtoConv::from_proto(ent)?),
            Value::ScalarFunction(ent) => {
                Self::ScalarFunction(DatabaseProtoConv::from_proto_ctx(ent, context)?)
            }
            Value::AggregateFunction(ent) => {
                Self::AggregateFunction(DatabaseProtoConv::from_proto_ctx(ent, context)?)
            }
            Value::TableFunction(ent) => {
                Self::TableFunction(DatabaseProtoConv::from_proto_ctx(ent, context)?)
            }
            Value::CopyToFunction(ent) => {
                Self::CopyToFunction(DatabaseProtoConv::from_proto_ctx(ent, context)?)
            }
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ScalarFunctionEntry {
    pub function: Box<dyn ScalarFunction>,
}

impl DatabaseProtoConv for ScalarFunctionEntry {
    type ProtoType = rayexec_proto::generated::catalog::ScalarFunctionEntry;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            function: Some(self.function.to_proto_ctx(context)?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            function: DatabaseProtoConv::from_proto_ctx(
                proto.function.required("function")?,
                context,
            )?,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct AggregateFunctionEntry {
    pub function: Box<dyn AggregateFunction>,
}

impl DatabaseProtoConv for AggregateFunctionEntry {
    type ProtoType = rayexec_proto::generated::catalog::AggregateFunctionEntry;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            function: Some(self.function.to_proto_ctx(context)?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            function: DatabaseProtoConv::from_proto_ctx(
                proto.function.required("function")?,
                context,
            )?,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct TableFunctionEntry {
    pub function: Box<dyn TableFunction>,
}

impl DatabaseProtoConv for TableFunctionEntry {
    type ProtoType = rayexec_proto::generated::catalog::TableFunctionEntry;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            function: Some(self.function.to_proto_ctx(context)?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            function: DatabaseProtoConv::from_proto_ctx(
                proto.function.required("function")?,
                context,
            )?,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct CopyToFunctionEntry {
    /// COPY TO function implemenation.
    pub function: Box<dyn CopyToFunction>,
    /// The format this COPY TO is for.
    ///
    /// For example, this should be 'parquet' for the parquet COPY TO. This is
    /// looked at when the user includes a `(FORMAT <format>)` option in the
    /// statement.
    pub format: String,
}

impl DatabaseProtoConv for CopyToFunctionEntry {
    type ProtoType = rayexec_proto::generated::catalog::CopyToFunctionEntry;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            function: Some(self.function.to_proto_ctx(context)?),
            format: self.format.clone(),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            function: DatabaseProtoConv::from_proto_ctx(
                proto.function.required("function")?,
                context,
            )?,
            format: proto.format,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableEntry {
    pub columns: Vec<Field>,
}

impl ProtoConv for TableEntry {
    type ProtoType = rayexec_proto::generated::catalog::TableEntry;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            columns: self
                .columns
                .iter()
                .map(|c| c.to_proto())
                .collect::<Result<_>>()?,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            columns: proto
                .columns
                .into_iter()
                .map(ProtoConv::from_proto)
                .collect::<Result<_>>()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewEntry {
    pub column_aliases: Option<Vec<String>>,
    pub query_sql: String,
}

impl ProtoConv for ViewEntry {
    type ProtoType = ();

    fn to_proto(&self) -> Result<Self::ProtoType> {
        unimplemented!()
    }

    fn from_proto(_proto: Self::ProtoType) -> Result<Self> {
        unimplemented!()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SchemaEntry {}

impl ProtoConv for SchemaEntry {
    type ProtoType = rayexec_proto::generated::catalog::SchemaEntry;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {})
    }

    fn from_proto(_proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {})
    }
}

impl CatalogEntry {
    pub fn entry_type(&self) -> CatalogEntryType {
        match &self.entry {
            CatalogEntryInner::Table(_) => CatalogEntryType::Table,
            CatalogEntryInner::Schema(_) => CatalogEntryType::Schema,
            CatalogEntryInner::View(_) => CatalogEntryType::View,
            CatalogEntryInner::ScalarFunction(_) => CatalogEntryType::ScalarFunction,
            CatalogEntryInner::AggregateFunction(_) => CatalogEntryType::AggregateFunction,
            CatalogEntryInner::TableFunction(_) => CatalogEntryType::TableFunction,
            CatalogEntryInner::CopyToFunction(_) => CatalogEntryType::CopyToFunction,
        }
    }

    pub fn try_as_table_entry(&self) -> Result<&TableEntry> {
        match &self.entry {
            CatalogEntryInner::Table(ent) => Ok(ent),
            _ => Err(RayexecError::new("Entry not a table")),
        }
    }

    pub fn try_as_schema_entry(&self) -> Result<&SchemaEntry> {
        match &self.entry {
            CatalogEntryInner::Schema(ent) => Ok(ent),
            _ => Err(RayexecError::new("Entry not a schema")),
        }
    }

    pub fn try_as_scalar_function_entry(&self) -> Result<&ScalarFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::ScalarFunction(ent) => Ok(ent),
            _ => Err(RayexecError::new("Entry not a scalar function")),
        }
    }

    pub fn try_as_aggregate_function_entry(&self) -> Result<&AggregateFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::AggregateFunction(ent) => Ok(ent),
            _ => Err(RayexecError::new("Entry not an aggregate function")),
        }
    }

    pub fn try_as_table_function_entry(&self) -> Result<&TableFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::TableFunction(ent) => Ok(ent),
            _ => Err(RayexecError::new("Entry not a table function")),
        }
    }

    pub fn try_as_copy_to_function_entry(&self) -> Result<&CopyToFunctionEntry> {
        match &self.entry {
            CatalogEntryInner::CopyToFunction(ent) => Ok(ent),
            _ => Err(RayexecError::new("Entry not a copy to function")),
        }
    }
}
