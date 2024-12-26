use std::sync::Arc;

use rayexec_error::{OptionExt, Result};
use rayexec_parser::ast;
use rayexec_proto::ProtoConv;

use crate::database::catalog_entry::CatalogEntry;
use crate::database::{AttachInfo, DatabaseContext};
use crate::proto::DatabaseProtoConv;

/// Table or CTE found in the FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedTableOrCteReference {
    /// Resolved table.
    Table(ResolvedTableReference),
    /// Resolved CTE.
    ///
    /// Stores the normalized name of the CTE so that it can be looked up during
    /// binding.
    Cte(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedTableReference {
    pub catalog: String,
    pub schema: String,
    pub entry: Arc<CatalogEntry>,
}

impl DatabaseProtoConv for ResolvedTableOrCteReference {
    type ProtoType = rayexec_proto::generated::resolver::ResolvedTableOrCteReference;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::resolver::resolved_table_or_cte_reference::Value;
        use rayexec_proto::generated::resolver::{
            ResolvedCteReference,
            ResolvedTableReference as ProtoResolvedTableReference,
        };

        let value = match self {
            Self::Table(ResolvedTableReference {
                catalog,
                schema,
                entry,
            }) => Value::Table(ProtoResolvedTableReference {
                catalog: catalog.clone(),
                schema: schema.clone(),
                entry: Some(entry.to_proto_ctx(context)?),
            }),
            Self::Cte(name) => Value::Cte(ResolvedCteReference { name: name.clone() }),
        };

        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        use rayexec_proto::generated::resolver::resolved_table_or_cte_reference::Value;

        Ok(match proto.value.required("value")? {
            Value::Table(table) => Self::Table(ResolvedTableReference {
                catalog: table.catalog,
                schema: table.schema,
                entry: Arc::new(CatalogEntry::from_proto_ctx(
                    table.entry.required("entry")?,
                    context,
                )?),
            }),
            Value::Cte(cte) => Self::Cte(cte.name),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedTableReference {
    /// The raw ast reference.
    pub reference: ast::ObjectReference,
    /// Name of the catalog this table is in.
    pub catalog: String,
    /// How we attach the catalog.
    ///
    /// Currently it's expected that this is always Some (we get attach info
    /// from the client), but there's a path where this can be None, and attach
    /// info gets injected on the server-side. Right now, the server will error
    /// if this is None.
    pub attach_info: Option<AttachInfo>,
}

impl ProtoConv for UnresolvedTableReference {
    type ProtoType = rayexec_proto::generated::resolver::UnresolvedTableReference;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            reference: Some(self.reference.to_proto()?),
            catalog: self.catalog.clone(),
            attach_info: self
                .attach_info
                .as_ref()
                .map(|i| i.to_proto())
                .transpose()?,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            reference: ProtoConv::from_proto(proto.reference.required("reference")?)?,
            catalog: proto.catalog,
            attach_info: proto.attach_info.map(ProtoConv::from_proto).transpose()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use rayexec_proto::testutil::assert_proto_roundtrip;

    use super::*;
    use crate::arrays::scalar::OwnedScalarValue;

    #[test]
    fn roundtrip_unbound_table_reference() {
        let reference = UnresolvedTableReference {
            reference: ast::ObjectReference::from_strings(["my", "table"]),
            catalog: "catalog".to_string(),
            attach_info: Some(AttachInfo {
                datasource: "snowbricks".to_string(),
                options: [("key".to_string(), OwnedScalarValue::Float32(3.5))].into(),
            }),
        };

        assert_proto_roundtrip(reference);
    }
}
