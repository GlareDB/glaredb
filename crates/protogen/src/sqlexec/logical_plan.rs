use crate::{
    gen::metastore::{
        options::TableOptions,
        service::{
            AlterDatabaseRename, AlterTunnelRotateKeys, CreateCredentials, CreateExternalDatabase,
            CreateTunnel,
        },
    },
    ProtoConvError,
};
use std::borrow::Cow;

use datafusion_proto::protobuf::{DfSchema, LogicalPlanNode, OwnedTableReference};
use prost::{Message, Oneof};

#[derive(Clone, PartialEq, Message)]
pub struct CreateTable {
    #[prost(message, tag = "1")]
    pub table_name: Option<OwnedTableReference>,
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
    #[prost(message, optional, tag = "3")]
    pub schema: Option<DfSchema>,
    #[prost(message, optional, tag = "4")]
    pub source: Option<LogicalPlanNode>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateExternalTable {
    #[prost(message, tag = "1")]
    pub table_name: Option<OwnedTableReference>,
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
    #[prost(message, tag = "3")]
    pub table_options: Option<TableOptions>,
    #[prost(message, optional, tag = "4")]
    pub tunnel: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateSchema {
    #[prost(message, tag = "1")]
    pub schema_name: Option<OwnedSchemaReference>,
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct DropTables {
    #[prost(message, repeated, tag = "1")]
    pub names: Vec<OwnedTableReference>,
    #[prost(bool, tag = "2")]
    pub if_exists: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct AlterTableRename {
    #[prost(message, tag = "1")]
    pub name: Option<OwnedTableReference>,
    #[prost(message, tag = "2")]
    pub new_name: Option<OwnedTableReference>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Message)]
pub struct LogicalPlanExtension {
    #[prost(
        oneof = "LogicalPlanExtensionType",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10"
    )]
    pub inner: Option<LogicalPlanExtensionType>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Oneof)]
pub enum LogicalPlanExtensionType {
    #[prost(message, tag = "1")]
    CreateTable(CreateTable),
    #[prost(message, tag = "2")]
    CreateSchema(CreateSchema),
    #[prost(message, tag = "3")]
    CreateExternalTable(CreateExternalTable),
    #[prost(message, tag = "4")]
    DropTables(DropTables),
    #[prost(message, tag = "5")]
    AlterTableRename(AlterTableRename),
    #[prost(message, tag = "6")]
    AlterDatabaseRename(AlterDatabaseRename),
    #[prost(message, tag = "7")]
    AlterTunnelRotateKeys(AlterTunnelRotateKeys),
    #[prost(message, tag = "8")]
    CreateCredentials(CreateCredentials),
    #[prost(message, tag = "9")]
    CreateExternalDatabase(CreateExternalDatabase),
    #[prost(message, tag = "10")]
    CreateTunnel(CreateTunnel),
}

// -----
// boilerplate below here
// -----

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Message)]
pub struct OwnedSchemaReference {
    #[prost(oneof = "SchemaReferenceEnum", tags = "1, 2")]
    pub schema_reference_enum: Option<SchemaReferenceEnum>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Message)]
pub struct BareSchemaReference {
    #[prost(string, tag = "1")]
    pub table: String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Message)]
pub struct FullSchemaReference {
    #[prost(string, tag = "1")]
    pub schema: String,
    #[prost(string, tag = "2")]
    pub catalog: String,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Oneof)]
pub enum SchemaReferenceEnum {
    #[prost(message, tag = "1")]
    Bare(BareSchemaReference),
    #[prost(message, tag = "2")]
    Full(FullSchemaReference),
}

impl TryFrom<OwnedSchemaReference> for datafusion::common::OwnedSchemaReference {
    type Error = crate::errors::ProtoConvError;

    fn try_from(value: OwnedSchemaReference) -> Result<Self, Self::Error> {
        let table_reference_enum = value
            .schema_reference_enum
            .ok_or_else(|| ProtoConvError::RequiredField("schema_reference_enum".to_string()))?;

        match table_reference_enum {
            SchemaReferenceEnum::Bare(BareSchemaReference { table }) => {
                Ok(datafusion::common::OwnedSchemaReference::Bare {
                    schema: Cow::Owned(table),
                })
            }
            SchemaReferenceEnum::Full(FullSchemaReference { catalog, schema }) => {
                Ok(datafusion::common::OwnedSchemaReference::Full {
                    catalog: Cow::Owned(catalog),
                    schema: Cow::Owned(schema),
                })
            }
        }
    }
}

impl TryFrom<datafusion::common::OwnedSchemaReference> for OwnedSchemaReference {
    type Error = crate::errors::ProtoConvError;

    fn try_from(value: datafusion::common::OwnedSchemaReference) -> Result<Self, Self::Error> {
        match value {
            datafusion::common::SchemaReference::Bare { schema } => Ok(OwnedSchemaReference {
                schema_reference_enum: Some(SchemaReferenceEnum::Bare(BareSchemaReference {
                    table: schema.into_owned(),
                })),
            }),
            datafusion::common::SchemaReference::Full { schema, catalog } => {
                Ok(OwnedSchemaReference {
                    schema_reference_enum: Some(SchemaReferenceEnum::Full(FullSchemaReference {
                        schema: schema.into_owned(),
                        catalog: catalog.into_owned(),
                    })),
                })
            }
        }
    }
}
