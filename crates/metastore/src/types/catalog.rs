use super::{FromOptionalField, ProtoConvError};
use crate::proto::arrow;
use crate::proto::catalog;
use datafusion::arrow::datatypes::DataType;
use proptest_derive::Arbitrary;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone)]
pub struct CatalogState {
    pub version: u64,
    pub entries: HashMap<u32, CatalogEntry>,
}

impl TryFrom<catalog::CatalogState> for CatalogState {
    type Error = ProtoConvError;
    fn try_from(value: catalog::CatalogState) -> Result<Self, Self::Error> {
        let mut entries = HashMap::with_capacity(value.entries.len());
        for (id, ent) in value.entries {
            entries.insert(id, ent.try_into()?);
        }
        Ok(CatalogState {
            version: value.version,
            entries,
        })
    }
}

impl TryFrom<CatalogState> for catalog::CatalogState {
    type Error = ProtoConvError;
    fn try_from(value: CatalogState) -> Result<Self, Self::Error> {
        Ok(catalog::CatalogState {
            version: value.version,
            entries: value
                .entries
                .into_iter()
                .map(|(id, ent)| match ent.try_into() {
                    Ok(ent) => Ok((id, ent)),
                    Err(e) => Err(e),
                })
                .collect::<Result<_, _>>()?,
        })
    }
}

// TODO: Implement Arbitrary and add test. This would require implementing
// Arbitrary for arrow's DataType.
#[derive(Debug, Clone)]
pub enum CatalogEntry {
    Schema(SchemaEntry),
    Table(TableEntry),
    View(ViewEntry),
    Connection(ConnectionEntry),
    ExternalTable(ExternalTableEntry),
}

impl CatalogEntry {
    pub const fn entry_type(&self) -> EntryType {
        match self {
            CatalogEntry::Schema(_) => EntryType::Schema,
            CatalogEntry::View(_) => EntryType::View,
            CatalogEntry::Table(_) => EntryType::Table,
            CatalogEntry::Connection(_) => EntryType::Connection,
            CatalogEntry::ExternalTable(_) => EntryType::ExternalTable,
        }
    }

    pub const fn is_schema(&self) -> bool {
        matches!(self, CatalogEntry::Schema(_))
    }

    /// Get the entry metadata.
    pub fn get_meta(&self) -> &EntryMeta {
        match self {
            CatalogEntry::Schema(schema) => &schema.meta,
            CatalogEntry::View(view) => &view.meta,
            CatalogEntry::Table(table) => &table.meta,
            CatalogEntry::Connection(conn) => &conn.meta,
            CatalogEntry::ExternalTable(tbl) => &tbl.meta,
        }
    }
}

impl TryFrom<catalog::catalog_entry::Entry> for CatalogEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::catalog_entry::Entry) -> Result<Self, Self::Error> {
        Ok(match value {
            catalog::catalog_entry::Entry::Schema(v) => CatalogEntry::Schema(v.try_into()?),
            catalog::catalog_entry::Entry::Table(v) => CatalogEntry::Table(v.try_into()?),
            catalog::catalog_entry::Entry::View(v) => CatalogEntry::View(v.try_into()?),
            catalog::catalog_entry::Entry::Connection(v) => CatalogEntry::Connection(v.try_into()?),
            catalog::catalog_entry::Entry::ExternalTable(v) => {
                CatalogEntry::ExternalTable(v.try_into()?)
            }
        })
    }
}

impl TryFrom<catalog::CatalogEntry> for CatalogEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::CatalogEntry) -> Result<Self, Self::Error> {
        value.entry.required("entry")
    }
}

impl TryFrom<CatalogEntry> for catalog::CatalogEntry {
    type Error = ProtoConvError;
    fn try_from(value: CatalogEntry) -> Result<Self, Self::Error> {
        let ent = match value {
            CatalogEntry::Schema(v) => catalog::catalog_entry::Entry::Schema(v.into()),
            CatalogEntry::View(v) => catalog::catalog_entry::Entry::View(v.into()),
            CatalogEntry::Table(v) => catalog::catalog_entry::Entry::Table(v.try_into()?),
            CatalogEntry::Connection(v) => catalog::catalog_entry::Entry::Connection(v.into()),
            CatalogEntry::ExternalTable(v) => {
                catalog::catalog_entry::Entry::ExternalTable(v.into())
            }
        };
        Ok(catalog::CatalogEntry { entry: Some(ent) })
    }
}

#[derive(Debug, Clone, Copy, Arbitrary, PartialEq, Eq)]
pub enum EntryType {
    Schema,
    ExternalTable,
    Table,
    View,
    Connection,
}

impl TryFrom<i32> for EntryType {
    type Error = ProtoConvError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        catalog::entry_meta::EntryType::from_i32(value)
            .ok_or(ProtoConvError::UnknownEnumVariant("EntryType", value))
            .and_then(|t| t.try_into())
    }
}

impl TryFrom<catalog::entry_meta::EntryType> for EntryType {
    type Error = ProtoConvError;
    fn try_from(value: catalog::entry_meta::EntryType) -> Result<Self, Self::Error> {
        Ok(match value {
            catalog::entry_meta::EntryType::Unknown => {
                return Err(ProtoConvError::ZeroValueEnumVariant("EntryType"))
            }
            catalog::entry_meta::EntryType::Schema => EntryType::Schema,
            catalog::entry_meta::EntryType::ExternalTable => EntryType::ExternalTable,
            catalog::entry_meta::EntryType::Table => EntryType::Table,
            catalog::entry_meta::EntryType::View => EntryType::View,
            catalog::entry_meta::EntryType::Connection => EntryType::Connection,
        })
    }
}

impl From<EntryType> for catalog::entry_meta::EntryType {
    fn from(value: EntryType) -> Self {
        match value {
            EntryType::Schema => catalog::entry_meta::EntryType::Schema,
            EntryType::Table => catalog::entry_meta::EntryType::Table,
            EntryType::ExternalTable => catalog::entry_meta::EntryType::ExternalTable,
            EntryType::View => catalog::entry_meta::EntryType::View,
            EntryType::Connection => catalog::entry_meta::EntryType::Connection,
        }
    }
}

impl fmt::Display for EntryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            catalog::entry_meta::EntryType::from(*self).as_str_name()
        )
    }
}

/// Metadata associated with every entry in the catalog.
#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct EntryMeta {
    pub entry_type: EntryType,
    pub id: u32,
    pub parent: u32,
    pub name: String,
    pub builtin: bool,
}

impl From<EntryMeta> for catalog::EntryMeta {
    fn from(value: EntryMeta) -> Self {
        let entry_type: catalog::entry_meta::EntryType = value.entry_type.into();
        catalog::EntryMeta {
            entry_type: entry_type as i32,
            id: value.id,
            parent: value.parent,
            name: value.name,
            builtin: value.builtin,
        }
    }
}

impl TryFrom<catalog::EntryMeta> for EntryMeta {
    type Error = ProtoConvError;
    fn try_from(value: catalog::EntryMeta) -> Result<Self, Self::Error> {
        Ok(EntryMeta {
            entry_type: value.entry_type.try_into()?,
            id: value.id,
            parent: value.parent,
            name: value.name,
            builtin: value.builtin,
        })
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct SchemaEntry {
    pub meta: EntryMeta,
}

impl TryFrom<catalog::SchemaEntry> for SchemaEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::SchemaEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(SchemaEntry { meta })
    }
}

impl From<SchemaEntry> for catalog::SchemaEntry {
    fn from(value: SchemaEntry) -> Self {
        catalog::SchemaEntry {
            meta: Some(value.meta.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableEntry {
    pub meta: EntryMeta,
    pub columns: Vec<ColumnDefinition>,
}

impl TryFrom<catalog::TableEntry> for TableEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(TableEntry {
            meta,
            columns: value
                .columns
                .into_iter()
                .map(|col| col.try_into())
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<TableEntry> for catalog::TableEntry {
    type Error = ProtoConvError;
    fn try_from(value: TableEntry) -> Result<Self, Self::Error> {
        Ok(catalog::TableEntry {
            meta: Some(value.meta.into()),
            columns: value
                .columns
                .into_iter()
                .map(|col| col.try_into())
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub nullable: bool,
    pub arrow_type: DataType,
}

impl ColumnDefinition {
    /// Create a vec of column definitions.
    ///
    /// Tuples are in the form of:
    /// (name, datatype, nullable)
    pub fn from_tuples<C, N>(cols: C) -> Vec<ColumnDefinition>
    where
        C: IntoIterator<Item = (N, DataType, bool)>,
        N: Into<String>,
    {
        cols.into_iter()
            .map(|(name, arrow_type, nullable)| ColumnDefinition {
                name: name.into(),
                nullable,
                arrow_type,
            })
            .collect()
    }
}

impl TryFrom<catalog::ColumnDefinition> for ColumnDefinition {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ColumnDefinition) -> Result<Self, Self::Error> {
        let arrow_type: DataType = value.arrow_type.as_ref().required("arrow_type")?;
        Ok(ColumnDefinition {
            name: value.name,
            nullable: value.nullable,
            arrow_type,
        })
    }
}

// TODO: Try to make this just `From`. Would require some additional conversions
// for the arrow types.
impl TryFrom<ColumnDefinition> for catalog::ColumnDefinition {
    type Error = ProtoConvError;
    fn try_from(value: ColumnDefinition) -> Result<Self, Self::Error> {
        let arrow_type = arrow::ArrowType::try_from(&value.arrow_type)?;
        Ok(catalog::ColumnDefinition {
            name: value.name,
            nullable: value.nullable,
            arrow_type: Some(arrow_type),
        })
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct ViewEntry {
    pub meta: EntryMeta,
    pub sql: String,
}

impl TryFrom<catalog::ViewEntry> for ViewEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ViewEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(ViewEntry {
            meta,
            sql: value.sql,
        })
    }
}

impl From<ViewEntry> for catalog::ViewEntry {
    fn from(value: ViewEntry) -> Self {
        catalog::ViewEntry {
            meta: Some(value.meta.into()),
            sql: value.sql,
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct ConnectionEntry {
    pub meta: EntryMeta,
    pub options: ConnectionOptions,
}

impl TryFrom<catalog::ConnectionEntry> for ConnectionEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ConnectionEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(ConnectionEntry {
            meta,
            options: value.options.required("options")?,
        })
    }
}

impl From<ConnectionEntry> for catalog::ConnectionEntry {
    fn from(value: ConnectionEntry) -> Self {
        catalog::ConnectionEntry {
            meta: Some(value.meta.into()),
            options: Some(value.options.into()),
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct ExternalTableEntry {
    pub meta: EntryMeta,
    pub connection_id: u32,
    pub options: TableOptions,
}

impl TryFrom<catalog::ExternalTableEntry> for ExternalTableEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ExternalTableEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(ExternalTableEntry {
            meta,
            connection_id: value.connection_id,
            options: value.options.required("options")?,
        })
    }
}

impl From<ExternalTableEntry> for catalog::ExternalTableEntry {
    fn from(value: ExternalTableEntry) -> Self {
        catalog::ExternalTableEntry {
            meta: Some(value.meta.into()),
            connection_id: value.connection_id,
            options: Some(value.options.into()),
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub enum TableOptions {
    Debug(TableOptionsDebug),
    Postgres(TableOptionsPostgres),
    BigQuery(TableOptionsBigQuery),
    Mysql(TableOptionsMysql),
    Local(TableOptionsLocal),
    Gcs(TableOptionsGcs),
    S3(TableOptionsS3),
}

impl TableOptions {
    pub const DEBUG: &str = "debug";
    pub const POSTGRES: &str = "postgres";
    pub const BIGQUERY: &str = "bigquery";
    pub const MYSQL: &str = "mysql";
    pub const LOCAL: &str = "local";
    pub const GCS: &str = "gcs";
    pub const S3_STORAGE: &str = "s3";

    pub fn as_str(&self) -> &'static str {
        match self {
            TableOptions::Debug(_) => Self::DEBUG,
            TableOptions::Postgres(_) => Self::POSTGRES,
            TableOptions::BigQuery(_) => Self::BIGQUERY,
            TableOptions::Mysql(_) => Self::MYSQL,
            TableOptions::Local(_) => Self::LOCAL,
            TableOptions::Gcs(_) => Self::GCS,
            TableOptions::S3(_) => Self::S3_STORAGE,
        }
    }
}

impl fmt::Display for TableOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<catalog::table_options::Options> for TableOptions {
    type Error = ProtoConvError;
    fn try_from(value: catalog::table_options::Options) -> Result<Self, Self::Error> {
        Ok(match value {
            catalog::table_options::Options::Debug(v) => TableOptions::Debug(v.try_into()?),
            catalog::table_options::Options::Postgres(v) => TableOptions::Postgres(v.try_into()?),
            catalog::table_options::Options::Bigquery(v) => TableOptions::BigQuery(v.try_into()?),
            catalog::table_options::Options::Mysql(v) => TableOptions::Mysql(v.try_into()?),
            catalog::table_options::Options::Local(v) => TableOptions::Local(v.try_into()?),
            catalog::table_options::Options::Gcs(v) => TableOptions::Gcs(v.try_into()?),
            catalog::table_options::Options::S3(v) => TableOptions::S3(v.try_into()?),
        })
    }
}

impl TryFrom<catalog::TableOptions> for TableOptions {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableOptions) -> Result<Self, Self::Error> {
        value.options.required("options")
    }
}

impl From<TableOptions> for catalog::table_options::Options {
    fn from(value: TableOptions) -> Self {
        match value {
            TableOptions::Debug(v) => catalog::table_options::Options::Debug(v.into()),
            TableOptions::Postgres(v) => catalog::table_options::Options::Postgres(v.into()),
            TableOptions::BigQuery(v) => catalog::table_options::Options::Bigquery(v.into()),
            TableOptions::Mysql(v) => catalog::table_options::Options::Mysql(v.into()),
            TableOptions::Local(v) => catalog::table_options::Options::Local(v.into()),
            TableOptions::Gcs(v) => catalog::table_options::Options::Gcs(v.into()),
            TableOptions::S3(v) => catalog::table_options::Options::S3(v.into()),
        }
    }
}

impl From<TableOptions> for catalog::TableOptions {
    fn from(value: TableOptions) -> Self {
        catalog::TableOptions {
            options: Some(value.into()),
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct TableOptionsDebug {
    pub table_type: String,
}

impl TryFrom<catalog::TableOptionsDebug> for TableOptionsDebug {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableOptionsDebug) -> Result<Self, Self::Error> {
        Ok(TableOptionsDebug {
            table_type: value.table_type,
        })
    }
}

impl From<TableOptionsDebug> for catalog::TableOptionsDebug {
    fn from(value: TableOptionsDebug) -> Self {
        catalog::TableOptionsDebug {
            table_type: value.table_type,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct TableOptionsPostgres {
    pub schema: String,
    pub table: String,
}

impl TryFrom<catalog::TableOptionsPostgres> for TableOptionsPostgres {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableOptionsPostgres) -> Result<Self, Self::Error> {
        Ok(TableOptionsPostgres {
            schema: value.schema,
            table: value.table,
        })
    }
}

impl From<TableOptionsPostgres> for catalog::TableOptionsPostgres {
    fn from(value: TableOptionsPostgres) -> Self {
        catalog::TableOptionsPostgres {
            schema: value.schema,
            table: value.table,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct TableOptionsBigQuery {
    pub dataset_id: String,
    pub table_id: String,
}

impl TryFrom<catalog::TableOptionsBigQuery> for TableOptionsBigQuery {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableOptionsBigQuery) -> Result<Self, Self::Error> {
        Ok(TableOptionsBigQuery {
            dataset_id: value.dataset_id,
            table_id: value.table_id,
        })
    }
}

impl From<TableOptionsBigQuery> for catalog::TableOptionsBigQuery {
    fn from(value: TableOptionsBigQuery) -> Self {
        catalog::TableOptionsBigQuery {
            dataset_id: value.dataset_id,
            table_id: value.table_id,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct TableOptionsMysql {
    pub schema: String,
    pub table: String,
}

impl TryFrom<catalog::TableOptionsMysql> for TableOptionsMysql {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableOptionsMysql) -> Result<Self, Self::Error> {
        Ok(TableOptionsMysql {
            schema: value.schema,
            table: value.table,
        })
    }
}

impl From<TableOptionsMysql> for catalog::TableOptionsMysql {
    fn from(value: TableOptionsMysql) -> Self {
        catalog::TableOptionsMysql {
            schema: value.schema,
            table: value.table,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct TableOptionsLocal {
    pub location: String,
}

impl TryFrom<catalog::TableOptionsLocal> for TableOptionsLocal {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableOptionsLocal) -> Result<Self, Self::Error> {
        Ok(TableOptionsLocal {
            location: value.location,
        })
    }
}

impl From<TableOptionsLocal> for catalog::TableOptionsLocal {
    fn from(value: TableOptionsLocal) -> Self {
        catalog::TableOptionsLocal {
            location: value.location,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct TableOptionsGcs {
    pub bucket_name: String,
    pub location: String,
}

impl TryFrom<catalog::TableOptionsGcs> for TableOptionsGcs {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableOptionsGcs) -> Result<Self, Self::Error> {
        Ok(TableOptionsGcs {
            bucket_name: value.bucket_name,
            location: value.location,
        })
    }
}

impl From<TableOptionsGcs> for catalog::TableOptionsGcs {
    fn from(value: TableOptionsGcs) -> Self {
        catalog::TableOptionsGcs {
            bucket_name: value.bucket_name,
            location: value.location,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct TableOptionsS3 {
    pub region: String,
    pub bucket_name: String,
    pub location: String,
}

impl TryFrom<catalog::TableOptionsS3> for TableOptionsS3 {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableOptionsS3) -> Result<Self, Self::Error> {
        Ok(TableOptionsS3 {
            region: value.region,
            bucket_name: value.bucket_name,
            location: value.location,
        })
    }
}

impl From<TableOptionsS3> for catalog::TableOptionsS3 {
    fn from(value: TableOptionsS3) -> Self {
        catalog::TableOptionsS3 {
            region: value.region,
            bucket_name: value.bucket_name,
            location: value.location,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub enum ConnectionOptions {
    Debug(ConnectionOptionsDebug),
    Postgres(ConnectionOptionsPostgres),
    BigQuery(ConnectionOptionsBigQuery),
    Mysql(ConnectionOptionsMysql),
    Local(ConnectionOptionsLocal),
    Gcs(ConnectionOptionsGcs),
    S3(ConnectionOptionsS3),
    Ssh(ConnectionOptionsSsh),
}

impl ConnectionOptions {
    pub const DEBUG: &str = "debug";
    pub const POSTGRES: &str = "postgres";
    pub const BIGQUERY: &str = "bigquery";
    pub const MYSQL: &str = "mysql";
    pub const LOCAL: &str = "local";
    pub const GCS: &str = "gcs";
    pub const S3_STORAGE: &str = "s3";
    pub const SSH: &str = "ssh";

    pub fn as_str(&self) -> &'static str {
        match self {
            ConnectionOptions::Debug(_) => Self::DEBUG,
            ConnectionOptions::Postgres(_) => Self::POSTGRES,
            ConnectionOptions::BigQuery(_) => Self::BIGQUERY,
            ConnectionOptions::Mysql(_) => Self::MYSQL,
            ConnectionOptions::Local(_) => Self::LOCAL,
            ConnectionOptions::Gcs(_) => Self::GCS,
            ConnectionOptions::S3(_) => Self::S3_STORAGE,
            ConnectionOptions::Ssh(_) => Self::SSH,
        }
    }
}

impl fmt::Display for ConnectionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<catalog::connection_options::Options> for ConnectionOptions {
    type Error = ProtoConvError;
    fn try_from(value: catalog::connection_options::Options) -> Result<Self, Self::Error> {
        Ok(match value {
            catalog::connection_options::Options::Debug(v) => {
                ConnectionOptions::Debug(v.try_into()?)
            }
            catalog::connection_options::Options::Postgres(v) => {
                ConnectionOptions::Postgres(v.try_into()?)
            }
            catalog::connection_options::Options::Bigquery(v) => {
                ConnectionOptions::BigQuery(v.try_into()?)
            }
            catalog::connection_options::Options::Mysql(v) => {
                ConnectionOptions::Mysql(v.try_into()?)
            }
            catalog::connection_options::Options::Local(v) => {
                ConnectionOptions::Local(v.try_into()?)
            }
            catalog::connection_options::Options::Gcs(v) => ConnectionOptions::Gcs(v.try_into()?),
            catalog::connection_options::Options::S3(v) => ConnectionOptions::S3(v.try_into()?),
            catalog::connection_options::Options::Ssh(v) => ConnectionOptions::Ssh(v.try_into()?),
        })
    }
}

impl TryFrom<catalog::ConnectionOptions> for ConnectionOptions {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ConnectionOptions) -> Result<Self, Self::Error> {
        value.options.required("options")
    }
}

impl From<ConnectionOptions> for catalog::connection_options::Options {
    fn from(value: ConnectionOptions) -> Self {
        match value {
            ConnectionOptions::Debug(v) => catalog::connection_options::Options::Debug(v.into()),
            ConnectionOptions::Postgres(v) => {
                catalog::connection_options::Options::Postgres(v.into())
            }
            ConnectionOptions::BigQuery(v) => {
                catalog::connection_options::Options::Bigquery(v.into())
            }
            ConnectionOptions::Mysql(v) => catalog::connection_options::Options::Mysql(v.into()),
            ConnectionOptions::Local(v) => catalog::connection_options::Options::Local(v.into()),
            ConnectionOptions::Gcs(v) => catalog::connection_options::Options::Gcs(v.into()),
            ConnectionOptions::S3(v) => catalog::connection_options::Options::S3(v.into()),
            ConnectionOptions::Ssh(v) => catalog::connection_options::Options::Ssh(v.into()),
        }
    }
}

impl From<ConnectionOptions> for catalog::ConnectionOptions {
    fn from(value: ConnectionOptions) -> Self {
        catalog::ConnectionOptions {
            options: Some(value.into()),
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct ConnectionOptionsDebug {}

impl TryFrom<catalog::ConnectionOptionsDebug> for ConnectionOptionsDebug {
    type Error = ProtoConvError;
    fn try_from(_value: catalog::ConnectionOptionsDebug) -> Result<Self, Self::Error> {
        Ok(ConnectionOptionsDebug {})
    }
}

impl From<ConnectionOptionsDebug> for catalog::ConnectionOptionsDebug {
    fn from(_value: ConnectionOptionsDebug) -> Self {
        catalog::ConnectionOptionsDebug {}
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct ConnectionOptionsPostgres {
    pub connection_string: String,
    pub ssh_tunnel: Option<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum PostgresValidationError {
    #[error("Coming soon! This feature is unimplemented")]
    Unimplemented,
}

impl ConnectionOptionsPostgres {
    /// Validate postgres connection
    pub fn validate(&self) -> Result<(), PostgresValidationError> {
        tracing::warn!("unimplemented");
        return Err(PostgresValidationError::Unimplemented);
    }
}

impl TryFrom<catalog::ConnectionOptionsPostgres> for ConnectionOptionsPostgres {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ConnectionOptionsPostgres) -> Result<Self, Self::Error> {
        Ok(ConnectionOptionsPostgres {
            connection_string: value.connection_string,
            ssh_tunnel: value.ssh_tunnel,
        })
    }
}

impl From<ConnectionOptionsPostgres> for catalog::ConnectionOptionsPostgres {
    fn from(value: ConnectionOptionsPostgres) -> Self {
        catalog::ConnectionOptionsPostgres {
            connection_string: value.connection_string,
            ssh_tunnel: value.ssh_tunnel,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct ConnectionOptionsBigQuery {
    pub service_account_key: String,
    pub project_id: String,
}

impl TryFrom<catalog::ConnectionOptionsBigQuery> for ConnectionOptionsBigQuery {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ConnectionOptionsBigQuery) -> Result<Self, Self::Error> {
        Ok(ConnectionOptionsBigQuery {
            service_account_key: value.service_account_key,
            project_id: value.project_id,
        })
    }
}

impl From<ConnectionOptionsBigQuery> for catalog::ConnectionOptionsBigQuery {
    fn from(value: ConnectionOptionsBigQuery) -> Self {
        catalog::ConnectionOptionsBigQuery {
            service_account_key: value.service_account_key,
            project_id: value.project_id,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct ConnectionOptionsMysql {
    pub connection_string: String,
    pub ssh_tunnel: Option<String>,
}

impl TryFrom<catalog::ConnectionOptionsMysql> for ConnectionOptionsMysql {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ConnectionOptionsMysql) -> Result<Self, Self::Error> {
        Ok(ConnectionOptionsMysql {
            connection_string: value.connection_string,
            ssh_tunnel: value.ssh_tunnel,
        })
    }
}

impl From<ConnectionOptionsMysql> for catalog::ConnectionOptionsMysql {
    fn from(value: ConnectionOptionsMysql) -> Self {
        catalog::ConnectionOptionsMysql {
            connection_string: value.connection_string,
            ssh_tunnel: value.ssh_tunnel,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct ConnectionOptionsLocal {}

impl TryFrom<catalog::ConnectionOptionsLocal> for ConnectionOptionsLocal {
    type Error = ProtoConvError;
    fn try_from(_value: catalog::ConnectionOptionsLocal) -> Result<Self, Self::Error> {
        Ok(ConnectionOptionsLocal {})
    }
}

impl From<ConnectionOptionsLocal> for catalog::ConnectionOptionsLocal {
    fn from(_value: ConnectionOptionsLocal) -> Self {
        catalog::ConnectionOptionsLocal {}
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct ConnectionOptionsGcs {
    pub service_account_key: String,
}

impl TryFrom<catalog::ConnectionOptionsGcs> for ConnectionOptionsGcs {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ConnectionOptionsGcs) -> Result<Self, Self::Error> {
        Ok(ConnectionOptionsGcs {
            service_account_key: value.service_account_key,
        })
    }
}

impl From<ConnectionOptionsGcs> for catalog::ConnectionOptionsGcs {
    fn from(value: ConnectionOptionsGcs) -> Self {
        catalog::ConnectionOptionsGcs {
            service_account_key: value.service_account_key,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct ConnectionOptionsS3 {
    pub access_key_id: String,
    pub access_key_secret: String,
}

impl TryFrom<catalog::ConnectionOptionsS3> for ConnectionOptionsS3 {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ConnectionOptionsS3) -> Result<Self, Self::Error> {
        Ok(ConnectionOptionsS3 {
            access_key_id: value.access_key_id,
            access_key_secret: value.access_key_secret,
        })
    }
}

impl From<ConnectionOptionsS3> for catalog::ConnectionOptionsS3 {
    fn from(value: ConnectionOptionsS3) -> Self {
        catalog::ConnectionOptionsS3 {
            access_key_id: value.access_key_id,
            access_key_secret: value.access_key_secret,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct ConnectionOptionsSsh {
    pub host: String,
    pub user: String,
    pub port: u16,
    pub keypair: Vec<u8>,
}

impl TryFrom<catalog::ConnectionOptionsSsh> for ConnectionOptionsSsh {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ConnectionOptionsSsh) -> Result<Self, Self::Error> {
        Ok(ConnectionOptionsSsh {
            host: value.host,
            user: value.user,
            port: value.port.try_into()?,
            keypair: value.keypair,
        })
    }
}

impl From<ConnectionOptionsSsh> for catalog::ConnectionOptionsSsh {
    fn from(value: ConnectionOptionsSsh) -> Self {
        catalog::ConnectionOptionsSsh {
            host: value.host,
            user: value.user,
            port: value.port.into(),
            keypair: value.keypair,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::arbitrary::any;
    use proptest::proptest;

    proptest! {
        #[test]
        fn roundtrip_entry_type(expected in any::<EntryType>()) {
            let p: catalog::entry_meta::EntryType = expected.clone().into();
            let got: EntryType = p.try_into().unwrap();
            assert_eq!(expected, got);
        }
    }

    proptest! {
        #[test]
        fn roundtrip_entry_meta(expected in any::<EntryMeta>()) {
            let p: catalog::EntryMeta = expected.clone().into();
            let got: EntryMeta = p.try_into().unwrap();
            assert_eq!(expected, got);
        }
    }

    proptest! {
        #[test]
        fn roundtrip_table_options(expected in any::<TableOptions>()) {
            let p: catalog::TableOptions = expected.clone().into();
            let got: TableOptions = p.try_into().unwrap();
            assert_eq!(expected, got);
        }
    }

    proptest! {
        #[test]
        fn roundtrip_connection_options(expected in any::<ConnectionOptions>()) {
            let p: catalog::ConnectionOptions = expected.clone().into();
            let got: ConnectionOptions = p.try_into().unwrap();
            assert_eq!(expected, got);
        }
    }
}
