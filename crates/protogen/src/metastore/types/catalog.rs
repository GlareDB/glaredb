use super::options::{
    CredentialsOptions, DatabaseOptions, InternalColumnDefinition, TableOptions,
    TableOptionsInternal, TunnelOptions,
};
use crate::gen::common::arrow::ArrowType;
use crate::gen::metastore::catalog::{self, type_signature};
use crate::{FromOptionalField, ProtoConvError};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use proptest_derive::Arbitrary;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogState {
    pub version: u64,
    pub entries: HashMap<u32, CatalogEntry>,
    pub deployment: DeploymentMetadata,
}

impl TryFrom<catalog::CatalogState> for CatalogState {
    type Error = ProtoConvError;
    fn try_from(value: catalog::CatalogState) -> Result<Self, Self::Error> {
        let mut entries = HashMap::with_capacity(value.entries.len());
        for (id, ent) in value.entries {
            entries.insert(id, ent.try_into()?);
        }

        // Try to convert deployment metadata if we have it.
        //
        // If we don't have it (e.g. for catalogs that existed prior to this
        // field being added), use a default.
        let deployment = value
            .deployment
            .map(DeploymentMetadata::try_from)
            .transpose()?
            .unwrap_or_default();

        Ok(CatalogState {
            version: value.version,
            entries,
            deployment,
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
            deployment: Some(value.deployment.try_into()?),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeploymentMetadata {
    pub storage_size: u64,
}

impl TryFrom<catalog::DeploymentMetadata> for DeploymentMetadata {
    type Error = ProtoConvError;
    fn try_from(value: catalog::DeploymentMetadata) -> Result<Self, Self::Error> {
        Ok(Self {
            storage_size: value.storage_size,
        })
    }
}

impl TryFrom<DeploymentMetadata> for catalog::DeploymentMetadata {
    type Error = ProtoConvError;

    fn try_from(value: DeploymentMetadata) -> Result<Self, Self::Error> {
        Ok(Self {
            storage_size: value.storage_size,
        })
    }
}

// TODO: Implement Arbitrary and add test. This would require implementing
// Arbitrary for arrow's DataType.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogEntry {
    Database(DatabaseEntry),
    Schema(SchemaEntry),
    Table(TableEntry),
    View(ViewEntry),
    Tunnel(TunnelEntry),
    Function(FunctionEntry),
    Credentials(CredentialsEntry),
}

impl CatalogEntry {
    pub const fn entry_type(&self) -> EntryType {
        match self {
            CatalogEntry::Database(_) => EntryType::Database,
            CatalogEntry::Schema(_) => EntryType::Schema,
            CatalogEntry::View(_) => EntryType::View,
            CatalogEntry::Table(_) => EntryType::Table,
            CatalogEntry::Tunnel(_) => EntryType::Tunnel,
            CatalogEntry::Function(_) => EntryType::Function,
            CatalogEntry::Credentials(_) => EntryType::Credentials,
        }
    }

    /// Get the entry metadata.
    pub fn get_meta(&self) -> &EntryMeta {
        match self {
            CatalogEntry::Database(db) => &db.meta,
            CatalogEntry::Schema(schema) => &schema.meta,
            CatalogEntry::View(view) => &view.meta,
            CatalogEntry::Table(table) => &table.meta,
            CatalogEntry::Tunnel(tunnel) => &tunnel.meta,
            CatalogEntry::Function(func) => &func.meta,
            CatalogEntry::Credentials(creds) => &creds.meta,
        }
    }

    /// Get a mutable entry metadata referenuce.
    pub fn get_meta_mut(&mut self) -> &mut EntryMeta {
        match self {
            CatalogEntry::Database(db) => &mut db.meta,
            CatalogEntry::Schema(schema) => &mut schema.meta,
            CatalogEntry::View(view) => &mut view.meta,
            CatalogEntry::Table(table) => &mut table.meta,
            CatalogEntry::Tunnel(tunnel) => &mut tunnel.meta,
            CatalogEntry::Function(func) => &mut func.meta,
            CatalogEntry::Credentials(creds) => &mut creds.meta,
        }
    }
}

impl TryFrom<catalog::catalog_entry::Entry> for CatalogEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::catalog_entry::Entry) -> Result<Self, Self::Error> {
        Ok(match value {
            catalog::catalog_entry::Entry::Database(v) => CatalogEntry::Database(v.try_into()?),
            catalog::catalog_entry::Entry::Schema(v) => CatalogEntry::Schema(v.try_into()?),
            catalog::catalog_entry::Entry::Table(v) => CatalogEntry::Table(v.try_into()?),
            catalog::catalog_entry::Entry::View(v) => CatalogEntry::View(v.try_into()?),
            catalog::catalog_entry::Entry::Tunnel(v) => CatalogEntry::Tunnel(v.try_into()?),
            catalog::catalog_entry::Entry::Function(v) => CatalogEntry::Function(v.try_into()?),
            catalog::catalog_entry::Entry::Credentials(v) => {
                CatalogEntry::Credentials(v.try_into()?)
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
            CatalogEntry::Database(v) => catalog::catalog_entry::Entry::Database(v.into()),
            CatalogEntry::Schema(v) => catalog::catalog_entry::Entry::Schema(v.into()),
            CatalogEntry::View(v) => catalog::catalog_entry::Entry::View(v.into()),
            CatalogEntry::Table(v) => catalog::catalog_entry::Entry::Table(v.try_into()?),
            CatalogEntry::Tunnel(v) => catalog::catalog_entry::Entry::Tunnel(v.into()),
            CatalogEntry::Function(v) => catalog::catalog_entry::Entry::Function(v.into()),
            CatalogEntry::Credentials(v) => catalog::catalog_entry::Entry::Credentials(v.into()),
        };
        Ok(catalog::CatalogEntry { entry: Some(ent) })
    }
}

#[derive(Debug, Clone, Copy, Arbitrary, PartialEq, Eq, Hash)]
pub enum EntryType {
    Database,
    Schema,
    Table,
    View,
    Tunnel,
    Function,
    Credentials,
}

impl EntryType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            EntryType::Database => "database",
            EntryType::Schema => "schema",
            EntryType::Table => "table",
            EntryType::View => "view",
            EntryType::Tunnel => "tunnel",
            EntryType::Function => "function",
            EntryType::Credentials => "credentials",
        }
    }
}

impl TryFrom<i32> for EntryType {
    type Error = ProtoConvError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        catalog::entry_meta::EntryType::try_from(value)
            .map_err(|_| ProtoConvError::UnknownEnumVariant("EntryType", value))
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
            catalog::entry_meta::EntryType::Database => EntryType::Database,
            catalog::entry_meta::EntryType::Schema => EntryType::Schema,
            catalog::entry_meta::EntryType::Table => EntryType::Table,
            catalog::entry_meta::EntryType::View => EntryType::View,
            catalog::entry_meta::EntryType::Tunnel => EntryType::Tunnel,
            catalog::entry_meta::EntryType::Function => EntryType::Function,
            catalog::entry_meta::EntryType::Credentials => EntryType::Credentials,
        })
    }
}

impl From<EntryType> for catalog::entry_meta::EntryType {
    fn from(value: EntryType) -> Self {
        match value {
            EntryType::Database => catalog::entry_meta::EntryType::Database,
            EntryType::Schema => catalog::entry_meta::EntryType::Schema,
            EntryType::Table => catalog::entry_meta::EntryType::Table,
            EntryType::View => catalog::entry_meta::EntryType::View,
            EntryType::Tunnel => catalog::entry_meta::EntryType::Tunnel,
            EntryType::Function => catalog::entry_meta::EntryType::Function,
            EntryType::Credentials => catalog::entry_meta::EntryType::Credentials,
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
#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
pub struct EntryMeta {
    pub entry_type: EntryType,
    pub id: u32,
    pub parent: u32,
    pub name: String,
    pub builtin: bool,
    pub external: bool,
    pub is_temp: bool,
    pub sql_example: Option<String>,
    pub description: Option<String>,
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
            external: value.external,
            is_temp: value.is_temp,
            sql_example: value.sql_example,
            description: value.description,
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
            external: value.external,
            is_temp: value.is_temp,
            sql_example: value.sql_example,
            description: value.description,
        })
    }
}

#[derive(Debug, Clone, Copy, Arbitrary, PartialEq, Eq, Hash)]
pub enum SourceAccessMode {
    ReadOnly,
    ReadWrite,
}

impl FromStr for SourceAccessMode {
    type Err = ProtoConvError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_uppercase();
        let access_mode = catalog::SourceAccessMode::from_str_name(&s).ok_or_else(|| {
            ProtoConvError::ParseError(format!("invalid source access mode: {s}"))
        })?;
        Ok(access_mode.into())
    }
}

impl Display for SourceAccessMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl SourceAccessMode {
    pub const fn has_read_access(&self) -> bool {
        matches!(self, Self::ReadOnly | Self::ReadWrite)
    }

    pub const fn has_write_access(&self) -> bool {
        matches!(self, Self::ReadWrite)
    }

    pub fn as_str(&self) -> &'static str {
        // TODO: Maybe lowercase this.
        catalog::SourceAccessMode::from(*self).as_str_name()
    }
}

impl From<catalog::SourceAccessMode> for SourceAccessMode {
    fn from(value: catalog::SourceAccessMode) -> Self {
        match value {
            catalog::SourceAccessMode::ReadOnly => Self::ReadOnly,
            catalog::SourceAccessMode::ReadWrite => Self::ReadWrite,
        }
    }
}

impl TryFrom<i32> for SourceAccessMode {
    type Error = ProtoConvError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Ok(match value {
            x if x == catalog::SourceAccessMode::ReadOnly as i32 => Self::ReadOnly,
            x if x == catalog::SourceAccessMode::ReadWrite as i32 => Self::ReadWrite,
            x => {
                return Err(ProtoConvError::ParseError(format!(
                    "invalid source access mode: {x}"
                )))
            }
        })
    }
}

impl From<SourceAccessMode> for catalog::SourceAccessMode {
    fn from(value: SourceAccessMode) -> Self {
        match value {
            SourceAccessMode::ReadOnly => Self::ReadOnly,
            SourceAccessMode::ReadWrite => Self::ReadWrite,
        }
    }
}

impl From<SourceAccessMode> for i32 {
    fn from(value: SourceAccessMode) -> Self {
        let value: catalog::SourceAccessMode = value.into();
        value as i32
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct DatabaseEntry {
    pub meta: EntryMeta,
    pub options: DatabaseOptions,
    pub tunnel_id: Option<u32>,
    pub access_mode: SourceAccessMode,
}

impl TryFrom<catalog::DatabaseEntry> for DatabaseEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::DatabaseEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(DatabaseEntry {
            meta,
            options: value.options.required("options")?,
            tunnel_id: value.tunnel_id,
            access_mode: value.access_mode.try_into()?,
        })
    }
}

impl From<DatabaseEntry> for catalog::DatabaseEntry {
    fn from(value: DatabaseEntry) -> Self {
        catalog::DatabaseEntry {
            meta: Some(value.meta.into()),
            options: Some(value.options.into()),
            tunnel_id: value.tunnel_id,
            access_mode: value.access_mode.into(),
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableEntry {
    pub meta: EntryMeta,
    pub options: TableOptions,
    pub tunnel_id: Option<u32>,
    pub access_mode: SourceAccessMode,
    pub columns: Option<Vec<InternalColumnDefinition>>,
}

impl TableEntry {
    /// Try to get the columns for this table if available.
    pub fn get_internal_columns(&self) -> Option<Vec<InternalColumnDefinition>> {
        match &self.options {
            TableOptions::Internal(TableOptionsInternal { columns, .. }) => {
                let mut out = Vec::with_capacity(
                    columns.len() + self.columns.as_ref().map_or(0, |v| v.len()),
                );
                out.extend_from_slice(columns.as_slice());
                if self.columns.is_some() {
                    out.extend_from_slice(self.columns.as_ref().unwrap().as_slice());
                }
                Some(out)
            }
            _ => {
                if let Some(columns) = self.columns.as_ref() {
                    let mut out = Vec::with_capacity(columns.len());
                    out.extend_from_slice(columns.as_slice());
                    Some(out)
                } else {
                    None
                }
            }
        }
    }
}

impl TryFrom<catalog::TableEntry> for TableEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        let mut columns = Vec::with_capacity(value.columns.len());
        for col in value.columns {
            columns.push(col.try_into()?);
        }

        Ok(TableEntry {
            meta,
            options: value.options.required("options".to_string())?,
            tunnel_id: value.tunnel_id,
            access_mode: value.access_mode.try_into()?,
            columns: Some(columns),
        })
    }
}

impl TryFrom<TableEntry> for catalog::TableEntry {
    type Error = ProtoConvError;
    fn try_from(value: TableEntry) -> Result<Self, Self::Error> {
        let columns = if let Some(columns) = value.columns {
            let mut out = Vec::with_capacity(columns.len());
            for col in columns {
                out.push(col.try_into()?);
            }
            out
        } else {
            Vec::new()
        };
        Ok(catalog::TableEntry {
            meta: Some(value.meta.into()),
            options: Some(value.options.try_into()?),
            tunnel_id: value.tunnel_id,
            access_mode: value.access_mode.into(),
            columns,
        })
    }
}

impl fmt::Display for TableEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.meta.name, self.options.as_str())
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct ViewEntry {
    pub meta: EntryMeta,
    pub sql: String,
    pub columns: Vec<String>,
}

impl TryFrom<catalog::ViewEntry> for ViewEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ViewEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(ViewEntry {
            meta,
            sql: value.sql,
            columns: value.columns,
        })
    }
}

impl From<ViewEntry> for catalog::ViewEntry {
    fn from(value: ViewEntry) -> Self {
        catalog::ViewEntry {
            meta: Some(value.meta.into()),
            sql: value.sql,
            columns: value.columns,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct TunnelEntry {
    pub meta: EntryMeta,
    pub options: TunnelOptions,
}

impl TryFrom<catalog::TunnelEntry> for TunnelEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TunnelEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(TunnelEntry {
            meta,
            options: value.options.required("options")?,
        })
    }
}

impl From<TunnelEntry> for catalog::TunnelEntry {
    fn from(value: TunnelEntry) -> Self {
        catalog::TunnelEntry {
            meta: Some(value.meta.into()),
            options: Some(value.options.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, Arbitrary, PartialEq, Eq)]
pub enum FunctionType {
    Aggregate,
    Scalar,
    TableReturning,
}

impl FunctionType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            FunctionType::Aggregate => "aggregate",
            FunctionType::Scalar => "scalar",
            FunctionType::TableReturning => "table",
        }
    }
}

impl TryFrom<i32> for FunctionType {
    type Error = ProtoConvError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        catalog::function_entry::FunctionType::try_from(value)
            .map_err(|_| ProtoConvError::UnknownEnumVariant("FunctionType", value))
            .and_then(|t| t.try_into())
    }
}

impl TryFrom<catalog::function_entry::FunctionType> for FunctionType {
    type Error = ProtoConvError;
    fn try_from(value: catalog::function_entry::FunctionType) -> Result<Self, Self::Error> {
        Ok(match value {
            catalog::function_entry::FunctionType::Unknown => {
                return Err(ProtoConvError::ZeroValueEnumVariant("FunctionType"))
            }
            catalog::function_entry::FunctionType::Aggregate => FunctionType::Aggregate,
            catalog::function_entry::FunctionType::Scalar => FunctionType::Scalar,
            catalog::function_entry::FunctionType::TableReturning => FunctionType::TableReturning,
        })
    }
}

impl From<FunctionType> for catalog::function_entry::FunctionType {
    fn from(value: FunctionType) -> Self {
        match value {
            FunctionType::Aggregate => catalog::function_entry::FunctionType::Aggregate,
            FunctionType::Scalar => catalog::function_entry::FunctionType::Scalar,
            FunctionType::TableReturning => catalog::function_entry::FunctionType::TableReturning,
        }
    }
}

/// The runtime preference for a function.
#[derive(Debug, Clone, Copy, Arbitrary, PartialEq, Eq, Hash)]
pub enum RuntimePreference {
    Unspecified,
    Local,
    Remote,
}

impl RuntimePreference {
    pub const fn as_str(&self) -> &'static str {
        match self {
            RuntimePreference::Unspecified => "unspecified",
            RuntimePreference::Local => "local",
            RuntimePreference::Remote => "remote",
        }
    }
}

impl TryFrom<i32> for RuntimePreference {
    type Error = ProtoConvError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let pref = catalog::function_entry::RuntimePreference::try_from(value)
            .map_err(|_| ProtoConvError::UnknownEnumVariant("RuntimePreference", value))?;
        Ok(pref.into())
    }
}

impl From<catalog::function_entry::RuntimePreference> for RuntimePreference {
    fn from(value: catalog::function_entry::RuntimePreference) -> Self {
        match value {
            catalog::function_entry::RuntimePreference::Unspecified => {
                RuntimePreference::Unspecified
            }
            catalog::function_entry::RuntimePreference::Local => RuntimePreference::Local,
            catalog::function_entry::RuntimePreference::Remote => RuntimePreference::Remote,
        }
    }
}

impl From<RuntimePreference> for catalog::function_entry::RuntimePreference {
    fn from(value: RuntimePreference) -> Self {
        match value {
            RuntimePreference::Unspecified => {
                catalog::function_entry::RuntimePreference::Unspecified
            }
            RuntimePreference::Local => catalog::function_entry::RuntimePreference::Local,
            RuntimePreference::Remote => catalog::function_entry::RuntimePreference::Remote,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionEntry {
    pub meta: EntryMeta,
    pub func_type: FunctionType,
    pub signature: Option<Signature>,
}

impl TryFrom<catalog::FunctionEntry> for FunctionEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::FunctionEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(FunctionEntry {
            meta,
            func_type: value.func_type.try_into()?,
            signature: value.signature.map(|s| s.try_into()).transpose()?,
        })
    }
}

impl From<Volatility> for catalog::Volatility {
    fn from(value: Volatility) -> Self {
        match value {
            datafusion::logical_expr::Volatility::Immutable => catalog::Volatility::Immutable,
            datafusion::logical_expr::Volatility::Stable => catalog::Volatility::Stable,
            datafusion::logical_expr::Volatility::Volatile => catalog::Volatility::Volatile,
        }
    }
}

impl TryFrom<catalog::Volatility> for Volatility {
    type Error = ProtoConvError;

    fn try_from(value: catalog::Volatility) -> Result<Self, Self::Error> {
        match value {
            catalog::Volatility::Immutable => Ok(Volatility::Immutable),
            catalog::Volatility::Stable => Ok(Volatility::Stable),
            catalog::Volatility::Volatile => Ok(Volatility::Volatile),
        }
    }
}

impl From<TypeSignature> for catalog::TypeSignature {
    fn from(value: TypeSignature) -> Self {
        use catalog::type_signature::Signature as ProtoSignature;
        let inner = match value {
            TypeSignature::Variadic(args) => {
                let args: Vec<ArrowType> = args.iter().map(|t| t.try_into().unwrap()).collect();
                let var_sig = catalog::VariadicSignature { args };

                ProtoSignature::Variadic(var_sig)
            }
            TypeSignature::VariadicEqual => {
                ProtoSignature::VariadicEqual(catalog::VariadicEqualSignature {})
            }
            TypeSignature::VariadicAny => {
                ProtoSignature::VariadicAny(catalog::VariadicAnySignature {})
            }
            TypeSignature::Uniform(n, args) => {
                let args: Vec<ArrowType> = args.iter().map(|t| t.try_into().unwrap()).collect();
                let uniform_sig = catalog::UniformSignature {
                    num_args: n as u32,
                    args,
                };

                ProtoSignature::Uniform(uniform_sig)
            }
            TypeSignature::Exact(args) => {
                let args: Vec<ArrowType> = args.iter().map(|t| t.try_into().unwrap()).collect();
                let exact_sig = catalog::ExactSignature { args };

                ProtoSignature::Exact(exact_sig)
            }
            TypeSignature::Any(n) => {
                ProtoSignature::Any(catalog::AnySignature { num_args: n as u32 })
            }
            TypeSignature::OneOf(sigs) => {
                let sigs: Vec<catalog::TypeSignature> =
                    sigs.into_iter().map(|s| s.into()).collect();
                ProtoSignature::OneOf(catalog::OneOfSignature { args: sigs })
            }
        };

        catalog::TypeSignature {
            signature: Some(inner),
        }
    }
}

impl TryFrom<catalog::TypeSignature> for TypeSignature {
    type Error = ProtoConvError;

    fn try_from(value: catalog::TypeSignature) -> Result<Self, Self::Error> {
        match value.signature.unwrap() {
            type_signature::Signature::Variadic(args) => {
                let args: Vec<DataType> = args
                    .args
                    .iter()
                    .map(|t| t.try_into())
                    .collect::<Result<_, _>>()?;

                Ok(TypeSignature::Variadic(args))
            }
            type_signature::Signature::VariadicEqual(_) => Ok(TypeSignature::VariadicEqual),
            type_signature::Signature::VariadicAny(_) => Ok(TypeSignature::VariadicAny),
            type_signature::Signature::Uniform(catalog::UniformSignature { num_args, args }) => {
                let args: Vec<DataType> = args
                    .iter()
                    .map(|t| t.try_into())
                    .collect::<Result<_, _>>()?;

                Ok(TypeSignature::Uniform(num_args as usize, args))
            }
            type_signature::Signature::Exact(catalog::ExactSignature { args }) => {
                let args: Vec<DataType> = args
                    .iter()
                    .map(|t| t.try_into())
                    .collect::<Result<_, _>>()?;

                Ok(TypeSignature::Exact(args))
            }
            type_signature::Signature::Any(catalog::AnySignature { num_args: n }) => {
                Ok(TypeSignature::Any(n as usize))
            }
            type_signature::Signature::OneOf(catalog::OneOfSignature { args }) => {
                let args: Vec<TypeSignature> = args
                    .into_iter()
                    .map(|t| t.try_into())
                    .collect::<Result<_, _>>()?;

                Ok(TypeSignature::OneOf(args))
            }
        }
    }
}
impl From<Signature> for catalog::Signature {
    fn from(value: Signature) -> Self {
        let volatility: catalog::Volatility = value.volatility.into();

        catalog::Signature {
            type_signature: Some(value.type_signature.into()),
            volatility: volatility as i32,
        }
    }
}

impl TryFrom<catalog::Signature> for Signature {
    type Error = ProtoConvError;
    fn try_from(value: catalog::Signature) -> Result<Self, Self::Error> {
        let volatility = catalog::Volatility::try_from(value.volatility)?;
        let volatility: Volatility = volatility.try_into()?;
        let type_signature: TypeSignature = value.type_signature.required("type_signature")?;
        Ok(Signature {
            type_signature,
            volatility,
        })
    }
}

impl From<FunctionEntry> for catalog::FunctionEntry {
    fn from(value: FunctionEntry) -> Self {
        let func_type: catalog::function_entry::FunctionType = value.func_type.into();
        catalog::FunctionEntry {
            meta: Some(value.meta.into()),
            func_type: func_type as i32,
            signature: value.signature.map(|s| s.into()),
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct CredentialsEntry {
    pub meta: EntryMeta,
    pub options: CredentialsOptions,
    pub comment: String,
}

impl TryFrom<catalog::CredentialsEntry> for CredentialsEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::CredentialsEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(CredentialsEntry {
            meta,
            options: value.options.required("options")?,
            comment: value.comment,
        })
    }
}

impl From<CredentialsEntry> for catalog::CredentialsEntry {
    fn from(value: CredentialsEntry) -> Self {
        catalog::CredentialsEntry {
            meta: Some(value.meta.into()),
            options: Some(value.options.into()),
            comment: value.comment,
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
            let p: catalog::entry_meta::EntryType = expected.into();
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

    #[test]
    fn convert_catalog_state_no_deployment_metadata() {
        // New `deployment` field added. Assert we can handle catalogs that
        // don't have this field.

        let state = catalog::CatalogState {
            version: 4,
            entries: HashMap::new(),
            deployment: None,
        };

        let converted: CatalogState = state.try_into().unwrap();
        let expected = CatalogState {
            version: 4,
            entries: HashMap::new(),
            deployment: DeploymentMetadata { storage_size: 0 },
        };

        assert_eq!(expected, converted);
    }

    #[test]
    fn source_access_mode_as_str() {
        let mode = SourceAccessMode::ReadOnly;
        assert_eq!("READ_ONLY", mode.as_str());
        let mode = SourceAccessMode::ReadWrite;
        assert_eq!("READ_WRITE", mode.as_str());
    }

    #[test]
    fn source_access_mode_from_str() {
        let mode = SourceAccessMode::from_str("READ_ONLY").unwrap();
        assert_eq!(SourceAccessMode::ReadOnly, mode);
        let mode = SourceAccessMode::from_str("READ_WRITE").unwrap();
        assert_eq!(SourceAccessMode::ReadWrite, mode);

        let _ = SourceAccessMode::from_str("DELETE").unwrap_err();
    }
}
