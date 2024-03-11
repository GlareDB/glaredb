use datafusion::logical_expr::Signature;

use super::catalog::{FunctionType, SourceAccessMode};
use super::options::{
    CredentialsOptions,
    DatabaseOptions,
    TableOptions,
    TableOptionsInternal,
    TunnelOptions,
};
use crate::gen::metastore::service;
use crate::{FromOptionalField, ProtoConvError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    DropDatabase(DropDatabase),
    DropSchema(DropSchema),
    DropObject(DropObject),
    CreateSchema(CreateSchema),
    CreateView(CreateView),
    CreateTable(CreateTable),
    CreateExternalTable(CreateExternalTable),
    CreateExternalDatabase(CreateExternalDatabase),
    AlterTable(AlterTable),
    AlterDatabase(AlterDatabase),
    CreateTunnel(CreateTunnel),
    DropTunnel(DropTunnel),
    AlterTunnelRotateKeys(AlterTunnelRotateKeys),
    CreateCredentials(CreateCredentials),
    DropCredentials(DropCredentials),
    // Deployment metadata updates
    UpdateDeploymentStorage(UpdateDeploymentStorage),
    CreateFunction(CreateFunction),
}

impl TryFrom<service::Mutation> for Mutation {
    type Error = ProtoConvError;
    fn try_from(value: service::Mutation) -> Result<Self, Self::Error> {
        value.mutation.required("mutation")
    }
}

impl TryFrom<service::mutation::Mutation> for Mutation {
    type Error = ProtoConvError;
    fn try_from(value: service::mutation::Mutation) -> Result<Self, Self::Error> {
        Ok(match value {
            service::mutation::Mutation::DropDatabase(v) => Mutation::DropDatabase(v.try_into()?),
            service::mutation::Mutation::DropSchema(v) => Mutation::DropSchema(v.try_into()?),
            service::mutation::Mutation::DropObject(v) => Mutation::DropObject(v.try_into()?),
            service::mutation::Mutation::CreateSchema(v) => Mutation::CreateSchema(v.try_into()?),
            service::mutation::Mutation::CreateView(v) => Mutation::CreateView(v.try_into()?),
            service::mutation::Mutation::CreateTable(v) => Mutation::CreateTable(v.try_into()?),
            service::mutation::Mutation::CreateExternalTable(v) => {
                Mutation::CreateExternalTable(v.try_into()?)
            }
            service::mutation::Mutation::CreateExternalDatabase(v) => {
                Mutation::CreateExternalDatabase(v.try_into()?)
            }
            service::mutation::Mutation::AlterTable(v) => Mutation::AlterTable(v.try_into()?),
            service::mutation::Mutation::AlterDatabase(v) => Mutation::AlterDatabase(v.try_into()?),
            service::mutation::Mutation::CreateTunnel(v) => Mutation::CreateTunnel(v.try_into()?),
            service::mutation::Mutation::DropTunnel(v) => Mutation::DropTunnel(v.try_into()?),
            service::mutation::Mutation::AlterTunnelRotateKeys(v) => {
                Mutation::AlterTunnelRotateKeys(v.try_into()?)
            }
            service::mutation::Mutation::CreateCredentials(v) => {
                Mutation::CreateCredentials(v.try_into()?)
            }
            service::mutation::Mutation::DropCredentials(v) => {
                Mutation::DropCredentials(v.try_into()?)
            }
            service::mutation::Mutation::UpdateDeploymentStorage(v) => {
                Mutation::UpdateDeploymentStorage(v.try_into()?)
            }
            service::mutation::Mutation::CreateFunction(v) => {
                Mutation::CreateFunction(v.try_into()?)
            }
        })
    }
}
impl TryFrom<Mutation> for service::mutation::Mutation {
    type Error = ProtoConvError;
    fn try_from(value: Mutation) -> Result<Self, Self::Error> {
        Ok(match value {
            Mutation::DropDatabase(v) => service::mutation::Mutation::DropDatabase(v.into()),
            Mutation::DropSchema(v) => service::mutation::Mutation::DropSchema(v.into()),
            Mutation::DropObject(v) => service::mutation::Mutation::DropObject(v.into()),
            Mutation::CreateSchema(v) => service::mutation::Mutation::CreateSchema(v.into()),
            Mutation::CreateView(v) => service::mutation::Mutation::CreateView(v.into()),
            Mutation::CreateTable(v) => service::mutation::Mutation::CreateTable(v.try_into()?),
            Mutation::CreateExternalTable(v) => {
                service::mutation::Mutation::CreateExternalTable(v.try_into()?)
            }
            Mutation::CreateExternalDatabase(v) => {
                service::mutation::Mutation::CreateExternalDatabase(v.into())
            }
            Mutation::AlterTable(v) => service::mutation::Mutation::AlterTable(v.into()),
            Mutation::AlterDatabase(v) => service::mutation::Mutation::AlterDatabase(v.into()),
            Mutation::CreateTunnel(v) => service::mutation::Mutation::CreateTunnel(v.into()),
            Mutation::DropTunnel(v) => service::mutation::Mutation::DropTunnel(v.into()),
            Mutation::AlterTunnelRotateKeys(v) => {
                service::mutation::Mutation::AlterTunnelRotateKeys(v.into())
            }
            Mutation::CreateCredentials(v) => {
                service::mutation::Mutation::CreateCredentials(v.into())
            }
            Mutation::DropCredentials(v) => service::mutation::Mutation::DropCredentials(v.into()),
            Mutation::UpdateDeploymentStorage(v) => {
                service::mutation::Mutation::UpdateDeploymentStorage(v.into())
            }
            Mutation::CreateFunction(v) => {
                service::mutation::Mutation::CreateFunction(v.try_into()?)
            }
        })
    }
}

impl TryFrom<Mutation> for service::Mutation {
    type Error = ProtoConvError;
    fn try_from(value: Mutation) -> Result<Self, Self::Error> {
        Ok(service::Mutation {
            mutation: Some(value.try_into()?),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropDatabase {
    pub name: String,
    pub if_exists: bool,
}

impl TryFrom<service::DropDatabase> for DropDatabase {
    type Error = ProtoConvError;
    fn try_from(value: service::DropDatabase) -> Result<Self, Self::Error> {
        // TODO: Check if string is zero value.
        Ok(DropDatabase {
            name: value.name,
            if_exists: value.if_exists,
        })
    }
}

impl From<DropDatabase> for service::DropDatabase {
    fn from(value: DropDatabase) -> Self {
        service::DropDatabase {
            name: value.name,
            if_exists: value.if_exists,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropSchema {
    pub name: String,
    pub if_exists: bool,
    pub cascade: bool,
}

impl TryFrom<service::DropSchema> for DropSchema {
    type Error = ProtoConvError;
    fn try_from(value: service::DropSchema) -> Result<Self, Self::Error> {
        // TODO: Check if string is zero value.
        Ok(DropSchema {
            name: value.name,
            if_exists: value.if_exists,
            cascade: value.cascade,
        })
    }
}

impl From<DropSchema> for service::DropSchema {
    fn from(value: DropSchema) -> Self {
        service::DropSchema {
            name: value.name,
            if_exists: value.if_exists,
            cascade: value.cascade,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropObject {
    pub schema: String,
    pub name: String,
    pub if_exists: bool,
}

impl TryFrom<service::DropObject> for DropObject {
    type Error = ProtoConvError;
    fn try_from(value: service::DropObject) -> Result<Self, Self::Error> {
        // TODO: Check if strings are zero value.
        Ok(DropObject {
            schema: value.schema,
            name: value.name,
            if_exists: value.if_exists,
        })
    }
}

impl From<DropObject> for service::DropObject {
    fn from(value: DropObject) -> Self {
        service::DropObject {
            schema: value.schema,
            name: value.name,
            if_exists: value.if_exists,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSchema {
    pub name: String,
    pub if_not_exists: bool,
}

impl TryFrom<service::CreateSchema> for CreateSchema {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateSchema) -> Result<Self, Self::Error> {
        // TODO: Check if string are zero value.
        Ok(CreateSchema {
            name: value.name,
            if_not_exists: value.if_not_exists,
        })
    }
}

impl From<CreateSchema> for service::CreateSchema {
    fn from(value: CreateSchema) -> Self {
        service::CreateSchema {
            name: value.name,
            if_not_exists: value.if_not_exists,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateView {
    pub schema: String,
    pub name: String,
    pub sql: String,
    pub or_replace: bool,
    pub columns: Vec<String>,
}

impl TryFrom<service::CreateView> for CreateView {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateView) -> Result<Self, Self::Error> {
        // TODO: Check if string are zero value.
        Ok(CreateView {
            schema: value.schema,
            name: value.name,
            sql: value.sql,
            or_replace: value.or_replace,
            columns: value.columns,
        })
    }
}

impl From<CreateView> for service::CreateView {
    fn from(value: CreateView) -> Self {
        service::CreateView {
            schema: value.schema,
            name: value.name,
            sql: value.sql,
            or_replace: value.or_replace,
            columns: value.columns,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    pub schema: String,
    pub name: String,
    pub options: TableOptionsInternal,
    pub if_not_exists: bool,
    pub or_replace: bool,
}

impl TryFrom<service::CreateTable> for CreateTable {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateTable) -> Result<Self, Self::Error> {
        let options: TableOptionsInternal = value.options.required("options")?;
        Ok(CreateTable {
            schema: value.schema,
            name: value.name,
            options,
            if_not_exists: value.if_not_exists,
            or_replace: value.or_replace,
        })
    }
}

impl TryFrom<CreateTable> for service::CreateTable {
    type Error = ProtoConvError;
    fn try_from(value: CreateTable) -> Result<service::CreateTable, Self::Error> {
        Ok(service::CreateTable {
            schema: value.schema,
            name: value.name,
            options: Some(value.options.try_into()?),
            if_not_exists: value.if_not_exists,
            or_replace: value.or_replace,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateFunction {
    pub name: String,
    pub aliases: Vec<String>,
    pub signature: Signature,
    pub function_type: FunctionType,
}

impl TryFrom<service::CreateFunction> for CreateFunction {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateFunction) -> Result<Self, Self::Error> {
        let signature = value.signature.required("signature")?;
        let function_type = value.r#type;
        let function_type = FunctionType::try_from(function_type)?;

        Ok(CreateFunction {
            name: value.name,
            aliases: value.aliases,
            signature,
            function_type,
        })
    }
}

impl TryFrom<CreateFunction> for service::CreateFunction {
    type Error = ProtoConvError;
    fn try_from(value: CreateFunction) -> Result<service::CreateFunction, Self::Error> {
        Ok(service::CreateFunction {
            name: value.name,
            aliases: value.aliases,
            signature: Some(value.signature.into()),
            r#type: value.function_type as i32,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateExternalTable {
    pub schema: String,
    pub name: String,
    pub options: TableOptions,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub tunnel: Option<String>,
    pub credentials: Option<String>,
}


impl TryFrom<service::CreateExternalTable> for CreateExternalTable {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateExternalTable) -> Result<Self, Self::Error> {
        // TODO: Check if string are zero value.
        Ok(CreateExternalTable {
            schema: value.schema,
            name: value.name,
            options: value.options.required("options")?,
            or_replace: value.or_replace,
            if_not_exists: value.if_not_exists,
            tunnel: value.tunnel,
            credentials: value.credentials,
        })
    }
}

impl TryFrom<CreateExternalTable> for service::CreateExternalTable {
    type Error = ProtoConvError;
    fn try_from(value: CreateExternalTable) -> Result<Self, Self::Error> {
        Ok(service::CreateExternalTable {
            schema: value.schema,
            name: value.name,
            options: Some(value.options.into()),
            or_replace: value.or_replace,
            if_not_exists: value.if_not_exists,
            tunnel: value.tunnel,
            credentials: value.credentials,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateExternalDatabase {
    pub name: String,
    pub options: DatabaseOptions,
    pub if_not_exists: bool,
    pub tunnel: Option<String>,
}

impl TryFrom<service::CreateExternalDatabase> for CreateExternalDatabase {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateExternalDatabase) -> Result<Self, Self::Error> {
        Ok(CreateExternalDatabase {
            name: value.name,
            options: value.options.required("options")?,
            if_not_exists: value.if_not_exists,
            tunnel: value.tunnel,
        })
    }
}

impl From<CreateExternalDatabase> for service::CreateExternalDatabase {
    fn from(value: CreateExternalDatabase) -> Self {
        service::CreateExternalDatabase {
            name: value.name,
            options: Some(value.options.into()),
            if_not_exists: value.if_not_exists,
            tunnel: value.tunnel,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterTableOperation {
    RenameTable { new_name: String },
    SetAccessMode { access_mode: SourceAccessMode },
}

impl TryFrom<service::alter_table_operation::Operation> for AlterTableOperation {
    type Error = ProtoConvError;
    fn try_from(value: service::alter_table_operation::Operation) -> Result<Self, Self::Error> {
        Ok(match value {
            service::alter_table_operation::Operation::AlterTableOperationRename(
                service::AlterTableOperationRename { new_name },
            ) => Self::RenameTable { new_name },
            service::alter_table_operation::Operation::AlterTableOperationSetAccessMode(
                service::AlterTableOperationSetAccessMode { access_mode },
            ) => Self::SetAccessMode {
                access_mode: access_mode.try_into()?,
            },
        })
    }
}

impl From<AlterTableOperation> for service::alter_table_operation::Operation {
    fn from(value: AlterTableOperation) -> Self {
        match value {
            AlterTableOperation::RenameTable { new_name } => {
                service::alter_table_operation::Operation::AlterTableOperationRename(
                    service::AlterTableOperationRename { new_name },
                )
            }
            AlterTableOperation::SetAccessMode { access_mode } => {
                service::alter_table_operation::Operation::AlterTableOperationSetAccessMode(
                    service::AlterTableOperationSetAccessMode {
                        access_mode: access_mode.into(),
                    },
                )
            }
        }
    }
}

impl TryFrom<service::AlterTableOperation> for AlterTableOperation {
    type Error = ProtoConvError;
    fn try_from(value: service::AlterTableOperation) -> Result<Self, Self::Error> {
        value.operation.required("alter table operation")
    }
}

impl From<AlterTableOperation> for service::AlterTableOperation {
    fn from(value: AlterTableOperation) -> Self {
        Self {
            operation: Some(value.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTable {
    pub schema: String,
    pub name: String,
    pub operation: AlterTableOperation,
}

impl TryFrom<service::AlterTable> for AlterTable {
    type Error = ProtoConvError;
    fn try_from(value: service::AlterTable) -> Result<Self, Self::Error> {
        Ok(AlterTable {
            schema: value.schema,
            name: value.name,
            operation: value.operation.required("alter table operation")?,
        })
    }
}

impl From<AlterTable> for service::AlterTable {
    fn from(value: AlterTable) -> Self {
        service::AlterTable {
            schema: value.schema,
            name: value.name,
            operation: Some(value.operation.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterDatabaseOperation {
    RenameDatabase { new_name: String },
    SetAccessMode { access_mode: SourceAccessMode },
}

impl TryFrom<service::alter_database_operation::Operation> for AlterDatabaseOperation {
    type Error = ProtoConvError;
    fn try_from(value: service::alter_database_operation::Operation) -> Result<Self, Self::Error> {
        Ok(match value {
            service::alter_database_operation::Operation::AlterDatabaseOperationRename(
                service::AlterDatabaseOperationRename { new_name },
            ) => Self::RenameDatabase { new_name },
            service::alter_database_operation::Operation::AlterDatabaseOperationSetAccessMode(
                service::AlterDatabaseOperationSetAccessMode { access_mode },
            ) => Self::SetAccessMode {
                access_mode: access_mode.try_into()?,
            },
        })
    }
}

impl From<AlterDatabaseOperation> for service::alter_database_operation::Operation {
    fn from(value: AlterDatabaseOperation) -> Self {
        match value {
            AlterDatabaseOperation::RenameDatabase { new_name } => {
                service::alter_database_operation::Operation::AlterDatabaseOperationRename(
                    service::AlterDatabaseOperationRename { new_name },
                )
            }
            AlterDatabaseOperation::SetAccessMode { access_mode } => {
                service::alter_database_operation::Operation::AlterDatabaseOperationSetAccessMode(
                    service::AlterDatabaseOperationSetAccessMode {
                        access_mode: access_mode.into(),
                    },
                )
            }
        }
    }
}

impl TryFrom<service::AlterDatabaseOperation> for AlterDatabaseOperation {
    type Error = ProtoConvError;
    fn try_from(value: service::AlterDatabaseOperation) -> Result<Self, Self::Error> {
        value.operation.required("alter database operation")
    }
}

impl From<AlterDatabaseOperation> for service::AlterDatabaseOperation {
    fn from(value: AlterDatabaseOperation) -> Self {
        Self {
            operation: Some(value.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterDatabase {
    pub name: String,
    pub operation: AlterDatabaseOperation,
}

impl TryFrom<service::AlterDatabase> for AlterDatabase {
    type Error = ProtoConvError;
    fn try_from(value: service::AlterDatabase) -> Result<Self, Self::Error> {
        Ok(AlterDatabase {
            name: value.name,
            operation: value.operation.required("alter database operation")?,
        })
    }
}

impl From<AlterDatabase> for service::AlterDatabase {
    fn from(value: AlterDatabase) -> Self {
        service::AlterDatabase {
            name: value.name,
            operation: Some(value.operation.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTunnel {
    pub name: String,
    pub options: TunnelOptions,
    pub if_not_exists: bool,
}

impl TryFrom<service::CreateTunnel> for CreateTunnel {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateTunnel) -> Result<Self, Self::Error> {
        Ok(CreateTunnel {
            name: value.name,
            options: value.options.required("options")?,
            if_not_exists: value.if_not_exists,
        })
    }
}

impl From<CreateTunnel> for service::CreateTunnel {
    fn from(value: CreateTunnel) -> Self {
        service::CreateTunnel {
            name: value.name,
            options: Some(value.options.into()),
            if_not_exists: value.if_not_exists,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTunnel {
    pub name: String,
    pub if_exists: bool,
}

impl TryFrom<service::DropTunnel> for DropTunnel {
    type Error = ProtoConvError;
    fn try_from(value: service::DropTunnel) -> Result<Self, Self::Error> {
        Ok(DropTunnel {
            name: value.name,
            if_exists: value.if_exists,
        })
    }
}

impl From<DropTunnel> for service::DropTunnel {
    fn from(value: DropTunnel) -> Self {
        service::DropTunnel {
            name: value.name,
            if_exists: value.if_exists,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTunnelRotateKeys {
    pub name: String,
    pub if_exists: bool,
    pub new_ssh_key: Vec<u8>,
}

impl TryFrom<service::AlterTunnelRotateKeys> for AlterTunnelRotateKeys {
    type Error = ProtoConvError;
    fn try_from(value: service::AlterTunnelRotateKeys) -> Result<Self, Self::Error> {
        Ok(AlterTunnelRotateKeys {
            name: value.name,
            if_exists: value.if_exists,
            new_ssh_key: value.new_ssh_key,
        })
    }
}

impl From<AlterTunnelRotateKeys> for service::AlterTunnelRotateKeys {
    fn from(value: AlterTunnelRotateKeys) -> Self {
        service::AlterTunnelRotateKeys {
            name: value.name,
            if_exists: value.if_exists,
            new_ssh_key: value.new_ssh_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCredentials {
    pub name: String,
    pub options: CredentialsOptions,
    pub comment: String,
    pub or_replace: bool,
}

impl TryFrom<service::CreateCredentials> for CreateCredentials {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateCredentials) -> Result<Self, Self::Error> {
        Ok(CreateCredentials {
            name: value.name,
            options: value.options.required("options")?,
            comment: value.comment,
            or_replace: value.or_replace,
        })
    }
}

impl From<CreateCredentials> for service::CreateCredentials {
    fn from(value: CreateCredentials) -> Self {
        service::CreateCredentials {
            name: value.name,
            options: Some(value.options.into()),
            comment: value.comment,
            or_replace: value.or_replace,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropCredentials {
    pub name: String,
    pub if_exists: bool,
}

impl TryFrom<service::DropCredentials> for DropCredentials {
    type Error = ProtoConvError;
    fn try_from(value: service::DropCredentials) -> Result<Self, Self::Error> {
        Ok(DropCredentials {
            name: value.name,
            if_exists: value.if_exists,
        })
    }
}

impl From<DropCredentials> for service::DropCredentials {
    fn from(value: DropCredentials) -> Self {
        service::DropCredentials {
            name: value.name,
            if_exists: value.if_exists,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateDeploymentStorage {
    pub new_storage_size: u64,
}

impl TryFrom<service::UpdateDeploymentStorage> for UpdateDeploymentStorage {
    type Error = ProtoConvError;
    fn try_from(value: service::UpdateDeploymentStorage) -> Result<Self, Self::Error> {
        Ok(Self {
            new_storage_size: value.new_storage_size,
        })
    }
}

impl From<UpdateDeploymentStorage> for service::UpdateDeploymentStorage {
    fn from(value: UpdateDeploymentStorage) -> Self {
        Self {
            new_storage_size: value.new_storage_size,
        }
    }
}
