use super::options::TunnelOptions;
use super::{FromOptionalField, ProtoConvError};
use crate::proto::service;
use crate::types::options::{DatabaseOptions, TableOptions};
use proptest_derive::Arbitrary;

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub enum Mutation {
    DropDatabase(DropDatabase),
    DropSchema(DropSchema),
    DropObject(DropObject),
    CreateSchema(CreateSchema),
    CreateView(CreateView),
    CreateExternalTable(CreateExternalTable),
    CreateExternalDatabase(CreateExternalDatabase),
    AlterTableRename(AlterTableRename),
    AlterDatabaseRename(AlterDatabaseRename),
    CreateTunnel(CreateTunnel),
    DropTunnel(DropTunnel),
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
            service::mutation::Mutation::CreateExternalTable(v) => {
                Mutation::CreateExternalTable(v.try_into()?)
            }
            service::mutation::Mutation::CreateExternalDatabase(v) => {
                Mutation::CreateExternalDatabase(v.try_into()?)
            }
            service::mutation::Mutation::AlterTableRename(v) => {
                Mutation::AlterTableRename(v.try_into()?)
            }
            service::mutation::Mutation::AlterDatabaseRename(v) => {
                Mutation::AlterDatabaseRename(v.try_into()?)
            }
            service::mutation::Mutation::CreateTunnel(v) => Mutation::CreateTunnel(v.try_into()?),
            service::mutation::Mutation::DropTunnel(v) => Mutation::DropTunnel(v.try_into()?),
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
            Mutation::CreateExternalTable(v) => {
                service::mutation::Mutation::CreateExternalTable(v.try_into()?)
            }
            Mutation::CreateExternalDatabase(v) => {
                service::mutation::Mutation::CreateExternalDatabase(v.into())
            }
            Mutation::AlterTableRename(v) => {
                service::mutation::Mutation::AlterTableRename(v.into())
            }
            Mutation::AlterDatabaseRename(v) => {
                service::mutation::Mutation::AlterDatabaseRename(v.into())
            }
            Mutation::CreateTunnel(v) => service::mutation::Mutation::CreateTunnel(v.into()),
            Mutation::DropTunnel(v) => service::mutation::Mutation::DropTunnel(v.into()),
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct CreateSchema {
    pub name: String,
}

impl TryFrom<service::CreateSchema> for CreateSchema {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateSchema) -> Result<Self, Self::Error> {
        // TODO: Check if string are zero value.
        Ok(CreateSchema { name: value.name })
    }
}

impl From<CreateSchema> for service::CreateSchema {
    fn from(value: CreateSchema) -> Self {
        service::CreateSchema { name: value.name }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct CreateExternalTable {
    pub schema: String,
    pub name: String,
    pub options: TableOptions,
    pub if_not_exists: bool,
    pub tunnel: Option<String>,
}

impl TryFrom<service::CreateExternalTable> for CreateExternalTable {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateExternalTable) -> Result<Self, Self::Error> {
        // TODO: Check if string are zero value.
        Ok(CreateExternalTable {
            schema: value.schema,
            name: value.name,
            options: value.options.required("options")?,
            if_not_exists: value.if_not_exists,
            tunnel: value.tunnel,
        })
    }
}

impl TryFrom<CreateExternalTable> for service::CreateExternalTable {
    type Error = ProtoConvError;
    fn try_from(value: CreateExternalTable) -> Result<Self, Self::Error> {
        Ok(service::CreateExternalTable {
            schema: value.schema,
            name: value.name,
            options: Some(value.options.try_into()?),
            if_not_exists: value.if_not_exists,
            tunnel: value.tunnel,
        })
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct AlterTableRename {
    pub schema: String,
    pub name: String,
    pub new_name: String,
}

impl TryFrom<service::AlterTableRename> for AlterTableRename {
    type Error = ProtoConvError;
    fn try_from(value: service::AlterTableRename) -> Result<Self, Self::Error> {
        Ok(AlterTableRename {
            schema: value.schema,
            name: value.name,
            new_name: value.new_name,
        })
    }
}

impl From<AlterTableRename> for service::AlterTableRename {
    fn from(value: AlterTableRename) -> Self {
        service::AlterTableRename {
            schema: value.schema,
            name: value.name,
            new_name: value.new_name,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct AlterDatabaseRename {
    pub name: String,
    pub new_name: String,
}

impl TryFrom<service::AlterDatabaseRename> for AlterDatabaseRename {
    type Error = ProtoConvError;
    fn try_from(value: service::AlterDatabaseRename) -> Result<Self, Self::Error> {
        Ok(AlterDatabaseRename {
            name: value.name,
            new_name: value.new_name,
        })
    }
}

impl From<AlterDatabaseRename> for service::AlterDatabaseRename {
    fn from(value: AlterDatabaseRename) -> Self {
        service::AlterDatabaseRename {
            name: value.name,
            new_name: value.new_name,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::arbitrary::any;
    use proptest::proptest;

    proptest! {
        #[test]
        fn roundtrip_mutation(expected in any::<Mutation>()) {
            let p: service::mutation::Mutation = expected.clone().try_into().unwrap();
            let got: Mutation = p.try_into().unwrap();
            assert_eq!(expected, got)
        }
    }
}
