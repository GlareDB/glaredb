use super::{FromOptionalField, ProtoConvError};
use crate::proto::service;
use crate::types::catalog::{ColumnDefinition, ConnectionOptions, TableOptions};
use proptest_derive::Arbitrary;

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub enum Mutation {
    DropSchema(DropSchema),
    DropObject(DropObject),
    CreateSchema(CreateSchema),
    CreateView(CreateView),
    CreateConnection(CreateConnection),
    CreateExternalTable(CreateExternalTable),
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
            service::mutation::Mutation::DropSchema(v) => Mutation::DropSchema(v.try_into()?),
            service::mutation::Mutation::DropObject(v) => Mutation::DropObject(v.try_into()?),
            service::mutation::Mutation::CreateSchema(v) => Mutation::CreateSchema(v.try_into()?),
            service::mutation::Mutation::CreateView(v) => Mutation::CreateView(v.try_into()?),
            service::mutation::Mutation::CreateConnection(v) => {
                Mutation::CreateConnection(v.try_into()?)
            }
            service::mutation::Mutation::CreateExternalTable(v) => {
                Mutation::CreateExternalTable(v.try_into()?)
            }
        })
    }
}

impl TryFrom<Mutation> for service::mutation::Mutation {
    type Error = ProtoConvError;
    fn try_from(value: Mutation) -> Result<Self, Self::Error> {
        Ok(match value {
            Mutation::DropSchema(v) => service::mutation::Mutation::DropSchema(v.into()),
            Mutation::DropObject(v) => service::mutation::Mutation::DropObject(v.into()),
            Mutation::CreateSchema(v) => service::mutation::Mutation::CreateSchema(v.into()),
            Mutation::CreateView(v) => service::mutation::Mutation::CreateView(v.into()),
            Mutation::CreateConnection(v) => {
                service::mutation::Mutation::CreateConnection(v.into())
            }
            Mutation::CreateExternalTable(v) => {
                service::mutation::Mutation::CreateExternalTable(v.try_into()?)
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct DropSchema {
    pub name: String,
    pub if_exists: bool,
}

impl TryFrom<service::DropSchema> for DropSchema {
    type Error = ProtoConvError;
    fn try_from(value: service::DropSchema) -> Result<Self, Self::Error> {
        // TODO: Check if string is zero value.
        Ok(DropSchema {
            name: value.name,
            if_exists: value.if_exists,
        })
    }
}

impl From<DropSchema> for service::DropSchema {
    fn from(value: DropSchema) -> Self {
        service::DropSchema {
            name: value.name,
            if_exists: value.if_exists,
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
}

impl TryFrom<service::CreateView> for CreateView {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateView) -> Result<Self, Self::Error> {
        // TODO: Check if string are zero value.
        Ok(CreateView {
            schema: value.schema,
            name: value.name,
            sql: value.sql,
        })
    }
}

impl From<CreateView> for service::CreateView {
    fn from(value: CreateView) -> Self {
        service::CreateView {
            schema: value.schema,
            name: value.name,
            sql: value.sql,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct CreateConnection {
    pub schema: String,
    pub name: String,
    pub options: ConnectionOptions,
    pub if_not_exists: bool,
}

impl TryFrom<service::CreateConnection> for CreateConnection {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateConnection) -> Result<Self, Self::Error> {
        Ok(CreateConnection {
            schema: value.schema,
            name: value.name,
            options: value.options.required("options")?,
            if_not_exists: value.if_not_exists,
        })
    }
}

impl From<CreateConnection> for service::CreateConnection {
    fn from(value: CreateConnection) -> Self {
        service::CreateConnection {
            schema: value.schema,
            name: value.name,
            options: Some(value.options.into()),
            if_not_exists: value.if_not_exists,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct CreateExternalTable {
    pub schema: String,
    pub name: String,
    pub connection_id: u32,
    pub options: TableOptions,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDefinition>,
}

impl TryFrom<service::CreateExternalTable> for CreateExternalTable {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateExternalTable) -> Result<Self, Self::Error> {
        // TODO: Check if string are zero value.
        Ok(CreateExternalTable {
            schema: value.schema,
            name: value.name,
            connection_id: value.connection_id,
            options: value.options.required("options")?,
            if_not_exists: value.if_not_exists,
            columns: value
                .columns
                .into_iter()
                .map(|col| col.try_into())
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<CreateExternalTable> for service::CreateExternalTable {
    type Error = ProtoConvError;
    fn try_from(value: CreateExternalTable) -> Result<Self, Self::Error> {
        Ok(service::CreateExternalTable {
            schema: value.schema,
            name: value.name,
            connection_id: value.connection_id,
            options: Some(value.options.into()),
            if_not_exists: value.if_not_exists,
            columns: value
                .columns
                .into_iter()
                .map(|col| col.try_into())
                .collect::<Result<_, _>>()?,
        })
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
