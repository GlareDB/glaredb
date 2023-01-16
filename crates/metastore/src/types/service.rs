use super::{FromOptionalField, ProtoConvError};
use crate::proto::service;
use crate::types::catalog::{ConnectionOptions, TableOptions};
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

impl From<Mutation> for service::mutation::Mutation {
    fn from(value: Mutation) -> Self {
        match value {
            Mutation::DropSchema(v) => service::mutation::Mutation::DropSchema(v.into()),
            Mutation::DropObject(v) => service::mutation::Mutation::DropObject(v.into()),
            Mutation::CreateSchema(v) => service::mutation::Mutation::CreateSchema(v.into()),
            Mutation::CreateView(v) => service::mutation::Mutation::CreateView(v.into()),
            Mutation::CreateConnection(v) => {
                service::mutation::Mutation::CreateConnection(v.into())
            }
            Mutation::CreateExternalTable(v) => {
                service::mutation::Mutation::CreateExternalTable(v.into())
            }
        }
    }
}

impl From<Mutation> for service::Mutation {
    fn from(value: Mutation) -> Self {
        service::Mutation {
            mutation: Some(value.into()),
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct DropSchema {
    pub name: String,
}

impl TryFrom<service::DropSchema> for DropSchema {
    type Error = ProtoConvError;
    fn try_from(value: service::DropSchema) -> Result<Self, Self::Error> {
        // TODO: Check if string is zero value.
        Ok(DropSchema { name: value.name })
    }
}

impl From<DropSchema> for service::DropSchema {
    fn from(value: DropSchema) -> Self {
        service::DropSchema { name: value.name }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct DropObject {
    pub schema: String,
    pub name: String,
}

impl TryFrom<service::DropObject> for DropObject {
    type Error = ProtoConvError;
    fn try_from(value: service::DropObject) -> Result<Self, Self::Error> {
        // TODO: Check if strings are zero value.
        Ok(DropObject {
            schema: value.schema,
            name: value.name,
        })
    }
}

impl From<DropObject> for service::DropObject {
    fn from(value: DropObject) -> Self {
        service::DropObject {
            schema: value.schema,
            name: value.name,
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
}

impl TryFrom<service::CreateConnection> for CreateConnection {
    type Error = ProtoConvError;
    fn try_from(value: service::CreateConnection) -> Result<Self, Self::Error> {
        Ok(CreateConnection {
            schema: value.schema,
            name: value.name,
            options: value.options.required("options")?,
        })
    }
}

impl From<CreateConnection> for service::CreateConnection {
    fn from(value: CreateConnection) -> Self {
        service::CreateConnection {
            schema: value.schema,
            name: value.name,
            options: Some(value.options.into()),
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct CreateExternalTable {
    pub schema: String,
    pub name: String,
    pub connection_id: u32,
    pub options: TableOptions,
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
        })
    }
}

impl From<CreateExternalTable> for service::CreateExternalTable {
    fn from(value: CreateExternalTable) -> Self {
        service::CreateExternalTable {
            schema: value.schema,
            name: value.name,
            connection_id: value.connection_id,
            options: Some(value.options.into()),
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
            let p: service::mutation::Mutation = expected.clone().into();
            let got: Mutation = p.try_into().unwrap();
            assert_eq!(expected, got)
        }
    }
}
