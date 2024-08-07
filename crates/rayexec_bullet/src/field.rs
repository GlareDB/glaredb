use rayexec_error::{OptionExt, Result};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use crate::datatype::DataType;

/// A named field.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
}

impl Field {
    pub fn new(name: impl Into<String>, datatype: DataType, nullable: bool) -> Self {
        Field {
            name: name.into(),
            datatype,
            nullable,
        }
    }
}

impl ProtoConv for Field {
    type ProtoType = rayexec_proto::generated::schema::Field;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            name: self.name.clone(),
            datatype: Some(self.datatype.to_proto()?),
            nullable: self.nullable,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            name: proto.name,
            datatype: DataType::from_proto(proto.datatype.required("datatype")?)?,
            nullable: proto.nullable,
        })
    }
}

/// Represents the full schema of an output batch.
///
/// Includes the names and nullability of each of the columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    /// Create an empty schema.
    pub const fn empty() -> Self {
        Schema { fields: Vec::new() }
    }

    pub fn new(fields: impl IntoIterator<Item = Field>) -> Self {
        Schema {
            fields: fields.into_iter().collect(),
        }
    }

    pub fn merge(self, other: Schema) -> Self {
        Schema {
            fields: self.fields.into_iter().chain(other.fields).collect(),
        }
    }

    /// Return an iterator over all the fields in the schema.
    pub fn iter(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter()
    }

    pub fn type_schema(&self) -> TypeSchema {
        TypeSchema {
            types: self
                .fields
                .iter()
                .map(|field| field.datatype.clone())
                .collect(),
        }
    }

    /// Convert the schema into a type schema.
    pub fn into_type_schema(self) -> TypeSchema {
        TypeSchema {
            types: self
                .fields
                .into_iter()
                .map(|field| field.datatype)
                .collect(),
        }
    }
}

impl ProtoConv for Schema {
    type ProtoType = rayexec_proto::generated::schema::Schema;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        let fields = self
            .fields
            .iter()
            .map(|f| f.to_proto())
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::ProtoType { fields })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        let fields = proto
            .fields
            .into_iter()
            .map(Field::from_proto)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { fields })
    }
}

/// Represents the output types of a batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeSchema {
    pub types: Vec<DataType>,
}

impl TypeSchema {
    /// Create an empty type schema.
    pub const fn empty() -> Self {
        TypeSchema { types: Vec::new() }
    }

    pub fn new(types: impl IntoIterator<Item = DataType>) -> Self {
        TypeSchema {
            types: types.into_iter().collect(),
        }
    }

    pub fn merge(self, other: TypeSchema) -> Self {
        TypeSchema {
            types: self.types.into_iter().chain(other.types).collect(),
        }
    }
}

impl ProtoConv for TypeSchema {
    type ProtoType = rayexec_proto::generated::schema::TypeSchema;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        let types = self
            .types
            .iter()
            .map(|t| t.to_proto())
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::ProtoType { types })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        let types = proto
            .types
            .into_iter()
            .map(DataType::from_proto)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { types })
    }
}
