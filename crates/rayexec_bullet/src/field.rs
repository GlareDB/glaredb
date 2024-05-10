use std::fmt;

/// All possible data types.
// TODO: Additional types (compound, decimal, timestamp, etc)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Null,
    Boolean,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    Struct { fields: Vec<DataType> },
}

impl DataType {
    pub const fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Float32
                | Self::Float64
                | Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::UInt8
                | Self::UInt16
                | Self::UInt32
                | Self::UInt64
        )
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "Null"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Float32 => write!(f, "Float32"),
            Self::Float64 => write!(f, "Float64"),
            Self::Int8 => write!(f, "Int8"),
            Self::Int16 => write!(f, "Int16"),
            Self::Int32 => write!(f, "Int32"),
            Self::Int64 => write!(f, "Int64"),
            Self::UInt8 => write!(f, "UInt8"),
            Self::UInt16 => write!(f, "UInt16"),
            Self::UInt32 => write!(f, "UInt32"),
            Self::UInt64 => write!(f, "UInt64"),
            Self::Utf8 => write!(f, "Utf8"),
            Self::LargeUtf8 => write!(f, "LargeUtf8"),
            Self::Binary => write!(f, "Binary"),
            Self::LargeBinary => write!(f, "LargeBinary"),
            Self::Struct { fields } => write!(
                f,
                "{{{}}}",
                fields
                    .iter()
                    .map(|typ| format!("{typ}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

/// A named field.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Represents the full schema of an output batch.
///
/// Includes the names and nullability of each of the columns.
#[derive(Debug, Clone, PartialEq, Eq)]
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
            fields: self
                .fields
                .into_iter()
                .chain(other.fields.into_iter())
                .collect(),
        }
    }

    /// Return an iterator over all the fields in the schema.
    pub fn iter(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter()
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

/// Represents the output types of a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
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
            types: self
                .types
                .into_iter()
                .chain(other.types.into_iter())
                .collect(),
        }
    }
}
