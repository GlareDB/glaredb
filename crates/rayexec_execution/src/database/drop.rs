use rayexec_error::{OptionExt, Result};
use rayexec_proto::ProtoConv;

/// The object we're dropping.
///
/// Most objects are namespaced with a schema, and so will also have their names
/// included.
///
/// If we're dropping a scheme, that schema's name is already included in the
/// drop info.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DropObject {
    Index(String),
    Function(String),
    Table(String),
    View(String),
    Schema,
}

impl ProtoConv for DropObject {
    type ProtoType = rayexec_proto::generated::execution::DropObject;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::execution::drop_object::Value;
        use rayexec_proto::generated::execution::EmptyDropObject;

        Ok(Self::ProtoType {
            value: Some(match self {
                Self::Index(s) => Value::Index(s.clone()),
                Self::Function(s) => Value::Function(s.clone()),
                Self::Table(s) => Value::Table(s.clone()),
                Self::View(s) => Value::View(s.clone()),
                Self::Schema => Value::Schema(EmptyDropObject {}),
            }),
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        use rayexec_proto::generated::execution::drop_object::Value;

        Ok(match proto.value.required("value")? {
            Value::Index(s) => Self::Index(s),
            Value::Function(s) => Self::Function(s),
            Value::Table(s) => Self::Table(s),
            Value::View(s) => Self::View(s),
            Value::Schema(_) => Self::Schema,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropInfo {
    pub schema: String,
    pub object: DropObject,
    pub cascade: bool,
    pub if_exists: bool,
}

impl ProtoConv for DropInfo {
    type ProtoType = rayexec_proto::generated::execution::DropInfo;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            schema: self.schema.clone(),
            object: Some(self.object.to_proto()?),
            cascade: self.cascade,
            if_exists: self.if_exists,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            schema: proto.schema,
            object: DropObject::from_proto(proto.object.required("object")?)?,
            cascade: proto.cascade,
            if_exists: proto.if_exists,
        })
    }
}
