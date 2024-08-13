use crate::{
    database::DatabaseContext,
    functions::{aggregate::AggregateFunction, scalar::ScalarFunction},
    proto::DatabaseProtoConv,
};
use rayexec_error::{OptionExt, Result};

/// A bound aggregate or scalar function.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundFunction {
    Scalar(Box<dyn ScalarFunction>),
    Aggregate(Box<dyn AggregateFunction>),
}

impl BoundFunction {
    pub fn name(&self) -> &str {
        match self {
            Self::Scalar(f) => f.name(),
            Self::Aggregate(f) => f.name(),
        }
    }
}

impl DatabaseProtoConv for BoundFunction {
    type ProtoType = rayexec_proto::generated::binder::BoundFunctionReference;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::binder::bound_function_reference::Value;

        let value = match self {
            Self::Scalar(scalar) => Value::Scalar(scalar.to_proto_ctx(context)?),
            Self::Aggregate(agg) => Value::Aggregate(agg.to_proto_ctx(context)?),
        };

        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        use rayexec_proto::generated::binder::bound_function_reference::Value;

        Ok(match proto.value.required("value")? {
            Value::Scalar(scalar) => {
                Self::Scalar(DatabaseProtoConv::from_proto_ctx(scalar, context)?)
            }
            Value::Aggregate(agg) => {
                Self::Aggregate(DatabaseProtoConv::from_proto_ctx(agg, context)?)
            }
        })
    }
}
