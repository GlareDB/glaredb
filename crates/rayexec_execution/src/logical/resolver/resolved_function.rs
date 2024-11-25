use rayexec_error::{OptionExt, Result};

use crate::database::DatabaseContext;
use crate::functions::aggregate::AggregateFunction;
use crate::functions::scalar::ScalarFunction;
use crate::proto::DatabaseProtoConv;

/// "Builtin" functions that require special handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecialBuiltinFunction {
    Unnest,
}

impl SpecialBuiltinFunction {
    pub fn name(&self) -> &str {
        match self {
            Self::Unnest => "unnest",
        }
    }

    pub fn try_from_name(func_name: &str) -> Option<Self> {
        match func_name {
            "unnest" => Some(Self::Unnest),
            _ => None,
        }
    }
}

/// A resolved aggregate or scalar function.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedFunction {
    Scalar(Box<dyn ScalarFunction>),
    Aggregate(Box<dyn AggregateFunction>),
    Special(SpecialBuiltinFunction),
}

impl ResolvedFunction {
    pub fn name(&self) -> &str {
        match self {
            Self::Scalar(f) => f.name(),
            Self::Aggregate(f) => f.name(),
            Self::Special(f) => f.name(),
        }
    }

    pub fn is_aggregate(&self) -> bool {
        matches!(self, ResolvedFunction::Aggregate(_))
    }
}

impl DatabaseProtoConv for ResolvedFunction {
    type ProtoType = rayexec_proto::generated::resolver::ResolvedFunctionReference;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::resolver::resolved_function_reference::Value;

        let value = match self {
            Self::Scalar(scalar) => Value::Scalar(scalar.to_proto_ctx(context)?),
            Self::Aggregate(agg) => Value::Aggregate(agg.to_proto_ctx(context)?),
            Self::Special(_) => todo!(),
        };

        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        use rayexec_proto::generated::resolver::resolved_function_reference::Value;

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
