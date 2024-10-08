use std::borrow::Cow;
use std::fmt;

use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::compute::cast::array::cast_array;
use rayexec_bullet::compute::cast::behavior::CastFailBehavior;
use rayexec_bullet::datatype::DataType;
use rayexec_error::{OptionExt, Result};
use rayexec_proto::ProtoConv;

use super::PhysicalScalarExpression;
use crate::database::DatabaseContext;
use crate::proto::DatabaseProtoConv;

#[derive(Debug, Clone)]
pub struct PhysicalCastExpr {
    pub to: DataType,
    pub expr: Box<PhysicalScalarExpression>,
}

impl PhysicalCastExpr {
    pub fn eval<'a>(&self, batch: &'a Batch) -> Result<Cow<'a, Array>> {
        let input = self.expr.eval(batch)?;
        let out = cast_array(input.as_ref(), self.to.clone(), CastFailBehavior::Error)?;
        Ok(Cow::Owned(out))
    }
}

impl fmt::Display for PhysicalCastExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST({} TO {})", self.expr, self.to)
    }
}

impl DatabaseProtoConv for PhysicalCastExpr {
    type ProtoType = rayexec_proto::generated::physical_expr::PhysicalCastExpr;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            cast_to: Some(self.to.to_proto()?),
            expr: Some(Box::new(self.expr.to_proto_ctx(context)?)),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            to: ProtoConv::from_proto(proto.cast_to.required("to")?)?,
            expr: Box::new(DatabaseProtoConv::from_proto_ctx(
                *proto.expr.required("expr")?,
                context,
            )?),
        })
    }
}
