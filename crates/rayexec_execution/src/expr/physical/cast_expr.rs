use std::fmt;
use std::sync::Arc;

use rayexec_bullet::{
    array::Array, batch::Batch, bitmap::Bitmap, compute::cast::array::cast_array, datatype::DataType
};
use rayexec_error::{OptionExt, Result};
use rayexec_proto::ProtoConv;

use crate::{database::DatabaseContext, proto::DatabaseProtoConv};

use super::PhysicalScalarExpression;

#[derive(Debug, Clone)]
pub struct PhysicalCastExpr {
    pub to: DataType,
    pub expr: Box<PhysicalScalarExpression>,
}

impl PhysicalCastExpr {
    pub fn eval(&self, batch: &Batch, selection: Option<&Bitmap>) -> Result<Arc<Array>> {
        let input = self.expr.eval(batch, selection)?;
        let out = cast_array(&input, &self.to)?;
        Ok(Arc::new(out))
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
