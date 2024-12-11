use std::borrow::Cow;
use std::fmt;

use fmtutil::IntoDisplayableSlice;
use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_error::Result;

use super::PhysicalScalarExpression;
use crate::database::DatabaseContext;
use crate::functions::scalar::PlannedScalarFunction;
use crate::proto::DatabaseProtoConv;

#[derive(Debug, Clone)]
pub struct PhysicalScalarFunctionExpr {
    pub function: PlannedScalarFunction,
    pub inputs: Vec<PhysicalScalarExpression>,
}

impl PhysicalScalarFunctionExpr {
    pub fn eval<'a>(&self, batch: &'a Batch) -> Result<Cow<'a, Array>> {
        let inputs = self
            .inputs
            .iter()
            .map(|input| input.eval(batch))
            .collect::<Result<Vec<_>>>()?;

        let refs: Vec<_> = inputs.iter().map(|a| a.as_ref()).collect(); // Can I not?
        let mut out = self.function.function_impl.execute(&refs)?;

        // If function is provided no input, it's expected to return an
        // array of length 1. We extend the array here so that it's the
        // same size as the rest.
        //
        // TODO: Could just extend the selection vector too.
        if refs.is_empty() {
            let scalar = out.logical_value(0)?;
            out = scalar.as_array(batch.num_rows())?;
        }

        Ok(Cow::Owned(out))
    }
}

impl fmt::Display for PhysicalScalarFunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.function.function.name(),
            self.inputs.display_as_list()
        )
    }
}

impl DatabaseProtoConv for PhysicalScalarFunctionExpr {
    type ProtoType = rayexec_proto::generated::physical_expr::PhysicalScalarFunctionExpr;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
        // Ok(Self::ProtoType {
        //     function: Some(self.function.to_proto_ctx(context)?),
        //     inputs: self
        //         .inputs
        //         .iter()
        //         .map(|input| input.to_proto_ctx(context))
        //         .collect::<Result<Vec<_>>>()?,
        // })
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
        // Ok(Self {
        //     function: DatabaseProtoConv::from_proto_ctx(
        //         proto.function.required("function")?,
        //         context,
        //     )?,
        //     inputs: proto
        //         .inputs
        //         .into_iter()
        //         .map(|input| DatabaseProtoConv::from_proto_ctx(input, context))
        //         .collect::<Result<Vec<_>>>()?,
        // })
    }
}
