use std::ops::Deref;

use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId, ListTypeMeta};
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use super::{PlannedScalarFunction, ScalarFunction};
use crate::expr::Expression;
use crate::functions::{plan_check_num_args, FunctionInfo, Signature};
use crate::logical::binder::bind_context::BindContext;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListExtract;

impl FunctionInfo for ListExtract {
    fn name(&self) -> &'static str {
        "list_extract"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[DataTypeId::List, DataTypeId::Int64],
            variadic: None,
            return_type: DataTypeId::Any,
        }]
    }
}

impl ScalarFunction for ListExtract {
    fn plan_from_datatypes(&self, _inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        unreachable!("plan_from_expressions implemented")
    }

    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        let mut packed = PackedDecoder::new(state);
        let datatype = DataType::from_proto(packed.decode_next()?)?;
        let index: u64 = packed.decode_next()?;
        Ok(Box::new(ListExtractImpl {
            datatype,
            index: index as usize,
        }))
    }

    fn plan_from_expressions(
        &self,
        bind_context: &BindContext,
        inputs: &[&Expression],
    ) -> Result<Box<dyn PlannedScalarFunction>> {
        let datatypes = inputs
            .iter()
            .map(|expr| expr.datatype(bind_context))
            .collect::<Result<Vec<_>>>()?;

        plan_check_num_args(self, &datatypes, 2)?;

        let index = ConstFold::rewrite(bind_context, inputs[1].clone())?
            .try_into_scalar()?
            .try_as_i64()?;

        if index <= 0 {
            return Err(RayexecError::new("Index cannot be less than 1"));
        }
        let index = (index - 1) as usize;

        let inner_datatype = match &datatypes[0] {
            DataType::List(meta) => meta.datatype.deref().clone(),
            other => {
                return Err(RayexecError::new(format!(
                    "Cannot index into non-list type, got {other}",
                )))
            }
        };

        Ok(Box::new(ListExtractImpl {
            datatype: inner_datatype,
            index,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListExtractImpl {
    datatype: DataType,
    index: usize,
}

impl PlannedScalarFunction for ListExtractImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &ListExtract
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        let mut packed = PackedEncoder::new(state);
        packed.encode_next(&self.datatype.to_proto()?)?;
        packed.encode_next(&(self.index as u64))?;
        Ok(())
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn execute(&self, _inputs: &[&Array]) -> Result<Array> {
        not_implemented!("list extract")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListValues;

impl FunctionInfo for ListValues {
    fn name(&self) -> &'static str {
        "list_values"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: &[],
            variadic: Some(DataTypeId::Any),
            return_type: DataTypeId::List,
        }]
    }
}

impl ScalarFunction for ListValues {
    fn decode_state(&self, state: &[u8]) -> Result<Box<dyn PlannedScalarFunction>> {
        Ok(Box::new(ListValuesImpl {
            datatype: DataType::from_proto(PackedDecoder::new(state).decode_next()?)?,
        }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        let first = match inputs.first() {
            Some(dt) => dt,
            None => {
                return Ok(Box::new(ListValuesImpl {
                    datatype: DataType::Null,
                }))
            }
        };

        for dt in inputs {
            if dt != first {
                return Err(RayexecError::new(format!(
                    "Not all inputs are the same type, got {dt}, expected {first}"
                )));
            }
        }

        Ok(Box::new(ListValuesImpl {
            datatype: DataType::List(ListTypeMeta {
                datatype: Box::new(first.clone()),
            }),
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListValuesImpl {
    datatype: DataType,
}

impl PlannedScalarFunction for ListValuesImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &ListValues
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.datatype.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.datatype.clone()
    }

    fn execute(&self, _inputs: &[&Array]) -> Result<Array> {
        not_implemented!("list values")
    }
}
