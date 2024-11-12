use rayexec_bullet::array::Array;
use rayexec_bullet::datatype::{DataType, DataTypeId, ListTypeMeta};
use rayexec_bullet::executor::scalar::concat;
use rayexec_bullet::storage::ListStorage;
use rayexec_error::{RayexecError, Result};
use rayexec_proto::packed::{PackedDecoder, PackedEncoder};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction};
use crate::functions::{FunctionInfo, Signature};

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
            list_datatype: DataType::from_proto(PackedDecoder::new(state).decode_next()?)?,
        }))
    }

    fn plan_from_datatypes(&self, inputs: &[DataType]) -> Result<Box<dyn PlannedScalarFunction>> {
        let first = match inputs.first() {
            Some(dt) => dt,
            None => {
                return Ok(Box::new(ListValuesImpl {
                    list_datatype: DataType::List(ListTypeMeta {
                        datatype: Box::new(DataType::Null),
                    }),
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
            list_datatype: DataType::List(ListTypeMeta {
                datatype: Box::new(first.clone()),
            }),
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListValuesImpl {
    list_datatype: DataType,
}

impl PlannedScalarFunction for ListValuesImpl {
    fn scalar_function(&self) -> &dyn ScalarFunction {
        &ListValues
    }

    fn encode_state(&self, state: &mut Vec<u8>) -> Result<()> {
        PackedEncoder::new(state).encode_next(&self.list_datatype.to_proto()?)
    }

    fn return_type(&self) -> DataType {
        self.list_datatype.clone()
    }

    fn execute(&self, inputs: &[&Array]) -> Result<Array> {
        if inputs.is_empty() {
            let inner_type = match &self.list_datatype {
                DataType::List(l) => l.datatype.as_ref(),
                other => panic!("invalid data type: {other}"),
            };

            let data = ListStorage::empty_list(Array::new_typed_null_array(inner_type.clone(), 1)?);
            return Ok(Array::new_with_array_data(self.list_datatype.clone(), data));
        }

        let out = concat(inputs)?;
        let data = ListStorage::single_list(out);

        Ok(Array::new_with_array_data(self.list_datatype.clone(), data))
    }
}
