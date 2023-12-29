use bson::Document;
use datafusion::arrow::datatypes::Fields;
use serde_json::{from_str, Map, Value};

use super::*;
use datasources::bson::scalar as bson_scalar;

pub struct Json;

impl ConstBuiltinFunction for Json {
    const NAME: &'static str = "unwind_json";
    const DESCRIPTION: &'static str =
        "Converts a json document in a string feilld to a struct field.";
    const EXAMPLE: &'static str = "unwind_json(<value>)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            // args: <FIELD>
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8]),
            ]),
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for Json {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF {
            name: Self::NAME.to_string(),
            signature: ConstBuiltinFunction::signature(self).unwrap(),
            return_type: Arc::new(|_| Ok(Arc::new(DataType::Struct(Fields::empty())))),
            fun: Arc::new(move |input| {
                Ok(get_nth_scalar_value(input, 0, &|value| -> Result<
                    ScalarValue,
                    BuiltinError,
                > {
                    match value {
                        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => {
                            let jv = from_str::<Map<String, Value>>(v.unwrap_or_default().as_str())
                                .map_err(|e| BuiltinError::ParseError(e.to_string()))?;

                            let mut values = Vec::with_capacity(jv.len());
                            let mut fields = Vec::with_capacity(jv.len());

                            for (k, v) in jv {
                                match v {
                                    Value::Null => {
                                        fields.push(Field::new(k, DataType::Null, true));
                                        values.push(ScalarValue::Null);
                                    }
                                    Value::Bool(item) => {
                                        fields.push(Field::new(k, DataType::Boolean, true));
                                        values.push(ScalarValue::Boolean(Some(item)));
                                    }
                                    Value::String(item) => {
                                        fields.push(Field::new(k, DataType::Utf8, true));
                                        values.push(ScalarValue::Utf8(Some(item)));
                                    }
                                    Value::Number(item) => {
                                        if item.is_i64() {
                                            fields.push(Field::new(k, DataType::Int64, true));
                                            values.push(ScalarValue::Int64(item.as_i64()));
                                        } else if item.is_u64() {
                                            fields.push(Field::new(k, DataType::UInt64, true));
                                            values.push(ScalarValue::UInt64(item.as_u64()));
                                        } else if item.is_f64() {
                                            fields.push(Field::new(k, DataType::Float64, true));
                                            values.push(ScalarValue::Float64(item.as_f64()));
                                        } else {
                                            panic!("unreachable")
                                        }
                                    }
                                    Value::Array(_) | Value::Object(_) => {
                                        fields.push(Field::new(k, DataType::Utf8, true));
                                        values.push(ScalarValue::Utf8(Some(v.to_string())));
                                    }
                                }
                            }
                            Ok(ScalarValue::Struct(Some(values), fields.into()))
                        }
                        _ => Err(BuiltinError::IncorrectType(
                            value.data_type(),
                            DataType::Utf8,
                        )),
                    }
                })?)
            }),
        };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }
}

pub struct Bson;

impl ConstBuiltinFunction for Bson {
    const NAME: &'static str = "unwind_bson";
    const DESCRIPTION: &'static str =
        "Converts a bson document in a binary field to a struct value.";
    const EXAMPLE: &'static str = "unwind_bson(<value>)";
    const FUNCTION_TYPE: FunctionType = FunctionType::Scalar;

    fn signature(&self) -> Option<Signature> {
        Some(Signature::new(
            // args: <FIELD>
            TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Binary]),
                TypeSignature::Exact(vec![DataType::LargeBinary]),
            ]),
            Volatility::Immutable,
        ))
    }
}

impl BuiltinScalarUDF for Bson {
    fn as_expr(&self, args: Vec<Expr>) -> Expr {
        let udf =
            ScalarUDF {
                name: Self::NAME.to_string(),
                signature: ConstBuiltinFunction::signature(self).unwrap(),
                return_type: Arc::new(|_| Ok(Arc::new(DataType::Struct(Fields::empty())))),
                fun: Arc::new(move |input| {
                    Ok(get_nth_scalar_value(input, 0, &|value| -> Result<
                        ScalarValue,
                        BuiltinError,
                    > {
                        match value {
                            ScalarValue::Binary(v) | ScalarValue::LargeBinary(v) => {
                                match v {
                                    None => Ok(ScalarValue::Null),
                                    Some(slice) => bson_scalar::from_document(
                                        bson::de::from_slice::<Document>(v)?,
                                    )?,
                                }
                                Ok(ScalarValue::Struct(Some(values), fields.into()))
                            }
                            _ => Err(BuiltinError::IncorrectType(
                                value.data_type(),
                                DataType::Utf8,
                            )),
                        }
                    })?)
                }),
            };
        Expr::ScalarUDF(datafusion::logical_expr::expr::ScalarUDF::new(
            Arc::new(udf),
            args,
        ))
    }
}
