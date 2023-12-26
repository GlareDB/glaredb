use datafusion::arrow::{
    datatypes::{DataType, Field, Fields, Schema},
    error::ArrowError,
};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

// NOTE: This InferredType enum and the corresponding impl was copied from "arrow_json"
#[derive(Debug, Clone)]
enum InferredType {
    Scalar(HashSet<DataType>),
    Array(Box<InferredType>),
    Object(HashMap<String, InferredType>),
    Any,
}

impl InferredType {
    fn merge(&mut self, other: InferredType) -> Result<(), ArrowError> {
        match (self, other) {
            (InferredType::Array(s), InferredType::Array(o)) => {
                s.merge(*o)?;
            }
            (InferredType::Scalar(self_hs), InferredType::Scalar(other_hs)) => {
                other_hs.into_iter().for_each(|v| {
                    self_hs.insert(v);
                });
            }
            (InferredType::Object(self_map), InferredType::Object(other_map)) => {
                for (k, v) in other_map {
                    self_map.entry(k).or_insert(InferredType::Any).merge(v)?;
                }
            }
            (s @ InferredType::Any, v) => {
                *s = v;
            }
            (_, InferredType::Any) => {}
            // convert a scalar type to a single-item scalar array type.
            (InferredType::Array(self_inner_type), other_scalar @ InferredType::Scalar(_)) => {
                self_inner_type.merge(other_scalar)?;
            }
            (s @ InferredType::Scalar(_), InferredType::Array(mut other_inner_type)) => {
                other_inner_type.merge(s.clone())?;
                *s = InferredType::Array(other_inner_type);
            }
            // incompatible types
            (s, o) => {
                return Err(ArrowError::JsonError(format!(
                    "Incompatible type found during schema inference: {s:?} v.s. {o:?}",
                )));
            }
        }

        Ok(())
    }
}

fn set_object_scalar_field_type(
    field_types: &mut HashMap<String, InferredType>,
    key: &str,
    data_type: DataType,
) -> Result<(), ArrowError> {
    if !field_types.contains_key(key) {
        field_types.insert(key.to_string(), InferredType::Scalar(HashSet::new()));
    }

    match field_types.get_mut(key).unwrap() {
        InferredType::Scalar(hs) => {
            hs.insert(data_type);
            Ok(())
        }

        primitive_array @ InferredType::Array(_) => {
            let mut hs = HashSet::new();
            hs.insert(data_type);
            primitive_array.merge(InferredType::Scalar(hs))?;
            Ok(())
        }
        other => Err(ArrowError::JsonError(format!(
            "Expected primitive or primitive array JSON type, found: {other:?}",
        ))),
    }
}

fn get_nested_object_array_type(array: &[Value]) -> Result<InferredType, ArrowError> {
    let mut field_types = HashMap::new();

    for v in array {
        match v {
            Value::Object(map) => {
                get_types_from_object(&mut field_types, map)?;
            }
            _ => {
                return Err(ArrowError::JsonError(format!(
                    "Expected object as value for nested object array, got: {v:?}"
                )));
            }
        }
    }

    Ok(InferredType::Object(field_types))
}

fn get_nested_array_type(array: &[Value]) -> Result<InferredType, ArrowError> {
    let mut inner_ele_type = InferredType::Any;

    for v in array {
        match v {
            Value::Array(inner_array) => {
                inner_ele_type.merge(get_array_element_type(inner_array)?)?;
            }
            x => {
                return Err(ArrowError::JsonError(format!(
                    "Got non array element in nested array: {x:?}"
                )));
            }
        }
    }

    Ok(InferredType::Array(Box::new(inner_ele_type)))
}

fn get_primitive_array_type(array: &[Value]) -> Result<InferredType, ArrowError> {
    let mut hs = HashSet::new();

    for v in array {
        match v {
            Value::Null => {}
            Value::Number(int) => {
                if int.is_i64() {
                    hs.insert(DataType::Int64);
                } else if int.is_f64() {
                    hs.insert(DataType::Float64);
                } else {
                    hs.insert(DataType::UInt64);
                }
            }
            Value::Bool(_) => {
                hs.insert(DataType::Boolean);
            }
            Value::String(_) => {
                hs.insert(DataType::Utf8);
            }
            Value::Array(_) | Value::Object(_) => {
                return Err(ArrowError::JsonError(format!(
                    "Expected primitive value for primitive array, got: {v:?}"
                )));
            }
        }
    }

    Ok(InferredType::Scalar(hs))
}

fn get_array_element_type(array: &[Value]) -> Result<InferredType, ArrowError> {
    match array.iter().take(1).next() {
        // in this case, we have an empty array
        None => Ok(InferredType::Any),
        Some(a) => match a {
            Value::Array(_) => get_nested_array_type(array),
            Value::Object(_) => get_nested_object_array_type(array),
            _ => get_primitive_array_type(array),
        },
    }
}

fn get_types_from_object(
    type_defs: &mut HashMap<String, InferredType>,
    object: &serde_json::map::Map<String, Value>,
) -> Result<(), ArrowError> {
    for (key, value) in object {
        match value {
            //TODO skip, complain or exit. Figure it out.
            Value::Null => {}
            Value::Bool(_) => {
                set_object_scalar_field_type(type_defs, key, DataType::Boolean)?;
            }
            Value::String(_) => {
                set_object_scalar_field_type(type_defs, key, DataType::Utf8)?;
            }
            Value::Number(int) => {
                if int.is_i64() {
                    set_object_scalar_field_type(type_defs, key, DataType::Int64)?;
                } else if int.is_f64() {
                    set_object_scalar_field_type(type_defs, key, DataType::Float64)?;
                } else {
                    set_object_scalar_field_type(type_defs, key, DataType::UInt64)?;
                }
            }
            Value::Object(obj) => {
                if !type_defs.contains_key(key) {
                    type_defs.insert(key.to_string(), InferredType::Object(HashMap::new()));
                }
                match type_defs.get_mut(key).unwrap() {
                    InferredType::Object(inner_field_types) => {
                        get_types_from_object(inner_field_types, obj)?;
                    }
                    other => {
                        return Err(ArrowError::JsonError(format!(
                            "Expected object json type, found: {other:?}",
                        )));
                    }
                }
            }
            Value::Array(arr) => {
                let el_type = get_array_element_type(arr)?;
                if !type_defs.contains_key(key) {
                    match el_type {
                        InferredType::Scalar(_) => {
                            type_defs.insert(
                                key.to_string(),
                                InferredType::Array(Box::new(InferredType::Scalar(HashSet::new()))),
                            );
                        }
                        InferredType::Object(_) => {
                            type_defs.insert(
                                key.to_string(),
                                InferredType::Array(Box::new(InferredType::Object(HashMap::new()))),
                            );
                        }
                        InferredType::Any | InferredType::Array(_) => {
                            type_defs.insert(
                                key.to_string(),
                                InferredType::Array(Box::new(InferredType::Any)),
                            );
                        }
                    }
                }

                match type_defs.get_mut(key).unwrap() {
                    InferredType::Array(inner_type) => {
                        inner_type.merge(el_type)?;
                    }
                    // in case of column contains both primitive type and primitive array type, we
                    // convert type of this column to scalar array.
                    field_type @ InferredType::Scalar(_) => {
                        field_type.merge(el_type)?;
                        *field_type = InferredType::Array(Box::new(field_type.clone()));
                    }
                    t => {
                        return Err(ArrowError::JsonError(format!(
                            "Expected array json type, found: {t:?}",
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

// TODO revisit the "int64" and "float64" coercions. we don't want to assume since glaredb displays types on results.
/// Coerce data type during inference
///
/// * `Int64` and `Float64` should be `Float64`
/// * Lists and primitives are coerced to a list of a compatible primitives
/// * All other types are coerced to `Utf8`
fn coerce_data_type(datatypes: Vec<&DataType>) -> DataType {
    let mut dt_iter = datatypes.into_iter().cloned();
    let dt_init = dt_iter.next().unwrap_or(DataType::Utf8);

    dt_iter.fold(dt_init, |l, r| match (l, r) {
        (DataType::Boolean, DataType::Boolean) => DataType::Boolean,
        (DataType::Int64, DataType::Int64) => DataType::Int64,
        (DataType::Float64, DataType::Float64)
        | (DataType::Float64, DataType::Int64)
        | (DataType::Int64, DataType::Float64) => DataType::Float64,
        (DataType::List(l), DataType::List(r)) => DataType::List(Arc::new(Field::new(
            "item",
            coerce_data_type(vec![l.data_type(), r.data_type()]),
            true,
        ))),
        // coerce scalar and scalar array into scalar array
        (DataType::List(e), not_list) | (not_list, DataType::List(e)) => {
            DataType::List(Arc::new(Field::new(
                "item",
                coerce_data_type(vec![e.data_type(), &not_list]),
                true,
            )))
        }
        _ => DataType::Utf8,
    })
}

fn convert_inferred_type_to_arrow(t: &InferredType) -> Result<DataType, ArrowError> {
    Ok(match t {
        InferredType::Scalar(hs) => coerce_data_type(hs.iter().collect()),
        InferredType::Object(def) => DataType::Struct(generate_arrow_fields(def)?),
        InferredType::Array(ele_type) => DataType::List(Arc::new(Field::new(
            "item",
            convert_inferred_type_to_arrow(ele_type)?,
            true,
        ))),
        InferredType::Any => DataType::Null,
    })
}

fn generate_arrow_fields(definition: &HashMap<String, InferredType>) -> Result<Fields, ArrowError> {
    definition
        .iter()
        .map(|(k, types)| Ok(Field::new(k, convert_inferred_type_to_arrow(types)?, true)))
        .collect()
}

pub fn infer_schema_from_value(v: Value, range: usize) -> Result<Schema, ArrowError> {
    let mut type_defs: HashMap<String, InferredType> = HashMap::new();
    if !v.is_array() {
        return Err(ArrowError::JsonError(
            "Expected an array found an object".to_string(),
        ));
    }

    //TODO handle error
    let documents = v.as_array().unwrap();
    for doc in documents.iter().take(range) {
        match doc {
            Value::Object(map) => get_types_from_object(&mut type_defs, map)?,

            other => {
                return Err(ArrowError::JsonError(format!(
                    "Expected JSON record to be an object, found {other:?}"
                )));
            }
        }
    }

    Ok(Schema::new(generate_arrow_fields(&type_defs)?))
}
