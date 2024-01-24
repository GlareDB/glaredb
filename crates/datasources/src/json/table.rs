use std::collections::HashMap;
use std::sync::Arc;
use std::vec::Vec;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::TableProvider;
use serde_json::{Map, Value};

use crate::common::url::DatasourceUrl;
use crate::json::errors::JsonError;
use crate::object_store::generic::GenericStoreAccess;
use crate::object_store::ObjStoreAccess;

pub async fn json_streaming_table(
    store_access: GenericStoreAccess,
    source_url: DatasourceUrl,
) -> Result<Arc<dyn TableProvider>, JsonError> {
    let path = source_url.path();

    let store = store_access.create_store()?;

    // assume that the file type is a glob and see if there are
    // more files...
    let mut list = store_access.list_globbed(&store, path.as_ref()).await?;

    if list.is_empty() {
        return Err(JsonError::NotFound(path.into_owned()));
    }

    // for consistent results, particularly for the sample, always
    // sort by location
    list.sort_by(|a, b| a.location.cmp(&b.location));

    let mut data = Vec::new();
    for obj in list {
        let blob = store.get(&obj.location).await?.bytes().await?.to_vec();
        let dejson = serde_json::from_slice::<serde_json::Value>(blob.as_slice())?.to_owned();
        push_unwind_json_values(&mut data, dejson)?;
    }

    let mut field_set = indexmap::IndexMap::<String, DataType>::new();
    for obj in data {
        for key in obj.keys() {
            if field_set.contains_key(key) {
                continue;
            }
            field_set.insert(key.to_string(), type_for_value(obj.get(key).unwrap()));
        }
    }

    Err(JsonError::NotFound("/usr/bin/bleh".to_string()))
}

fn push_unwind_json_values(
    data: &mut Vec<Map<String, Value>>,
    val: Value,
) -> Result<(), JsonError> {
    Ok(match val {
        Value::Array(vals) => {
            for v in vals {
                match v {
                    Value::Object(doc) => data.push(doc),
                    Value::Null => data.push(Map::new()),
                    _ => {
                        return Err(JsonError::UnspportedType(
                            "only objects and arrays of objects are supported",
                        ))
                    }
                }
            }
        }
        Value::Object(doc) => data.push(doc),
        Value::Null => data.push(Map::new()),
        _ => {
            return Err(JsonError::UnspportedType(
                "only objects and arrays of objects are supported",
            ))
        }
    })
}

fn type_for_value(value: &Value) -> DataType {
    match value {
        Value::Array(v) => {
            if v.is_empty() {
                DataType::List(Arc::new(Field::new("", DataType::Null, true)))
            } else {
                DataType::List(Arc::new(Field::new(
                    "",
                    type_for_value(&v.get(0).unwrap()),
                    true,
                )))
            }
        }
        Value::String(_) => DataType::Utf8,
        Value::Bool(_) => DataType::Boolean,
        Value::Null => DataType::Null,
        Value::Number(n) => {
            if n.is_i64() {
                DataType::Int64
            } else if n.is_u64() {
                DataType::UInt64
            } else {
                DataType::Float64
            }
        }
        Value::Object(obj) => {
            let mut fields = Vec::with_capacity(obj.len());
            for (k, v) in obj.iter() {
                fields.push(Field::new(k, type_for_value(v), true))
            }
            DataType::Struct(fields.into())
        }
    }
}
