use std::sync::Arc;
use std::vec::Vec;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::streaming::PartitionStream;
use object_store::ObjectStore;
use serde_json::{Map, Value};

use crate::common::url::DatasourceUrl;
use crate::json::errors::JsonError;
use crate::json::stream::{ObjectStorePartition, VectorPartition};
use crate::object_store::{ObjStoreAccess, ObjStoreAccessor};

pub async fn json_streaming_table(
    store_access: Arc<dyn ObjStoreAccess>,
    source_url: DatasourceUrl,
    fields: Option<Vec<FieldRef>>,
) -> Result<Arc<dyn TableProvider>, JsonError> {
    let path = source_url.path().into_owned();

    let accessor = ObjStoreAccessor::new(store_access)?;

    let mut list = accessor.list_globbed(source_url.path()).await?;
    if list.is_empty() {
        return Err(JsonError::NotFound(path));
    }

    // for consistent results, particularly for the sample, always
    // sort by location
    list.sort_by(|a, b| a.location.cmp(&b.location));

    let store = accessor.into_object_store();

    let mut streams = Vec::<Arc<dyn PartitionStream>>::with_capacity(list.len());

    let schema = match fields {
        Some(fields) => Arc::new(Schema::new(fields)),
        None => {
            let mut data = Vec::new();
            {
                let first_obj = list.pop().ok_or_else(|| JsonError::NotFound(path))?;
                let blob = store
                    .get(&first_obj.location)
                    .await?
                    .bytes()
                    .await?
                    .to_vec();

                push_unwind_json_values(
                    &mut data,
                    serde_json::Deserializer::from_slice(&blob).into_iter(),
                )?;
            }

            let mut field_set = indexmap::IndexMap::<String, DataType>::new();
            for obj in &data {
                for (key, value) in obj.into_iter() {
                    let typ = type_for_value(value);
                    match field_set.get(key) {
                        Some(v) => match widen_type(v, typ) {
                            Some(wider) => field_set.insert(key.to_string(), wider),
                            None => None,
                        },
                        None => field_set.insert(key.to_string(), typ),
                    };
                }
            }

            let schema = Arc::new(Schema::new(
                field_set
                    .into_iter()
                    .map(|(k, v)| Field::new(k, v, true))
                    .collect::<Vec<_>>(),
            ));

            streams.push(Arc::new(VectorPartition::new(schema.clone(), data)));
            schema
        }
    };


    for obj in list {
        streams.push(Arc::new(ObjectStorePartition::new(
            schema.clone(),
            store.clone(),
            obj,
        )));
    }

    Ok(Arc::new(StreamingTable::try_new(schema.clone(), streams)?))
}


fn push_unwind_json_values(
    data: &mut Vec<Map<String, Value>>,
    vals: impl Iterator<Item = Result<Value, serde_json::Error>>,
) -> Result<(), JsonError> {
    for val in vals {
        match val? {
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
        };
    }
    Ok(())
}

fn widen_type(left: &DataType, right: DataType) -> Option<DataType> {
    match (left, right) {
        (&DataType::Null, right) => Some(right),
        (&DataType::Int64 | &DataType::UInt64, DataType::Float64) => Some(DataType::Float64),
        _ => None,
    }
}

fn type_for_value(value: &Value) -> DataType {
    match value {
        Value::Array(v) => {
            if v.is_empty() {
                DataType::List(Arc::new(Field::new("", DataType::Null, true)))
            } else {
                DataType::List(Arc::new(Field::new(
                    "item",
                    type_for_value(v.first().unwrap()),
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
