use std::sync::Arc;
use std::vec::Vec;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::streaming::PartitionStream;
use object_store::ObjectStore;
use serde_json::{Map, Value};

use crate::common::url::DatasourceUrl;
use crate::json::errors::JsonError;
use crate::json::stream::{JsonPartitionStream, LazyJsonPartitionStream};
use crate::object_store::ObjStoreAccess;

pub async fn json_streaming_table(
    store_access: Arc<dyn ObjStoreAccess>,
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
    {
        let first_obj = list
            .pop()
            .ok_or_else(|| JsonError::NotFound(path.into_owned()))?;
        let blob = store
            .get(&first_obj.location)
            .await?
            .bytes()
            .await?
            .to_vec();

        push_unwind_json_values(
            &mut data,
            serde_json::from_slice::<serde_json::Value>(&blob),
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

    let mut streams = Vec::<Arc<dyn PartitionStream>>::with_capacity(list.len());
    streams.push(Arc::new(JsonPartitionStream::new(schema.clone(), data)));
    for obj in list {
        streams.push(Arc::new(LazyJsonPartitionStream::new(
            schema.clone(),
            store.clone(),
            obj,
        )));
    }

    Ok(Arc::new(StreamingTable::try_new(schema.clone(), streams)?))
}

pub(crate) fn push_unwind_json_values(
    data: &mut Vec<Map<String, Value>>,
    val: Result<Value, serde_json::Error>,
) -> Result<(), JsonError> {
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
