use std::sync::Arc;
use std::vec::Vec;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::streaming::PartitionStream;
use jaq_interpret::{Ctx, Filter, FilterT, RcIter, Val};
use object_store::{ObjectMeta, ObjectStore};
use serde_json::{Map, Value};

use super::jaq::compile_jaq_query;
use crate::common::url::DatasourceUrl;
use crate::json::errors::JsonError;
use crate::json::stream::{ObjectStorePartition, VectorPartition};
use crate::object_store::{ObjStoreAccess, ObjStoreAccessor};

pub async fn json_streaming_table(
    store_access: Arc<dyn ObjStoreAccess>,
    source_url: DatasourceUrl,
    fields: Option<Schema>,
    jaq_filter: Option<String>,
) -> Result<Arc<dyn TableProvider>, JsonError> {
    let path = source_url.path().into_owned();

    let accessor = ObjStoreAccessor::new(store_access)?;

    let mut list = accessor.list_globbed(&path).await?;
    if list.is_empty() {
        return Err(JsonError::NotFound(path));
    }

    // for consistent results, particularly for the sample, always
    // sort by location
    list.sort_by(|a, b| a.location.cmp(&b.location));

    let store = accessor.into_object_store();

    json_streaming_table_inner(store, &path, list, fields, jaq_filter).await
}

pub async fn json_streaming_table_from_object(
    store: Arc<dyn ObjectStore>,
    object: ObjectMeta,
) -> Result<Arc<dyn TableProvider>, JsonError> {
    json_streaming_table_inner(store, "", vec![object], None, None).await
}

async fn json_streaming_table_inner(
    store: Arc<dyn ObjectStore>,
    original_path: &str, // Just for error
    mut list: Vec<ObjectMeta>,
    schema: Option<Schema>,
    jaq_filter: Option<String>,
) -> Result<Arc<dyn TableProvider>, JsonError> {
    let filter = match jaq_filter {
        Some(query) => Some(Arc::new(compile_jaq_query(query)?)),
        None => None,
    };

    let mut streams = Vec::<Arc<dyn PartitionStream>>::with_capacity(list.len());

    let schema = match schema {
        Some(fields) => Arc::new(fields),
        None => {
            let mut data = Vec::new();
            {
                let first_obj = list
                    .pop()
                    .ok_or_else(|| JsonError::NotFound(original_path.to_string()))?;

                let blob = store
                    .get(&first_obj.location)
                    .await?
                    .bytes()
                    .await?
                    .to_vec();

                push_unwind_json_values(
                    &mut data,
                    serde_json::Deserializer::from_slice(&blob).into_iter(),
                    &filter,
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
            filter.clone(),
        )));
    }

    Ok(Arc::new(StreamingTable::try_new(schema.clone(), streams)?))
}

fn push_unwind_json_values(
    data: &mut Vec<Map<String, Value>>,
    vals: impl Iterator<Item = Result<Value, serde_json::Error>>,
    filter: &Option<Arc<Filter>>,
) -> Result<(), JsonError> {
    for val in vals {
        let value = match filter {
            Some(jq) => {
                let inputs = RcIter::new(core::iter::empty());
                Value::from_iter(
                    jq.run((Ctx::new([], &inputs), Val::from(val?)))
                        .map(|res| res.map(Value::from))
                        .collect::<Result<Vec<_>, _>>()?,
                )
            }
            None => val?,
        };

        match value {
            Value::Array(vals) => {
                for v in vals {
                    match v {
                        Value::Object(doc) => data.push(doc),
                        Value::Null => data.push(Map::new()),
                        Value::Array(_) => {
                            if filter.is_some() {
                                return Err(JsonError::UnsupportedNestedArray(
                                    "must return objects from jaq expressions",
                                ));
                            } else {
                                return Err(JsonError::UnsupportedType(
                                    "arrays must contain objects or nulls",
                                ));
                            }
                        }
                        _ => {
                            return Err(JsonError::UnsupportedNestedArray(
                                "scalars in arrays are not supported",
                            ))
                        }
                    }
                }
            }
            Value::Object(doc) => data.push(doc),
            Value::Null => data.push(Map::new()),
            _ => return Err(JsonError::UnsupportedType("scalars cannot be rows")),
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
