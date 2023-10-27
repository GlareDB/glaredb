use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, FieldRef, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use napi::{bindgen_prelude::*, JsObject};

use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;

#[napi(object)]
pub struct JsRecordBatch {
    pub schema: JsSchema,
    pub columns: Vec<JsDynArray>,
    pub row_count: u32,
}

impl From<RecordBatch> for JsRecordBatch {
    fn from(value: RecordBatch) -> Self {
        let schema = JsSchema::from(value.schema());
        let row_count = value.num_rows() as u32;
        let columns = value
            .columns()
            .iter()
            .map(|col| {
                let col = col.as_ref();

                JsDynArray::from(col)
            })
            .collect::<Vec<_>>();

        Self {
            schema,
            row_count,
            columns,
        }
    }
}

#[napi(object)]
pub struct JsDynArray {
    pub datatype: DataTypeKind,
    pub nulls: Option<Vec<u8>>,
    pub offsets: Option<Vec<u8>>,
    pub values: Vec<u8>,
}

#[napi(object)]
pub struct JsArrowArray {
    pub datatype: DataTypeKind,
    pub data: JsObject,
}

impl From<&dyn Array> for JsDynArray {
    fn from(value: &dyn Array) -> Self {
        let nulls = value
            .nulls()
            .map(|nulls| nulls.buffer().as_slice().to_vec());

        let (offsets, values) = match value.to_data().buffers() {
            [values] => (None, values.as_slice().to_vec()),
            [offsets, values] => (
                Some(offsets.as_slice().to_vec()),
                values.as_slice().to_vec(),
            ),
            _ => todo!(),
        };

        Self {
            datatype: value.data_type().into(),
            offsets,
            values,
            nulls,
        }
    }
}

#[napi(object)]
pub struct JsDataType {
    pub kind: DataTypeKind,
    pub inner: Option<serde_json::Value>,
}

impl From<&DataType> for JsDataType {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::Null => Self {
                kind: DataTypeKind::Null,
                inner: None,
            },
            DataType::Boolean => Self {
                kind: DataTypeKind::Boolean,
                inner: None,
            },
            DataType::Int8 => Self {
                kind: DataTypeKind::Int8,
                inner: None,
            },
            DataType::Int16 => Self {
                kind: DataTypeKind::Int16,
                inner: None,
            },
            DataType::Int32 => Self {
                kind: DataTypeKind::Int32,
                inner: None,
            },
            DataType::Int64 => Self {
                kind: DataTypeKind::Int64,
                inner: None,
            },
            DataType::UInt8 => Self {
                kind: DataTypeKind::UInt8,
                inner: None,
            },
            DataType::UInt16 => Self {
                kind: DataTypeKind::UInt16,
                inner: None,
            },
            DataType::UInt32 => Self {
                kind: DataTypeKind::UInt32,
                inner: None,
            },
            DataType::UInt64 => Self {
                kind: DataTypeKind::UInt64,
                inner: None,
            },
            DataType::Float16 => Self {
                kind: DataTypeKind::Float16,
                inner: None,
            },
            DataType::Float32 => Self {
                kind: DataTypeKind::Float32,
                inner: None,
            },
            DataType::Float64 => Self {
                kind: DataTypeKind::Float64,
                inner: None,
            },
            DataType::Timestamp(tu, tz) => {
                let tz = match tz {
                    Some(tz) => tz,
                    None => "UTC",
                };
                Self {
                    kind: DataTypeKind::Float64,
                    inner: Some(json!({
                      "unit": tu_to_string(tu),
                      "timezone": tz,
                    })),
                }
            }
            DataType::Date32 => Self {
                kind: DataTypeKind::Date32,
                inner: None,
            },
            DataType::Date64 => Self {
                kind: DataTypeKind::Date64,
                inner: None,
            },
            DataType::Time32(tu) => Self {
                kind: DataTypeKind::Time32,
                inner: Some(json!({
                  "unit": tu_to_string(tu),
                })),
            },
            DataType::Time64(tu) => Self {
                kind: DataTypeKind::Time64,
                inner: Some(json!({
                  "unit": tu_to_string(tu),
                })),
            },
            DataType::Duration(tu) => Self {
                kind: DataTypeKind::Duration,
                inner: Some(json!({
                  "unit": tu_to_string(tu),
                })),
            },
            DataType::Interval(iu) => {
                let iu = match iu {
                    datafusion::arrow::datatypes::IntervalUnit::YearMonth => "YearMonth",
                    datafusion::arrow::datatypes::IntervalUnit::DayTime => "DayTime",
                    datafusion::arrow::datatypes::IntervalUnit::MonthDayNano => "MonthDayNano",
                };
                Self {
                    kind: DataTypeKind::Duration,
                    inner: Some(json!({
                      "interval": iu,
                    })),
                }
            }

            DataType::Binary => Self {
                kind: DataTypeKind::Binary,
                inner: None,
            },
            DataType::FixedSizeBinary(size) => Self {
                kind: DataTypeKind::FixedSizeBinary,
                inner: Some(json!({
                  "size": size,
                })),
            },
            DataType::LargeBinary => Self {
                kind: DataTypeKind::LargeBinary,
                inner: None,
            },
            DataType::Utf8 => Self {
                kind: DataTypeKind::Utf8,
                inner: None,
            },
            DataType::LargeUtf8 => Self {
                kind: DataTypeKind::LargeUtf8,
                inner: None,
            },
            DataType::List(field) => {
                let item = JsField::from(field);
                Self {
                    kind: DataTypeKind::List,
                    inner: Some(json!({
                      "item": item,
                    })),
                }
            }
            DataType::FixedSizeList(field, size) => {
                let item = JsField::from(field);
                Self {
                    kind: DataTypeKind::FixedSizeList,
                    inner: Some(json!({
                      "item": item,
                      "size": size,
                    })),
                }
            }
            DataType::LargeList(field) => {
                let item = JsField::from(field);
                Self {
                    kind: DataTypeKind::LargeList,
                    inner: Some(json!({
                      "item": item,
                    })),
                }
            }
            DataType::Struct(fields) => {
                let fields = fields.iter().map(JsField::from).collect::<Vec<_>>();
                Self {
                    kind: DataTypeKind::Struct,
                    inner: Some(json!({
                      "fields": fields,
                    })),
                }
            }
            DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Map(_, _)
            | DataType::RunEndEncoded(_, _) => todo!(),
        }
    }
}

#[napi(object)]
pub struct JsSchema {
    pub fields: Vec<JsField>,
    pub metadata: HashMap<String, String>,
}

impl From<SchemaRef> for JsSchema {
    fn from(value: SchemaRef) -> Self {
        let fields = value.fields.into_iter().map(JsField::from).collect();

        Self {
            fields,
            metadata: value.metadata.clone(),
        }
    }
}
#[napi(object)]
#[derive(Serialize, Deserialize)]
pub struct JsField {
    pub name: String,
    pub data_type: DataTypeKind,
    pub nullable: bool,
    pub dict_id: Option<i64>,
    pub dict_is_ordered: Option<bool>,
    pub metadata: HashMap<String, String>,
}

impl From<&FieldRef> for JsField {
    fn from(value: &FieldRef) -> Self {
        Self {
            name: value.name().clone(),
            data_type: value.data_type().into(),
            nullable: value.is_nullable(),
            dict_id: value.dict_id(),
            dict_is_ordered: value.dict_is_ordered(),
            metadata: value.metadata().clone(),
        }
    }
}

#[napi(string_enum)]
#[derive(Serialize, Deserialize)]
pub enum DataTypeKind {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Timestamp,
    Date32,
    Date64,
    Time32,
    Time64,
    Duration,
    Interval,
    Binary,
    FixedSizeBinary,
    LargeBinary,
    Utf8,
    LargeUtf8,
    List,
    FixedSizeList,
    LargeList,
    Struct,
    Union,
    Dictionary,
    Decimal128,
    Decimal256,
    Map,
    RunEndEncoded,
}

impl From<&DataType> for DataTypeKind {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::UInt8 => Self::UInt8,
            DataType::UInt16 => Self::UInt16,
            DataType::UInt32 => Self::UInt32,
            DataType::UInt64 => Self::UInt64,
            DataType::Float16 => Self::Float16,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Timestamp(_, _) => Self::Timestamp,
            DataType::Date32 => Self::Date32,
            DataType::Date64 => Self::Date64,
            DataType::Time32(_) => Self::Time32,
            DataType::Time64(_) => Self::Time64,
            DataType::Duration(_) => Self::Duration,
            DataType::Interval(_) => Self::Interval,
            DataType::Binary => Self::Binary,
            DataType::FixedSizeBinary(_) => Self::FixedSizeBinary,
            DataType::LargeBinary => Self::LargeBinary,
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::List(_) => Self::List,
            DataType::FixedSizeList(_, _) => Self::FixedSizeList,
            DataType::LargeList(_) => Self::LargeList,
            DataType::Struct(_) => Self::Struct,
            DataType::Union(_, _) => Self::Union,
            DataType::Dictionary(_, _) => Self::Dictionary,
            DataType::Decimal128(_, _) => Self::Decimal128,
            DataType::Decimal256(_, _) => Self::Decimal256,
            DataType::Map(_, _) => Self::Map,
            DataType::RunEndEncoded(_, _) => Self::RunEndEncoded,
        }
    }
}

fn tu_to_string(tu: &TimeUnit) -> &'static str {
    match tu {
        datafusion::arrow::datatypes::TimeUnit::Second => "s",
        datafusion::arrow::datatypes::TimeUnit::Millisecond => "ms",
        datafusion::arrow::datatypes::TimeUnit::Microsecond => "us",
        datafusion::arrow::datatypes::TimeUnit::Nanosecond => "ns",
    }
}
