use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use calamine::{open_workbook, Range, Reader, Xlsx};
use datafusion::arrow::{
    array::{ArrayRef, BooleanArray, Date64Array, PrimitiveArray, StringArray},
    datatypes::{DataType, Field, Float64Type, Int64Type, Schema},
    record_batch::RecordBatch,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to load XLSX: {msg}")]
    Load { msg: String },
    #[error("Failed to create record batch: {0}")]
    CreateRecordBatch(#[from] datafusion::arrow::error::ArrowError),
    #[error("Failed to open workbook: {0}")]
    OpenWorkbook(#[from] calamine::XlsxError),
}

fn infer_value_type(v: &calamine::DataType) -> Result<DataType, Error> {
    match v {
        calamine::DataType::Int(_) if v.get_int().is_some() => Ok(DataType::Int64),
        calamine::DataType::Float(_) if v.get_float().is_some() => Ok(DataType::Float64),
        calamine::DataType::Bool(_) if v.get_bool().is_some() => Ok(DataType::Boolean),
        calamine::DataType::String(_) if v.get_string().is_some() => Ok(DataType::Utf8),
        calamine::DataType::Error(e) => Err(Error::Load { msg: e.to_string() }),
        calamine::DataType::DateTime(_) => Ok(DataType::Date64),
        calamine::DataType::Empty => Ok(DataType::Null),
        _ => Err(Error::Load {
            msg: "Failed to parse the cell value".to_owned(),
        }),
    }
}

fn infer_schema(
    r: &Range<calamine::DataType>,
    has_header: Option<bool>,
    infer_schema_length: usize,
) -> Result<(Schema, bool), Error> {
    let mut col_types: HashMap<&str, HashSet<DataType>> = HashMap::new();
    let mut rows = r.rows();
    let mut skip_first = false;
    let col_names: Vec<String> = rows
        .next()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let s = c.get_string().map(|s| s.to_string());
            match (has_header, s) {
                (Some(true), Some(s)) => {
                    skip_first = true;
                    Ok(s)
                }
                (Some(true), None) => Err(Error::Load {
                    msg: "failed to parse header".to_string(),
                }),
                _ => Ok(format!("col{}", i)),
            }
        })
        .collect::<Result<_, _>>()?;

    for row in rows.take(infer_schema_length) {
        for (i, col_val) in row.iter().enumerate() {
            let col_name = col_names.get(i).unwrap();
            let col_type = infer_value_type(col_val).unwrap();
            if col_type == DataType::Null {
                continue;
            }
            let entry = col_types.entry(col_name).or_default();
            entry.insert(col_type);
        }
    }

    let fields: Vec<Field> = col_names
        .iter()
        .map(|col_name| {
            let set = col_types.entry(col_name).or_insert_with(|| {
                let mut set = HashSet::new();
                set.insert(DataType::Utf8);
                set
            });

            let mut dt_iter = set.iter().cloned();
            let dt = dt_iter.next().unwrap_or(DataType::Utf8);
            Field::new(col_name.replace(' ', "_"), dt, true)
        })
        .collect();
    Ok((Schema::new(fields), skip_first))
}

// TODO: vectorize this to improve performance
// Ideally we can iterate over the columns instead of iterating over the rows
fn xlsx_sheet_value_to_record_batch(
    r: Range<calamine::DataType>,
    has_header: Option<bool>,
    infer_schema_length: usize,
) -> Result<RecordBatch, Error> {
    let (schema, should_skip) = infer_schema(&r, has_header, infer_schema_length)?;
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let rows = if should_skip {
                r.rows().skip(1)
            } else {
                // Rows doesn't behave like a normal iterator here, so we need to skip `0` rows
                // just so we can get Skip<Rows>
                #[allow(clippy::iter_skip_zero)]
                r.rows().skip(0)
            };

            match field.data_type() {
                DataType::Boolean => Arc::new(
                    rows.map(|r| r.get(i).and_then(|v| v.get_bool()))
                        .collect::<BooleanArray>(),
                ) as ArrayRef,
                DataType::Int64 => Arc::new(
                    rows.map(|r| r.get(i).and_then(|v| v.get_int()))
                        .collect::<PrimitiveArray<Int64Type>>(),
                ) as ArrayRef,
                DataType::Float64 => Arc::new(
                    rows.map(|r| r.get(i).and_then(|v| v.get_float()))
                        .collect::<PrimitiveArray<Float64Type>>(),
                ) as ArrayRef,
                DataType::Date64 => {
                    let mut arr = Date64Array::builder(rows.len());
                    for r in rows {
                        if let Some(v) = r.get(i).and_then(|v| v.as_datetime()) {
                            let v = v.timestamp_millis();
                            arr.append_value(v);
                        } else {
                            arr.append_null();
                        }
                    }
                    Arc::new(arr.finish())
                }
                _ => Arc::new(
                    rows.map(|r| r.get(i).map(|v| v.get_string().unwrap_or("null")))
                        .collect::<StringArray>(),
                ) as ArrayRef,
            }
        })
        .collect::<Vec<ArrayRef>>();

    Ok(RecordBatch::try_new(Arc::new(schema), arrays)?)
}
pub async fn read_excel_impl(
    path: &PathBuf,
    sheet_name: Option<&str>,
    has_header: Option<bool>,
    infer_schema_length: usize,
) -> Result<datafusion::datasource::MemTable, Error> {
    let mut workbook: Xlsx<_> = open_workbook(path)?;
    let sheet = sheet_name.map(Cow::Borrowed).unwrap_or_else(|| {
        let sheets = workbook.sheet_names();
        let first = sheets.get(0).unwrap();
        Cow::Owned(first.clone())
    });

    if let Ok(r) = workbook.worksheet_range(&sheet) {
        let batch = xlsx_sheet_value_to_record_batch(r, has_header, infer_schema_length)?;
        let schema_ref = batch.schema();
        let partitions = vec![vec![batch]];
        Ok(datafusion::datasource::MemTable::try_new(schema_ref, partitions).unwrap())
    } else {
        Err(Error::Load {
            msg: "Failed to open .xlsx file.".to_owned(),
        })
    }
}
