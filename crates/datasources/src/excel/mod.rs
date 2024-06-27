use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;

use calamine::{DataType as CalamineDataType, Range, Reader, Sheets};
use datafusion::arrow::array::{
    ArrayRef,
    BooleanArray,
    Date64Array,
    NullArray,
    PrimitiveArray,
    StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use object_store::{ObjectMeta, ObjectStore};

use crate::common::url::DatasourceUrl;
use crate::object_store::{ObjStoreAccess, ObjStoreAccessor};

pub mod errors;
pub mod stream;
pub mod table;

use errors::ExcelError;

pub struct ExcelTable {
    cell_range: Range<calamine::Data>,
    has_header: bool,
}

impl ExcelTable {
    pub async fn open(
        store_access: Arc<dyn ObjStoreAccess>,
        source_url: DatasourceUrl,
        sheet_name: Option<String>,
        has_header: bool,
    ) -> Result<ExcelTable, ExcelError> {
        match source_url {
            DatasourceUrl::File(path) => {
                let path = ioutil::resolve_path(&path)?;
                let mut sheet = calamine::open_workbook_auto(path)?;

                let first_sheet = sheet_name.unwrap_or_else(|| {
                    let sheets = sheet.sheet_names();
                    sheets.first().expect("file has a sheet").to_owned()
                });

                Ok(ExcelTable {
                    cell_range: sheet.worksheet_range(&first_sheet)?,
                    has_header,
                })
            }

            DatasourceUrl::Url(_) => {
                let accessor = ObjStoreAccessor::new(store_access)?;

                let mut list = accessor.list_globbed(source_url.path()).await?;
                if list.is_empty() {
                    return Err(ExcelError::Load(
                        "could not find .xlsx file at remote".to_string(),
                    ));
                } else if list.len() > 1 {
                    return Err(ExcelError::Load(
                        "multi-file globs are not supported for .xlsx sources".to_string(),
                    ));
                };

                let meta = list
                    .pop()
                    .ok_or_else(|| ExcelError::Load("could not find first file".to_string()))?;

                let store = accessor.into_object_store();

                excel_table_from_object(store.as_ref(), meta, sheet_name, has_header).await
            }
        }
    }
}

pub async fn excel_table_from_object(
    store: &dyn ObjectStore,
    meta: ObjectMeta,
    sheet_name: Option<String>,
    has_header: bool,
) -> Result<ExcelTable, ExcelError> {
    let bs = store.get(&meta.location).await?.bytes().await?;

    let buffer = Cursor::new(bs);
    let mut sheets: Sheets<_> = calamine::open_workbook_auto_from_rs(buffer)?;

    let first_sheet = sheet_name.unwrap_or_else(|| {
        let sheets = sheets.sheet_names();
        sheets.first().expect("file has a sheet").to_owned()
    });

    Ok(ExcelTable {
        cell_range: sheets.worksheet_range(&first_sheet)?,
        has_header,
    })
}

// TODO: vectorize this to improve performance
// Ideally we can iterate over the columns instead of iterating over the rows
fn xlsx_sheet_value_to_record_batch(
    r: Range<calamine::Data>,
    has_header: bool,
    infer_schema_length: usize,
) -> Result<RecordBatch, ExcelError> {
    let schema = infer_schema(&r, has_header, infer_schema_length)?;
    let arrays = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let rows = if has_header {
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
                    rows.map(|r| r.get(i).and_then(|v| v.as_i64()))
                        .collect::<PrimitiveArray<Int64Type>>(),
                ) as ArrayRef,
                DataType::Float64 => Arc::new(
                    rows.map(|r| r.get(i).and_then(|v| v.as_f64()))
                        .collect::<PrimitiveArray<Float64Type>>(),
                ) as ArrayRef,
                DataType::Date64 => {
                    let mut arr = Date64Array::builder(rows.len());
                    for r in rows {
                        if let Some(v) = r.get(i).and_then(|v| v.as_datetime()) {
                            let v = v.and_utc().timestamp_millis();
                            arr.append_value(v);
                        } else {
                            arr.append_null();
                        }
                    }
                    Arc::new(arr.finish())
                }
                DataType::Null => Arc::new(NullArray::new(rows.len())),
                _ => Arc::new(
                    rows.map(|r| r.get(i).map(|v| v.as_string().unwrap_or_default()))
                        .collect::<StringArray>(),
                ) as ArrayRef,
            }
        })
        .collect::<Vec<ArrayRef>>();

    Ok(RecordBatch::try_new(Arc::new(schema), arrays)?)
}

pub fn infer_schema(
    r: &Range<calamine::Data>,
    has_header: bool,
    infer_schema_length: usize,
) -> Result<Schema, ExcelError> {
    let mut col_types: HashMap<&str, HashSet<DataType>> = HashMap::new();
    let mut rows = r.rows();
    let col_names: Vec<String> = if has_header {
        rows.next()
            .unwrap()
            .into_iter()
            .enumerate()
            .map(
                |(i, c)| match (has_header, c.get_string().map(|s| s.to_string())) {
                    (true, Some(s)) => Ok(s),
                    (true, None) => Err(ExcelError::Load("failed to parse header".to_string())),
                    (false, _) => Ok(format!("col{}", i)),
                },
            )
            .collect::<Result<_, _>>()?
    } else {
        (0..r.rows().next().unwrap_or_default().len())
            .map(|n| format!("{}", n))
            .collect()
    };


    for row in rows.take(infer_schema_length) {
        for (i, col_val) in row.iter().enumerate() {
            let col_name = col_names.get(i).unwrap();
            if let Ok(col_type) = infer_value_type(col_val) {
                let entry = col_types.entry(col_name).or_default();
                entry.insert(col_type);
            } else {
                continue;
            }
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

            let field_name = col_name.replace(' ', "_");

            if set.len() == 1 {
                Field::new(
                    field_name,
                    set.iter().next().cloned().unwrap_or(DataType::Utf8),
                    true,
                )
            } else if set.contains(&DataType::Utf8) {
                Field::new(field_name, DataType::Utf8, true)
            } else if set.contains(&DataType::Float64) {
                Field::new(field_name, DataType::Float64, true)
            } else if set.contains(&DataType::Int64) {
                Field::new(field_name, DataType::Int64, true)
            } else if set.contains(&DataType::Boolean) {
                Field::new(field_name, DataType::Boolean, true)
            } else if set.contains(&DataType::Null) {
                Field::new(field_name, DataType::Null, true)
            } else {
                Field::new(field_name, DataType::Utf8, true)
            }
        })
        .collect();

    Ok(Schema::new(fields))
}

fn infer_value_type(v: &calamine::Data) -> Result<DataType, ExcelError> {
    match v {
        calamine::Data::Int(_) => Ok(DataType::Int64),
        calamine::Data::Float(_) => Ok(DataType::Float64),
        calamine::Data::Bool(_) => Ok(DataType::Boolean),
        calamine::Data::String(_) => Ok(DataType::Utf8),
        // TODO: parsing value errors that we get from the calamine
        // library, could either become nulls or could be
        // errors. right now they are errors, and this should probably
        // be configurable, however...
        calamine::Data::Error(e) => Err(ExcelError::Load(e.to_string())),
        calamine::Data::DateTime(_) => Ok(DataType::Date64),
        calamine::Data::Empty => Ok(DataType::Null),
        _ => Err(ExcelError::Parse),
    }
}
