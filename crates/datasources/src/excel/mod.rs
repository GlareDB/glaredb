use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use calamine::{open_workbook, DataType as CalamineDataType, Range, Reader, Sheets, Xlsx};
use datafusion::arrow::array::{ArrayRef, BooleanArray, Date64Array, PrimitiveArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use object_store::ObjectStore;

use crate::common::url::DatasourceUrl;
use crate::object_store::generic::GenericStoreAccess;
use crate::object_store::glob_util::get_resolved_patterns;
use crate::object_store::ObjStoreAccess;

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
        store_access: GenericStoreAccess,
        source_url: DatasourceUrl,
        sheet_name: Option<&str>,
        has_header: bool,
    ) -> Result<ExcelTable, ExcelError> {
        match source_url {
            DatasourceUrl::File(path) => {
                let path = ioutil::resolve_path(&path)?;
                let mut sheet = calamine::open_workbook_auto(path)?;

                let first_sheet = sheet_name.map(Cow::Borrowed).unwrap_or_else(|| {
                    let sheets = sheet.sheet_names();
                    let first = sheets.first().expect("file has a sheet");
                    Cow::Owned(first.clone())
                });

                Ok(ExcelTable {
                    cell_range: sheet.worksheet_range(&first_sheet)?,
                    has_header,
                })
            }

            DatasourceUrl::Url(_) => {
                let store = store_access.create_store()?;

                let path = source_url.path().into_owned();
                let paths = get_resolved_patterns(path);

                let mut list = Vec::new();
                for path in paths {
                    let sub_list = store_access.list_globbed(&store, path).await?;
                    list.extend(sub_list);
                }

                if list.is_empty() {
                    return Err(ExcelError::Load(
                        "could not find .xlsx file at remote".to_string(),
                    ));
                } else if list.len() > 1 {
                    return Err(ExcelError::Load(
                        "single file sheet on remote supported".to_string(),
                    ));
                };

                let meta = list.first().expect("remote file has a sheet");
                let bs = store.get(&meta.location).await?.bytes().await?;

                let buffer = Cursor::new(bs);
                let mut sheets: Sheets<_> = calamine::open_workbook_auto_from_rs(buffer).unwrap();

                let first_sheet = sheet_name.map(Cow::Borrowed).unwrap_or_else(|| {
                    let sheets = sheets.sheet_names();
                    let first = sheets.first().unwrap();
                    Cow::Owned(first.clone())
                });

                Ok(ExcelTable {
                    cell_range: sheets.worksheet_range(&first_sheet).unwrap(),
                    has_header,
                })
            }
        }
    }
}

pub async fn read_excel_impl(
    path: &PathBuf,
    sheet_name: Option<&str>,
    has_header: bool,
    infer_schema_length: usize,
) -> Result<MemTable, ExcelError> {
    let mut workbook: Xlsx<_> = open_workbook(path).unwrap();
    let sheet = sheet_name.map(Cow::Borrowed).unwrap_or_else(|| {
        let sheets = workbook.sheet_names();
        let first = sheets.first().expect("sheet is not empty");
        Cow::Owned(first.clone())
    });

    if let Ok(r) = workbook.worksheet_range(&sheet) {
        let batch = xlsx_sheet_value_to_record_batch(r, has_header, infer_schema_length)?;
        let schema_ref = batch.schema();
        let partitions = vec![vec![batch]];
        Ok(MemTable::try_new(schema_ref, partitions).unwrap())
    } else {
        Err(ExcelError::Load("Failed to open .xlsx file.".to_owned()))
    }
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

pub fn infer_schema(
    r: &Range<calamine::Data>,
    has_header: bool,
    infer_schema_length: usize,
) -> Result<Schema, ExcelError> {
    let mut col_types: HashMap<&str, HashSet<DataType>> = HashMap::new();
    let mut rows = r.rows();
    let col_names: Vec<String> = rows
        .next()
        .unwrap()
        .iter()
        .enumerate()
        .map(
            |(i, c)| match (has_header, c.get_string().map(|s| s.to_string())) {
                (true, Some(s)) => Ok(s),
                (true, None) => Err(ExcelError::Load("failed to parse header".to_string())),
                (false, _) => Ok(format!("col{}", i)),
            },
        )
        .collect::<Result<_, _>>()?;

    for row in rows.take(infer_schema_length) {
        for (i, col_val) in row.iter().enumerate() {
            let col_name = col_names.get(i).unwrap();
            if let Ok(col_type) = infer_value_type(col_val) {
                if col_type == DataType::Null {
                    continue;
                }
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

            let dt = set.iter().next().cloned().unwrap_or(DataType::Utf8);
            Field::new(col_name.replace(' ', "_"), dt, true)
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
        calamine::Data::Error(e) => Err(ExcelError::Load(e.to_string())),
        calamine::Data::DateTime(_) => Ok(DataType::Date64),
        calamine::Data::Empty => Ok(DataType::Null),
        _ => Err(ExcelError::Load(
            "Failed to parse the cell value".to_owned(),
        )),
    }
}
