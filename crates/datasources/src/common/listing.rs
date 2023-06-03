//! Module for providing virtual schema listings for data sources.
//!
//! Virtual listers can list schema and table information about the underlying
//! data source. These essentially provide a trimmed down information schema.
use std::{fmt::Display, str::FromStr, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{StringArray, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::TableProvider,
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType},
    physical_plan::{memory::MemoryExec, ExecutionPlan},
    scalar::ScalarValue,
};
use metastore::builtins::{VIRTUAL_CATALOG_SCHEMATA_TABLE, VIRTUAL_CATALOG_TABLES_TABLE};

use super::errors::{DatasourceCommonError, Result};

#[derive(Debug, Clone)]
pub struct VirtualTable {
    pub schema: String,
    pub table: String,
}

#[async_trait]
pub trait VirtualLister: Sync + Send {
    /// List schemas for a data source.
    async fn list_schemas(&self) -> Result<Vec<String>>;

    /// List tables for a data source.
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<VirtualTable>>;
}

#[derive(Debug, Clone, Copy)]
pub struct EmptyLister;

#[async_trait]
impl VirtualLister for EmptyLister {
    async fn list_schemas(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<VirtualTable>> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VirtualCatalogTable {
    Schemata,
    Tables,
}

impl Display for VirtualCatalogTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Schemata => VIRTUAL_CATALOG_SCHEMATA_TABLE,
            Self::Tables => VIRTUAL_CATALOG_TABLES_TABLE,
        };
        f.write_str(s)
    }
}

impl FromStr for VirtualCatalogTable {
    type Err = DatasourceCommonError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let catalog = match s {
            VIRTUAL_CATALOG_SCHEMATA_TABLE => Self::Schemata,
            VIRTUAL_CATALOG_TABLES_TABLE => Self::Tables,
            other => {
                return Err(DatasourceCommonError::UnknownVirtualCatalogTable(
                    other.to_owned(),
                ))
            }
        };
        Ok(catalog)
    }
}

pub struct VirtualCatalogTableProvider {
    lister: Box<dyn VirtualLister>,
    catalog: VirtualCatalogTable,
    schema: SchemaRef,
}

impl VirtualCatalogTableProvider {
    pub fn new(lister: Box<dyn VirtualLister>, catalog: VirtualCatalogTable) -> Self {
        Self {
            lister,
            catalog,
            schema: Self::make_schema(catalog),
        }
    }

    fn make_schema(catalog: VirtualCatalogTable) -> SchemaRef {
        let fields = match catalog {
            VirtualCatalogTable::Schemata => vec![Field::new("schema_name", DataType::Utf8, false)],
            VirtualCatalogTable::Tables => vec![
                Field::new("table_schema", DataType::Utf8, false),
                Field::new("table_name", DataType::Utf8, false),
            ],
        };
        Arc::new(Schema::new(fields))
    }
}

#[async_trait]
impl TableProvider for VirtualCatalogTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let supports = match self.catalog {
            VirtualCatalogTable::Schemata => {
                vec![TableProviderFilterPushDown::Unsupported; filters.len()]
            }
            VirtualCatalogTable::Tables => filters
                .iter()
                .map(|f| {
                    if get_schema_from_filter(f).is_some() {
                        TableProviderFilterPushDown::Inexact
                    } else {
                        TableProviderFilterPushDown::Unsupported
                    }
                })
                .collect(),
        };
        Ok(supports)
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batch = match self.catalog {
            VirtualCatalogTable::Schemata => {
                let schema_list = self
                    .lister
                    .list_schemas()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let schema_list: StringArray = schema_list.into_iter().map(Some).collect();

                RecordBatch::try_new(self.schema.clone(), vec![Arc::new(schema_list)])?
            }
            VirtualCatalogTable::Tables => {
                // Since we don't support any other filter for the table provider,
                // the first filter should be the one we can use to directly match
                // the schema to the lister.
                let schema = filters.first().map(get_schema_from_filter).unwrap_or(None);
                let schema = schema.as_deref();

                let list = self
                    .lister
                    .list_tables(schema)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let (mut schema_list, mut table_list): (StringBuilder, StringBuilder) = list
                    .into_iter()
                    .map(|v| (Some(v.schema), Some(v.table)))
                    .unzip();

                RecordBatch::try_new(
                    self.schema.clone(),
                    vec![
                        Arc::new(schema_list.finish()),
                        Arc::new(table_list.finish()),
                    ],
                )?
            }
        };

        let schema = batch.schema();
        let exec = MemoryExec::try_new(&[vec![batch]], schema, projection.cloned())?;
        Ok(Arc::new(exec))
    }
}

fn get_schema_from_filter(filter: &Expr) -> Option<String> {
    fn extract_schema(left: &Expr, right: &Expr) -> Option<String> {
        // Check if left is a column with "table_schema" column.
        match left {
            Expr::Column(col) if col.name == "table_schema" => {}
            _ => return None,
        };
        // Check if right is a literal with schema value and return it.
        match right {
            Expr::Literal(ScalarValue::Utf8(schema)) => schema.clone(),
            _ => None,
        }
    }

    match filter {
        Expr::BinaryExpr(expr) if expr.op == Operator::Eq => {
            // Here we need one of left or right to be column "table_schema"
            // and the other to be literal.
            if let Some(schema) = extract_schema(&expr.left, &expr.right) {
                return Some(schema);
            }
            if let Some(schema) = extract_schema(&expr.right, &expr.left) {
                return Some(schema);
            }
        }
        _ => {}
    };
    None
}
