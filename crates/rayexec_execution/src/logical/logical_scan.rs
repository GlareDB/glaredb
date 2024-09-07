use std::sync::Arc;

use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use rayexec_bullet::datatype::DataType;

use crate::{
    database::catalog_entry::CatalogEntry, expr::Expression, functions::table::PlannedTableFunction,
};

use super::{
    binder::bind_context::TableRef,
    operator::{LogicalNode, Node},
};

#[derive(Debug, Clone, PartialEq)]
pub enum ScanSource {
    Table {
        catalog: String,
        schema: String,
        source: Arc<CatalogEntry>,
    },
    TableFunction {
        function: Box<dyn PlannedTableFunction>,
    },
    ExpressionList {
        rows: Vec<Vec<Expression>>,
    },
    View {
        catalog: String,
        schema: String,
        source: Arc<CatalogEntry>,
    },
}

/// Represents a scan from some source.
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalScan {
    /// Table reference representing output of this scan.
    pub table_ref: TableRef,
    /// Types representing all columns from the source.
    pub types: Vec<DataType>,
    /// Names for all columns from the source.
    pub names: Vec<String>,
    /// Positional column projections.
    pub projection: Vec<usize>,
    /// Source of the scan.
    pub source: ScanSource,
}

impl Explainable for LogicalScan {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut ent = ExplainEntry::new("Scan")
            .with_values("column_names", &self.names)
            .with_values("column_types", &self.types);

        match &self.source {
            ScanSource::Table {
                catalog,
                schema,
                source,
            }
            | ScanSource::View {
                catalog,
                schema,
                source,
            } => ent = ent.with_value("source", format!("{catalog}.{schema}.{}", source.name)),
            ScanSource::TableFunction { function } => {
                ent = ent.with_value("function_name", function.table_function().name())
            }
            ScanSource::ExpressionList { rows } => {
                ent = ent.with_value("num_rows", rows.len());
            }
        }

        if conf.verbose {
            ent = ent
                .with_value("table_ref", self.table_ref)
                .with_values("projection", &self.projection);
        }

        ent
    }
}

impl LogicalNode for Node<LogicalScan> {
    fn get_output_table_refs(&self) -> Vec<TableRef> {
        vec![self.node.table_ref]
    }
}
