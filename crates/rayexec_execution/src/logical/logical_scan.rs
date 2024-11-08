use std::sync::Arc;

use rayexec_bullet::datatype::DataType;
use rayexec_error::Result;

use super::binder::bind_context::{BindContext, TableRef};
use super::operator::{LogicalNode, Node};
use super::scan_filter::ScanFilter;
use super::statistics::StatisticsValue;
use crate::database::catalog_entry::CatalogEntry;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;
use crate::functions::table::PlannedTableFunction;

// TODO: Probably remove view from this.
// Maybe just split it all up.
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

impl ScanSource {
    pub fn cardinality(&self) -> StatisticsValue<usize> {
        match self {
            Self::Table { .. } => StatisticsValue::Unknown,
            Self::TableFunction { function } => function.cardinality(),
            Self::ExpressionList { rows } => StatisticsValue::Exact(rows.len()),
            Self::View { .. } => StatisticsValue::Unknown,
        }
    }
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
    ///
    /// Ascending order.
    pub projection: Vec<usize>,
    /// If we've pruned columns.
    ///
    /// If we did, that info will be passed into the data table.
    pub did_prune_columns: bool,
    /// Scan filters that have been pushed down.
    ///
    /// This represents some number of filters logically ANDed together.
    ///
    /// Currently scan filters are optional to be applied in the scan. At some
    /// point we should allow sources to determine what filters they can/can't
    /// use and push down accordingly. For now, a Filter operator remains in
    /// place directly above the scan with expressions representing the same
    /// filters applied here.
    pub scan_filters: Vec<ScanFilter>,
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
                .with_values("projection", &self.projection)
        }

        ent
    }
}

impl LogicalNode for Node<LogicalScan> {
    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        vec![self.node.table_ref]
    }

    fn for_each_expr<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        if let ScanSource::ExpressionList { rows } = &self.node.source {
            for row in rows {
                for expr in row {
                    func(expr)?;
                }
            }
        }
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        if let ScanSource::ExpressionList { rows } = &mut self.node.source {
            for row in rows {
                for expr in row {
                    func(expr)?;
                }
            }
        }
        Ok(())
    }
}
