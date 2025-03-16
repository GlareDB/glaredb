use std::sync::Arc;

use rayexec_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use super::scan_filter::ScanFilter;
use super::statistics::StatisticsValue;
use crate::arrays::datatype::DataType;
use crate::catalog::entry::CatalogEntry;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;
use crate::functions::table::PlannedTableFunction;

#[derive(Debug, Clone)]
pub struct TableScanSource {
    pub catalog: String,
    pub schema: String,
    pub source: Arc<CatalogEntry>,
    pub function: PlannedTableFunction,
}

impl PartialEq for TableScanSource {
    fn eq(&self, other: &Self) -> bool {
        self.catalog == other.catalog
            && self.schema == other.schema
            && self.source.name == other.source.name
    }
}

impl Eq for TableScanSource {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableFunctionScanSource {
    pub function: PlannedTableFunction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanSource {
    Table(TableScanSource),
    Function(TableFunctionScanSource),
}

impl ScanSource {
    pub fn into_function(self) -> PlannedTableFunction {
        match self {
            Self::Table(t) => t.function,
            Self::Function(f) => f.function,
        }
    }

    pub fn cardinality(&self) -> StatisticsValue<usize> {
        match self {
            Self::Table(table) => table.function.bind_state.cardinality,
            Self::Function(func) => func.function.bind_state.cardinality,
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
            ScanSource::Table(table) => {
                ent = ent.with_value(
                    "table",
                    format!("{}.{}.{}", table.catalog, table.schema, table.source.name),
                )
            }
            ScanSource::Function(func) => {
                ent = ent.with_value("function", func.function.name.to_string())
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
        if let ScanSource::Function(table_func) = &self.node.source {
            // TODO: Named args?
            for expr in &table_func.function.bind_state.input.positional {
                func(expr)?
            }
        }
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        if let ScanSource::Function(table_func) = &mut self.node.source {
            // TODO: Named args?
            for expr in &mut table_func.function.bind_state.input.positional {
                func(expr)?
            }
        }
        Ok(())
    }
}
