use std::sync::Arc;

use glaredb_error::Result;

use super::binder::bind_context::BindContext;
use super::binder::table_list::TableRef;
use super::operator::{LogicalNode, Node};
use crate::catalog::entry::CatalogEntry;
use crate::explain::explainable::{EntryBuilder, ExplainConfig, ExplainEntry, Explainable};
use crate::expr::Expression;
use crate::functions::table::PlannedTableFunction;
use crate::statistics::value::StatisticsValue;
use crate::storage::scan_filter::ScanFilter;

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
    /// Table located in a catalog. This will already have a table function
    /// associated with it.
    Table(TableScanSource),
    /// Table function.
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

/// Information about scanning a single table ref.
// TODO: Remove types, names. Just used for explains, and we can handle that we
// generating the explained plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableScan {
    /// Table reference representing output of this scan.
    pub table_ref: TableRef,
    /// Positional column projections.
    ///
    /// Ascending order.
    ///
    /// An empty projection list is valid. In such cases, we should emit batches
    /// containing no columns but the correct number of rows.
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
}

/// Represents a scan from some source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalScan {
    /// Scan information for scanning the 'data' columns.
    pub data_scan: TableScan,
    /// Scan information for scanning the 'metadata' columns.
    ///
    /// If None, then there's no metadata to scan.
    pub meta_scan: Option<TableScan>,
    /// Source of the scan.
    pub source: Box<ScanSource>,
}

impl Explainable for LogicalScan {
    fn explain_entry(&self, conf: ExplainConfig) -> ExplainEntry {
        let mut builder = EntryBuilder::new("Scan", conf)
            .with_contextual_values("data_scan_filters", &self.data_scan.scan_filters)
            .with_value_if_verbose("data_table_ref", self.data_scan.table_ref)
            .with_values_if_verbose("data_projection", &self.data_scan.projection);

        if let Some(meta_scan) = &self.meta_scan {
            builder = builder
                .with_contextual_values("meta_scan_filters", &meta_scan.scan_filters)
                .with_value_if_verbose("meta_table_ref", &meta_scan.table_ref)
                .with_values_if_verbose("meta_projection", &meta_scan.projection);
        }

        match self.source.as_ref() {
            ScanSource::Table(table) => {
                builder = builder.with_value(
                    "table",
                    format!("{}.{}.{}", table.catalog, table.schema, table.source.name),
                )
            }
            ScanSource::Function(func) => {
                builder = builder.with_value("function", func.function.name.to_string())
            }
        }

        builder.build()
    }
}

impl LogicalNode for Node<LogicalScan> {
    fn name(&self) -> &'static str {
        "Scan"
    }

    fn get_output_table_refs(&self, _bind_context: &BindContext) -> Vec<TableRef> {
        vec![self.node.data_scan.table_ref]
    }

    fn for_each_expr<'a, F>(&'a self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a Expression) -> Result<()>,
    {
        if let ScanSource::Function(table_func) = self.node.source.as_ref() {
            // TODO: Named args?
            for expr in &table_func.function.bind_state.input.positional {
                func(expr)?
            }
        }
        for filter in &self.node.data_scan.scan_filters {
            func(&filter.expression)?;
        }
        if let Some(meta_scan) = &self.node.meta_scan {
            for filter in &meta_scan.scan_filters {
                func(&filter.expression)?;
            }
        }

        Ok(())
    }

    fn for_each_expr_mut<'a, F>(&'a mut self, mut func: F) -> Result<()>
    where
        F: FnMut(&'a mut Expression) -> Result<()>,
    {
        if let ScanSource::Function(table_func) = self.node.source.as_mut() {
            // TODO: Named args?
            for expr in &mut table_func.function.bind_state.input.positional {
                func(expr)?
            }
        }
        for filter in &mut self.node.data_scan.scan_filters {
            func(&mut filter.expression)?;
        }
        if let Some(meta_scan) = &mut self.node.meta_scan {
            for filter in &mut meta_scan.scan_filters {
                func(&mut filter.expression)?;
            }
        }

        Ok(())
    }
}
