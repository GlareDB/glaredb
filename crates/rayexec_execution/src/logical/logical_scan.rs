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

// TODO: Probably remove view from this.
// Maybe just split it all up.
#[derive(Debug, Clone)]
pub enum ScanSource {
    Table {
        catalog: String,
        schema: String,
        source: Arc<CatalogEntry>,
    },
    View {
        catalog: String,
        schema: String,
        source: Arc<CatalogEntry>,
    },
}

// TODO: Remove...
impl PartialEq for ScanSource {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Table {
                    catalog: catalog_a,
                    schema: schema_a,
                    source: source_a,
                },
                Self::Table {
                    catalog: catalog_b,
                    schema: schema_b,
                    source: source_b,
                },
            ) => catalog_a == catalog_b && schema_a == schema_b && source_a.name == source_b.name,
            (
                Self::View {
                    catalog: catalog_a,
                    schema: schema_a,
                    source: source_a,
                },
                Self::View {
                    catalog: catalog_b,
                    schema: schema_b,
                    source: source_b,
                },
            ) => catalog_a == catalog_b && schema_a == schema_b && source_a.name == source_b.name,

            _ => false,
        }
    }
}

impl ScanSource {
    pub fn cardinality(&self) -> StatisticsValue<usize> {
        match self {
            Self::Table { .. } => StatisticsValue::Unknown,
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
        Ok(())
    }

    fn for_each_expr_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        Ok(())
    }
}
