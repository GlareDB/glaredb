use rayexec_error::Result;
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::database::catalog_entry::CatalogEntry;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CteIndex(pub usize);

/// Table or CTE found in the FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundTableOrCteReference {
    /// Resolved table.
    Table {
        catalog: String,
        schema: String,
        entry: Arc<CatalogEntry>,
    },
    /// Resolved CTE.
    Cte {
        /// Index of the cte in the bind data.
        cte_idx: CteIndex,
    },
}

impl ProtoConv for BoundTableOrCteReference {
    type ProtoType = rayexec_proto::generated::binder::BoundTableOrCteReference;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        // use rayexec_proto::generated::binder::{
        //     bound_table_or_cte_reference::Value, BoundCteReference, BoundTableReference,
        // };

        // let value = match self {
        //     Self::Table {
        //         catalog,
        //         schema,
        //         entry,
        //     } => Value::Table(BoundTableReference {
        //         catalog: catalog.clone(),
        //         schema: schema.clone(),
        //         table: Some(entry.to_proto()?),
        //     }),
        //     Self::Cte { cte_idx } => Value::Cte(BoundCteReference {
        //         idx: cte_idx.0 as u32,
        //     }),
        // };

        // Ok(Self::ProtoType { value: Some(value) })
        unimplemented!()
    }

    fn from_proto(_proto: Self::ProtoType) -> Result<Self> {
        // use rayexec_proto::generated::binder::bound_table_or_cte_reference::Value;

        unimplemented!()
        // Ok(match proto.value.required("value")? {
        //     Value::Table(table) => Self::Table {
        //         catalog: table.catalog,
        //         schema: table.schema,
        //         entry: TableEntry::from_proto(table.table.required("table")?)?,
        //     },
        //     Value::Cte(cte) => Self::Cte {
        //         cte_idx: CteIndex(cte.idx as usize),
        //     },
        // })
    }
}
