use std::fmt::Debug;
use std::hash::Hash;

use datafusion::prelude::SessionContext;
use once_cell::sync::Lazy;
use protogen::metastore::types::catalog::TableEntry;

use super::*;

use crate::planner::physical_plan::insert::INSERT_COUNT_SCHEMA;

pub static INSERT_LOGICAL_SCHEMA: Lazy<DFSchemaRef> =
    Lazy::new(|| Arc::new(INSERT_COUNT_SCHEMA.as_ref().clone().try_into().unwrap()));

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Insert {
    pub source: DfLogicalPlan,
    pub table: TableEntry,
}

impl UserDefinedLogicalNodeCore for Insert {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![&self.source]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &INSERT_LOGICAL_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", Self::EXTENSION_NAME)
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for Insert {
    type ProtoRepr = protogen::sqlexec::logical_plan::Insert;
    const EXTENSION_NAME: &'static str = "Insert";

    fn try_decode(
        _proto: Self::ProtoRepr,
        _ctx: &SessionContext,
        _codec: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        unimplemented!()
    }

    fn try_decode_extension(_extension: &datafusion::logical_expr::Extension) -> Result<Self> {
        unimplemented!()
    }

    fn try_encode(&self, _buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        unimplemented!()
    }
}
