use std::sync::Arc;

use super::simple::{SimpleOperator, StatelessOperation};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalScalarExpression;
use crate::proto::DatabaseProtoConv;
use rayexec_bullet::batch::Batch;
use rayexec_error::{OptionExt, Result};

pub type PhysicalFilter = SimpleOperator<FilterOperation>;

#[derive(Debug)]
pub struct FilterOperation {
    predicate: PhysicalScalarExpression,
}

impl FilterOperation {
    pub fn new(predicate: PhysicalScalarExpression) -> Self {
        FilterOperation { predicate }
    }
}

impl StatelessOperation for FilterOperation {
    fn execute(&self, batch: Batch) -> Result<Batch> {
        let selection = self.predicate.select(&batch)?;
        let batch = batch.select(Arc::new(selection)); // TODO: Select mut

        Ok(batch)
    }
}

impl Explainable for FilterOperation {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Filter").with_value("predicate", &self.predicate)
    }
}

impl DatabaseProtoConv for PhysicalFilter {
    type ProtoType = rayexec_proto::generated::execution::PhysicalFilter;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            predicate: Some(self.operation.predicate.to_proto_ctx(context)?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            operation: FilterOperation {
                predicate: PhysicalScalarExpression::from_proto_ctx(
                    proto.predicate.required("predicate")?,
                    context,
                )?,
            },
        })
    }
}
