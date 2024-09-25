use super::simple::{SimpleOperator, StatelessOperation};
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::expr::physical::PhysicalScalarExpression;
use crate::proto::DatabaseProtoConv;
use rayexec_bullet::{array::Array, batch::Batch, compute::filter::filter};
use rayexec_error::{OptionExt, RayexecError, Result};

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
        let selection = self.predicate.eval(&batch, None)?;
        let selection = match selection.as_ref() {
            Array::Boolean(arr) => arr,
            other => {
                return Err(RayexecError::new(format!(
                    "Expected filter predicate to evaluate to a boolean, got {}",
                    other.datatype()
                )))
            }
        };

        let filtered_arrays = if let Some(validity) = selection.validity() {
            // Need to account for nulls (which are falsy in this context).
            let mut bitmap = selection.values().clone();
            bitmap.bit_and_mut(validity)?;

            batch
                .columns()
                .iter()
                .map(|a| filter(a, &bitmap))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            // Can just use the values directly.
            batch
                .columns()
                .iter()
                .map(|a| filter(a, selection.values()))
                .collect::<Result<Vec<_>, _>>()?
        };

        let batch = if filtered_arrays.is_empty() {
            // If we're working on an empty input batch, just produce an new
            // empty batch with num rows equaling the number of trues in the
            // selection.
            Batch::empty_with_num_rows(selection.true_count())
        } else {
            // Otherwise use the actual filtered arrays.
            Batch::try_new(filtered_arrays)?
        };

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
