//! Rewrite based optimizations.
//!
//! A query plan _must_ be valid even before rewriting. Rewriting should only
//! enable additional optimizations.
use super::read::*;
use anyhow::{Result};
use lemur::repr::expr::ScalarExpr;

#[derive(Debug)]
pub struct FilterPushdown;

impl FilterPushdown {
    pub fn rewrite(&self, plan: &mut ReadPlan) -> Result<()> {
        plan.transform_mut(
            &mut |plan| match plan {
                ReadPlan::Filter(Filter { input, predicate }) => {
                    Ok(self.rewrite_target(input, predicate))
                }
                _ => Ok(plan),
            },
            &mut Ok,
        )
    }

    fn rewrite_target(&self, input: Box<ReadPlan>, predicate: ScalarExpr) -> ReadPlan {
        if !matches!(input.as_ref(), ReadPlan::ScanSource(_)) {
            return ReadPlan::Filter(Filter { input, predicate });
        }
        match *input {
            ReadPlan::ScanSource(ScanSource {
                table,
                filter,
                schema,
            }) => {
                let filter = match filter {
                    Some(filter) => Some(filter.and(predicate)),
                    None => Some(predicate),
                };
                ReadPlan::ScanSource(ScanSource {
                    table,
                    filter,
                    schema,
                })
            }
            _ => unreachable!(), // The above matches prevents reaching this.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableReference;
    use lemur::repr::expr::BinaryOperation;
    use lemur::repr::value::{Value, ValueType};

    #[test]
    fn filter_pushdown_scan() {
        let mut plan = ReadPlan::Filter(Filter {
            predicate: ScalarExpr::Binary {
                op: BinaryOperation::Gt,
                left: ScalarExpr::Constant(Value::Int8(Some(10))).boxed(),
                right: ScalarExpr::Column(0).boxed(),
            },
            input: Box::new(ReadPlan::ScanSource(ScanSource {
                table: TableReference {
                    catalog: "catalog".to_string(),
                    schema: "schema".to_string(),
                    table: "table1".to_string(),
                },
                filter: None,
                schema: vec![ValueType::Int8, ValueType::Int8].into(),
            })),
        });

        let expected = ReadPlan::ScanSource(ScanSource {
            table: TableReference {
                catalog: "catalog".to_string(),
                schema: "schema".to_string(),
                table: "table1".to_string(),
            },
            filter: Some(ScalarExpr::Binary {
                op: BinaryOperation::Gt,
                left: ScalarExpr::Constant(Value::Int8(Some(10))).boxed(),
                right: ScalarExpr::Column(0).boxed(),
            }),
            schema: vec![ValueType::Int8, ValueType::Int8].into(),
        });

        FilterPushdown.rewrite(&mut plan).unwrap();
        assert_eq!(expected, plan);
    }
}
