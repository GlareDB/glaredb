pub mod datasource;
pub mod plan;

#[cfg(test)]
mod tests {
    use crate::repr::compute::{VecCountAggregate, VecNumericAggregate};
    use crate::repr::df::groupby::Accumulator;
    use crate::repr::df::DataFrame;
    use crate::repr::expr::{BinaryOperation, ScalarExpr};
    use crate::repr::value::Value;
    use crate::runtime::datasource::{
        MemoryDataSource, ReadExecutor, ReadableSource, WriteableSource,
    };
    use crate::runtime::plan::builder::*;
    use anyhow::Result;
    use futures::{Stream, StreamExt};

    #[tokio::test]
    async fn values_cross_join_aggregate_project() {
        logutil::init_test();

        let plan = Builder::new()
            .values(vec![
                vec![
                    Value::Bool(Some(true)),
                    Value::Int8(Some(4)),
                    Value::Int8(Some(8)),
                ]
                .into(),
                vec![
                    Value::Bool(Some(true)),
                    Value::Int8(Some(5)),
                    Value::Int8(Some(9)),
                ]
                .into(),
            ])
            .unwrap()
            .debug()
            .unwrap()
            .cross_join(
                Builder::new()
                    .values(vec![
                        vec![Value::Int32(Some(16)), Value::Int32(Some(32))].into(),
                        vec![Value::Int32(Some(64)), Value::Int32(Some(128))].into(),
                    ])
                    .unwrap(),
            )
            .unwrap()
            .debug()
            .unwrap()
            .aggregate(
                Some(vec![1, 2]),
                vec![
                    Accumulator::first_value(1),
                    Accumulator::first_value(2),
                    Accumulator::with_func(3, VecNumericAggregate::sum_groups),
                ],
            )
            .unwrap()
            .debug()
            .unwrap()
            .project(vec![
                ScalarExpr::Binary {
                    op: BinaryOperation::Add,
                    left: ScalarExpr::Column(0).boxed(),
                    right: ScalarExpr::Constant(Value::Int8(Some(1))).boxed(),
                },
                ScalarExpr::Column(1),
                ScalarExpr::Column(2),
                ScalarExpr::Constant(Value::Utf8(Some("const".to_string()))),
            ])
            .unwrap()
            .debug()
            .unwrap()
            .into_read_plan()
            .unwrap();

        let source = MemoryDataSource::new();
        let stream = plan.execute_read(&source).await.unwrap();
        let df = DataFrame::from_dataframes(
            stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
                .unwrap(),
        )
        .unwrap();

        let expected = DataFrame::from_rows(vec![
            vec![
                Value::Int8(Some(5)),
                Value::Int8(Some(8)),
                Value::Int32(Some(80)),
                Value::Utf8(Some("const".to_string())),
            ]
            .into(),
            vec![
                Value::Int8(Some(6)),
                Value::Int8(Some(9)),
                Value::Int32(Some(80)),
                Value::Utf8(Some("const".to_string())),
            ]
            .into(),
        ])
        .unwrap();

        assert_eq!(expected, df);
    }
}
