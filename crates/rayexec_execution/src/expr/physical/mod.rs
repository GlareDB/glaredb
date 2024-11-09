pub mod planner;

pub mod case_expr;
pub mod cast_expr;
pub mod column_expr;
pub mod literal_expr;
pub mod scalar_function_expr;

use std::borrow::Cow;
use std::fmt;

use case_expr::PhysicalCaseExpr;
use cast_expr::PhysicalCastExpr;
use column_expr::PhysicalColumnExpr;
use literal_expr::PhysicalLiteralExpr;
use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::executor::scalar::SelectExecutor;
use rayexec_bullet::selection::SelectionVector;
use rayexec_error::{not_implemented, OptionExt, Result};
use rayexec_proto::ProtoConv;
use scalar_function_expr::PhysicalScalarFunctionExpr;

use crate::database::DatabaseContext;
use crate::functions::aggregate::PlannedAggregateFunction;
use crate::proto::DatabaseProtoConv;

#[derive(Debug, Clone)]
pub enum PhysicalScalarExpression {
    Case(PhysicalCaseExpr),
    Cast(PhysicalCastExpr),
    Column(PhysicalColumnExpr),
    Literal(PhysicalLiteralExpr),
    ScalarFunction(PhysicalScalarFunctionExpr),
}

impl PhysicalScalarExpression {
    pub fn eval<'a>(&self, batch: &'a Batch) -> Result<Cow<'a, Array>> {
        match self {
            Self::Case(e) => e.eval(batch),
            Self::Cast(e) => e.eval(batch),
            Self::Column(e) => e.eval(batch),
            Self::Literal(e) => e.eval(batch),
            Self::ScalarFunction(e) => e.eval(batch),
        }
    }

    /// Produce a selection vector for the batch using this expression.
    ///
    /// The selection vector will include row indices where the expression
    /// evaluates to true.
    pub fn select(&self, batch: &Batch) -> Result<SelectionVector> {
        let selected = self.eval(batch)?;

        let mut selection = SelectionVector::with_capacity(selected.logical_len());
        SelectExecutor::select(&selected, &mut selection)?;

        Ok(selection)
    }
}

impl fmt::Display for PhysicalScalarExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Case(expr) => expr.fmt(f),
            Self::Cast(expr) => expr.fmt(f),
            Self::Column(expr) => expr.fmt(f),
            Self::Literal(expr) => expr.fmt(f),
            Self::ScalarFunction(expr) => expr.fmt(f),
        }
    }
}

impl DatabaseProtoConv for PhysicalScalarExpression {
    type ProtoType = rayexec_proto::generated::physical_expr::PhysicalScalarExpression;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::physical_expr::physical_scalar_expression::Value;

        let value = match self {
            Self::Case(_) => not_implemented!("proto encode CASE"),
            Self::Cast(cast) => Value::Cast(Box::new(cast.to_proto_ctx(context)?)),
            Self::Column(cast) => Value::Column(cast.to_proto_ctx(context)?),
            Self::Literal(cast) => Value::Literal(cast.to_proto_ctx(context)?),
            Self::ScalarFunction(cast) => Value::Function(cast.to_proto_ctx(context)?),
        };

        Ok(Self::ProtoType { value: Some(value) })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        use rayexec_proto::generated::physical_expr::physical_scalar_expression::Value;

        Ok(match proto.value.required("value")? {
            Value::Column(proto) => {
                Self::Column(DatabaseProtoConv::from_proto_ctx(proto, context)?)
            }
            Value::Cast(proto) => Self::Cast(DatabaseProtoConv::from_proto_ctx(*proto, context)?),
            Value::Literal(proto) => {
                Self::Literal(DatabaseProtoConv::from_proto_ctx(proto, context)?)
            }
            Value::Function(proto) => {
                Self::ScalarFunction(DatabaseProtoConv::from_proto_ctx(proto, context)?)
            }
        })
    }
}

#[derive(Debug)]
pub struct PhysicalAggregateExpression {
    /// The function we'll be calling to produce the aggregate states.
    pub function: Box<dyn PlannedAggregateFunction>,
    /// Column expressions we're aggregating on.
    pub columns: Vec<PhysicalColumnExpr>,
    /// Output type of the aggregate.
    pub output_type: DataType,
    /// If inputs are distinct.
    pub is_distinct: bool,
    // TODO: Filter
}

impl PhysicalAggregateExpression {
    pub fn contains_column_idx(&self, column: usize) -> bool {
        self.columns.iter().any(|expr| expr.idx == column)
    }
}

impl DatabaseProtoConv for PhysicalAggregateExpression {
    type ProtoType = rayexec_proto::generated::physical_expr::PhysicalAggregateExpression;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            function: Some(self.function.to_proto_ctx(context)?),
            columns: self
                .columns
                .iter()
                .map(|c| c.to_proto_ctx(context))
                .collect::<Result<Vec<_>>>()?,
            output_type: Some(self.output_type.to_proto()?),
            is_distinct: self.is_distinct,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            function: DatabaseProtoConv::from_proto_ctx(
                proto.function.required("function")?,
                context,
            )?,
            columns: proto
                .columns
                .into_iter()
                .map(|c| DatabaseProtoConv::from_proto_ctx(c, context))
                .collect::<Result<Vec<_>>>()?,
            output_type: ProtoConv::from_proto(proto.output_type.required("output_type")?)?,
            is_distinct: proto.is_distinct,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalSortExpression {
    /// Column this expression is for.
    pub column: PhysicalColumnExpr,
    /// If sort should be descending.
    pub desc: bool,
    /// If nulls should be ordered first.
    pub nulls_first: bool,
}

impl DatabaseProtoConv for PhysicalSortExpression {
    type ProtoType = rayexec_proto::generated::physical_expr::PhysicalSortExpression;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            column: Some(self.column.to_proto_ctx(context)?),
            desc: self.desc,
            nulls_first: self.nulls_first,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            column: DatabaseProtoConv::from_proto_ctx(proto.column.required("column")?, context)?,
            desc: proto.desc,
            nulls_first: proto.nulls_first,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::scalar::comparison::GtImpl;

    #[test]
    fn select_some() {
        let batch = Batch::try_new([
            Array::from_iter([1, 4, 6, 9, 12]),
            Array::from_iter([2, 3, 8, 9, 10]),
        ])
        .unwrap();

        let expr = PhysicalScalarExpression::ScalarFunction(PhysicalScalarFunctionExpr {
            function: Box::new(GtImpl),
            inputs: vec![
                PhysicalScalarExpression::Column(PhysicalColumnExpr { idx: 0 }),
                PhysicalScalarExpression::Column(PhysicalColumnExpr { idx: 1 }),
            ],
        });

        let selection = expr.select(&batch).unwrap();
        let expected = SelectionVector::from_iter([1, 4]);

        assert_eq!(expected, selection)
    }

    #[test]
    fn select_none() {
        let batch = Batch::try_new([
            Array::from_iter([1, 2, 6, 9, 9]),
            Array::from_iter([2, 3, 8, 9, 10]),
        ])
        .unwrap();

        let expr = PhysicalScalarExpression::ScalarFunction(PhysicalScalarFunctionExpr {
            function: Box::new(GtImpl),
            inputs: vec![
                PhysicalScalarExpression::Column(PhysicalColumnExpr { idx: 0 }),
                PhysicalScalarExpression::Column(PhysicalColumnExpr { idx: 1 }),
            ],
        });

        let selection = expr.select(&batch).unwrap();
        let expected = SelectionVector::empty();

        assert_eq!(expected, selection)
    }
}
