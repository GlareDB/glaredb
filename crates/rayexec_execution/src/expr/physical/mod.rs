pub mod evaluator;
pub mod planner;
pub mod selection_evaluator;

pub mod case_expr;
pub mod cast_expr;
pub mod column_expr;
pub mod literal_expr;
pub mod scalar_function_expr;

use std::fmt;

use case_expr::PhysicalCaseExpr;
use cast_expr::PhysicalCastExpr;
use column_expr::PhysicalColumnExpr;
use evaluator::ExpressionState;
use literal_expr::PhysicalLiteralExpr;
use rayexec_error::{not_implemented, OptionExt, Result};
use scalar_function_expr::PhysicalScalarFunctionExpr;

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
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

impl From<PhysicalCaseExpr> for PhysicalScalarExpression {
    fn from(value: PhysicalCaseExpr) -> Self {
        PhysicalScalarExpression::Case(value)
    }
}

impl From<PhysicalCastExpr> for PhysicalScalarExpression {
    fn from(value: PhysicalCastExpr) -> Self {
        PhysicalScalarExpression::Cast(value)
    }
}

impl From<PhysicalColumnExpr> for PhysicalScalarExpression {
    fn from(value: PhysicalColumnExpr) -> Self {
        PhysicalScalarExpression::Column(value)
    }
}

impl From<PhysicalLiteralExpr> for PhysicalScalarExpression {
    fn from(value: PhysicalLiteralExpr) -> Self {
        PhysicalScalarExpression::Literal(value)
    }
}

impl From<PhysicalScalarFunctionExpr> for PhysicalScalarExpression {
    fn from(value: PhysicalScalarFunctionExpr) -> Self {
        PhysicalScalarExpression::ScalarFunction(value)
    }
}

impl PhysicalScalarExpression {
    pub(crate) fn create_state(&self, batch_size: usize) -> Result<ExpressionState> {
        match self {
            Self::Case(expr) => expr.create_state(batch_size),
            Self::Cast(expr) => expr.create_state(batch_size),
            Self::Column(expr) => expr.create_state(batch_size),
            Self::Literal(expr) => expr.create_state(batch_size),
            Self::ScalarFunction(expr) => expr.create_state(batch_size),
        }
    }

    pub fn datatype(&self) -> DataType {
        match self {
            Self::Case(expr) => expr.datatype(),
            Self::Cast(expr) => expr.datatype(),
            Self::Column(expr) => expr.datatype(),
            Self::Literal(expr) => expr.datatype(),
            Self::ScalarFunction(expr) => expr.datatype(),
        }
    }

    // TODO: Remove, needs to happen after operator revamp.
    #[deprecated]
    pub fn eval(&self, batch: &Batch) -> Result<Array> {
        unimplemented!("expr eval")
        // match self {
        //     Self::Case(e) => e.eval2(batch),
        //     Self::Cast(e) => e.eval2(batch),
        //     Self::Column(e) => e.eval2(batch),
        //     Self::Literal(e) => e.eval2(batch),
        //     Self::ScalarFunction(e) => e.eval2(batch),
        // }
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
    pub function: PlannedAggregateFunction,
    /// Column expressions we're aggregating on.
    pub columns: Vec<PhysicalColumnExpr>,
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

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
        // Ok(Self::ProtoType {
        //     function: Some(self.function.to_proto_ctx(context)?),
        //     columns: self
        //         .columns
        //         .iter()
        //         .map(|c| c.to_proto_ctx(context))
        //         .collect::<Result<Vec<_>>>()?,
        //     output_type: Some(self.output_type.to_proto()?),
        //     is_distinct: self.is_distinct,
        // })
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
        // Ok(Self {
        //     function: DatabaseProtoConv::from_proto_ctx(
        //         proto.function.required("function")?,
        //         context,
        //     )?,
        //     columns: proto
        //         .columns
        //         .into_iter()
        //         .map(|c| DatabaseProtoConv::from_proto_ctx(c, context))
        //         .collect::<Result<Vec<_>>>()?,
        //     output_type: ProtoConv::from_proto(proto.output_type.required("output_type")?)?,
        //     is_distinct: proto.is_distinct,
        // })
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalSortExpression {
    /// Column this expression is for.
    pub column: PhysicalScalarExpression,
    /// If sort should be descending.
    pub desc: bool,
    /// If nulls should be ordered first.
    pub nulls_first: bool,
}

impl DatabaseProtoConv for PhysicalSortExpression {
    type ProtoType = rayexec_proto::generated::physical_expr::PhysicalSortExpression;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        unimplemented!()
        // Ok(Self::ProtoType {
        //     column: Some(self.column.to_proto_ctx(context)?),
        //     desc: self.desc,
        //     nulls_first: self.nulls_first,
        // })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
        // Ok(Self {
        //     column: DatabaseProtoConv::from_proto_ctx(proto.column.required("column")?, context)?,
        //     desc: proto.desc,
        //     nulls_first: proto.nulls_first,
        // })
    }
}
