use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::functions::scalar::{
    arith, boolean, comparison, concat, like, negate, PlannedScalarFunction, ScalarFunction,
};

#[derive(Debug, Clone, PartialEq)]
pub struct PlannedUnaryOperator {
    pub op: UnaryOperator,
    pub scalar: Box<dyn PlannedScalarFunction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOperator {
    IsTrue,
    IsFalse,
    IsNull,
    IsNotNull,
    Negate,
}

impl UnaryOperator {
    pub fn scalar_function(&self) -> &dyn ScalarFunction {
        match self {
            Self::Negate => &negate::Negate,
            other => unimplemented!("{other}"),
        }
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IsTrue => write!(f, "IS TRUE"),
            Self::IsFalse => write!(f, "IS FALSE"),
            Self::IsNull => write!(f, "IS NULL"),
            Self::IsNotNull => write!(f, "IS NOT NULL"),
            Self::Negate => write!(f, "-"),
        }
    }
}

impl ProtoConv for UnaryOperator {
    type ProtoType = rayexec_proto::generated::logical::UnaryOperator;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(match self {
            Self::IsTrue => Self::ProtoType::UnaryIsTrue,
            Self::IsFalse => Self::ProtoType::UnaryIsFalse,
            Self::IsNull => Self::ProtoType::UnaryIsNull,
            Self::IsNotNull => Self::ProtoType::UnaryIsNotNull,
            Self::Negate => Self::ProtoType::UnaryNegate,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(match proto {
            Self::ProtoType::InvalidUnaryOperator => return Err(RayexecError::new("invalid")),
            Self::ProtoType::UnaryIsTrue => Self::IsTrue,
            Self::ProtoType::UnaryIsFalse => Self::IsFalse,
            Self::ProtoType::UnaryIsNull => Self::IsNull,
            Self::ProtoType::UnaryIsNotNull => Self::IsNotNull,
            Self::ProtoType::UnaryNegate => Self::Negate,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlannedBinaryOperator {
    pub op: BinaryOperator,
    pub scalar: Box<dyn PlannedScalarFunction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryOperator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    And,
    Or,
    StringConcat,
    StringStartsWith,
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eq => write!(f, "="),
            Self::NotEq => write!(f, "!="),
            Self::Lt => write!(f, "<"),
            Self::LtEq => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::GtEq => write!(f, ">="),
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
            Self::Multiply => write!(f, "*"),
            Self::Divide => write!(f, "/"),
            Self::Modulo => write!(f, "%"),
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
            Self::StringConcat => write!(f, "||"),
            Self::StringStartsWith => write!(f, "^@"),
        }
    }
}

impl BinaryOperator {
    /// Get the scalar function that represents this binary operator.
    pub fn scalar_function(&self) -> &dyn ScalarFunction {
        match self {
            Self::Eq => &comparison::Eq,
            Self::NotEq => &comparison::Neq,
            Self::Lt => &comparison::Lt,
            Self::LtEq => &comparison::LtEq,
            Self::Gt => &comparison::Gt,
            Self::GtEq => &comparison::GtEq,
            Self::Plus => &arith::Add,
            Self::Minus => &arith::Sub,
            Self::Multiply => &arith::Mul,
            Self::Divide => &arith::Div,
            Self::Modulo => &arith::Rem,
            Self::And => &boolean::And,
            Self::Or => &boolean::Or,
            Self::StringConcat => &concat::Concat,
            Self::StringStartsWith => &like::StartsWith,
        }
    }
}

impl TryFrom<ast::BinaryOperator> for BinaryOperator {
    type Error = RayexecError;
    fn try_from(value: ast::BinaryOperator) -> Result<Self> {
        Ok(match value {
            ast::BinaryOperator::Plus => BinaryOperator::Plus,
            ast::BinaryOperator::Minus => BinaryOperator::Minus,
            ast::BinaryOperator::Multiply => BinaryOperator::Multiply,
            ast::BinaryOperator::Divide => BinaryOperator::Divide,
            ast::BinaryOperator::Modulo => BinaryOperator::Modulo,
            ast::BinaryOperator::Eq => BinaryOperator::Eq,
            ast::BinaryOperator::NotEq => BinaryOperator::NotEq,
            ast::BinaryOperator::Gt => BinaryOperator::Gt,
            ast::BinaryOperator::GtEq => BinaryOperator::GtEq,
            ast::BinaryOperator::Lt => BinaryOperator::Lt,
            ast::BinaryOperator::LtEq => BinaryOperator::LtEq,
            ast::BinaryOperator::And => BinaryOperator::And,
            ast::BinaryOperator::Or => BinaryOperator::Or,
            ast::BinaryOperator::StringConcat => BinaryOperator::StringConcat,
            ast::BinaryOperator::StringStartsWith => BinaryOperator::StringStartsWith,
            other => {
                return Err(RayexecError::new(format!(
                    "Unsupported SQL operator: {other:?}"
                )))
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VariadicOperator {
    And,
    Or,
}

impl fmt::Display for VariadicOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}