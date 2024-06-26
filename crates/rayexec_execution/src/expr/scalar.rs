use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast;
use std::fmt;

use crate::functions::scalar::{arith, boolean, comparison, negate, GenericScalarFunction};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UnaryOperator {
    IsTrue,
    IsFalse,
    IsNull,
    IsNotNull,
    Negate,
}

impl UnaryOperator {
    pub fn scalar_function(&self) -> &dyn GenericScalarFunction {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
        }
    }
}

impl BinaryOperator {
    /// Get the scalar function that represents this binary operator.
    pub fn scalar_function(&self) -> &dyn GenericScalarFunction {
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
