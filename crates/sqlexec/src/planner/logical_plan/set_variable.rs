use super::{
    ast, internal, DFSchemaRef, DfLogicalPlan, ExtensionNode, Result, UserDefinedLogicalNodeCore,
    GENERIC_OPERATION_LOGICAL_SCHEMA,
};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct SetVariable {
    pub variable: String,
    pub values: String,
}

impl SetVariable {
    pub fn try_new(variable: impl Into<String>, values: Vec<ast::Expr>) -> Result<Self> {
        let expr_to_string = |expr: &ast::Expr| {
            Ok(match expr {
                ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_) => expr.to_string(),
                ast::Expr::Value(ast::Value::SingleQuotedString(s)) => s.clone(),
                ast::Expr::Value(ast::Value::DoubleQuotedString(s)) => format!("\"{}\"", s),
                ast::Expr::Value(ast::Value::UnQuotedString(s)) => s.clone(),
                ast::Expr::Value(ast::Value::Number(s, _)) => s.clone(),
                ast::Expr::Value(v) => v.to_string(),
                other => return Err(internal!("invalid expression for SET var: {:}", other)),
            })
        };
        let values = values
            .iter()
            .map(expr_to_string)
            .collect::<Result<Vec<_>>>()?
            .join(",");

        Ok(Self {
            variable: variable.into(),
            values,
        })
    }
}

impl UserDefinedLogicalNodeCore for SetVariable {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &GENERIC_OPERATION_LOGICAL_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SET {:} = {:}", self.variable, self.values)
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for SetVariable {
    const EXTENSION_NAME: &'static str = "SetVariable";
}
