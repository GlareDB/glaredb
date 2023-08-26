use super::*;

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
        write!(f, "SetVariable")
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
    type ProtoRepr = protogen::sqlexec::logical_plan::SetVariable;
    const EXTENSION_NAME: &'static str = "SetVariable";
    fn try_decode(
        proto: Self::ProtoRepr,
        _ctx: &SessionContext,
        _codec: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        Ok(Self {
            variable: proto.variable,
            values: proto.values,
        })
    }
    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!("SetVariable::try_decode_extension failed",)),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;

        let proto = protogen::SetVariable {
            variable: self.variable.clone(),
            values: self.values.clone(),
        };
        let plan_type = protogen::LogicalPlanExtensionType::SetVariable(proto);

        let lp_extension = protogen::LogicalPlanExtension {
            inner: Some(plan_type),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
