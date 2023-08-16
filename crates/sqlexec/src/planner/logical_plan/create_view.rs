use super::*;
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateView {
    pub view_name: OwnedTableReference,
    pub sql: String,
    pub columns: Vec<String>,
    pub or_replace: bool,
}

impl TryFrom<protogen::sqlexec::logical_plan::CreateView> for CreateView {
    type Error = ProtoConvError;

    fn try_from(value: protogen::sqlexec::logical_plan::CreateView) -> Result<Self, Self::Error> {
        let view_name = value
            .view_name
            .ok_or(ProtoConvError::RequiredField(
                "view_name is required".to_string(),
            ))?
            .try_into()?;

        Ok(CreateView {
            view_name,
            sql: value.sql,
            columns: value.columns,
            or_replace: value.or_replace,
        })
    }
}

impl UserDefinedLogicalNodeCore for CreateView {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &EMPTY_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", Self::EXTENSION_NAME)
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for CreateView {
    const EXTENSION_NAME: &'static str = "CreateView";

    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "CreateView::try_from_extension: unsupported extension",
            )),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;

        let proto = protogen::CreateView {
            view_name: Some(self.view_name.clone().into()),
            sql: self.sql.clone(),
            columns: self.columns.clone(),
            or_replace: self.or_replace,
        };

        let extension = protogen::LogicalPlanExtensionType::CreateView(proto);

        let lp_extension = protogen::LogicalPlanExtension {
            inner: Some(extension),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
