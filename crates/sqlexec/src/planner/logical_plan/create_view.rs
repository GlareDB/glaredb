use super::*;
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateView {
    pub view_reference: OwnedFullObjectReference,
    pub sql: String,
    pub columns: Vec<String>,
    pub or_replace: bool,
}

impl UserDefinedLogicalNodeCore for CreateView {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &GENERIC_OPERATION_LOGICAL_SCHEMA
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
    type ProtoRepr = protogen::sqlexec::logical_plan::CreateView;
    const EXTENSION_NAME: &'static str = "CreateView";

    fn try_decode(
        proto: Self::ProtoRepr,
        _ctx: &SessionContext,
        _codec: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        let reference = proto
            .reference
            .ok_or(ProtoConvError::RequiredField(
                "reference is required".to_string(),
            ))?
            .into();

        Ok(CreateView {
            view_reference: reference,
            sql: proto.sql,
            columns: proto.columns,
            or_replace: proto.or_replace,
        })
    }
    fn try_downcast_extension(extension: &LogicalPlanExtension) -> Result<Self> {
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
            reference: Some(self.view_reference.clone().into()),
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
