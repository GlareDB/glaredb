use super::*;
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct DropTables {
    pub references: Vec<OwnedFullObjectReference>,
    pub if_exists: bool,
}

impl UserDefinedLogicalNodeCore for DropTables {
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
        write!(f, "DropTables")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for DropTables {
    type ProtoRepr = protogen::sqlexec::logical_plan::DropTables;
    const EXTENSION_NAME: &'static str = "DropTables";
    fn try_decode(
        proto: Self::ProtoRepr,
        _ctx: &SessionContext,
        _codec: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        let references = proto
            .references
            .into_iter()
            .map(|r| r.into())
            .collect::<Vec<_>>();

        Ok(Self {
            references,
            if_exists: proto.if_exists,
        })
    }
    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "DropTables::try_decode_extension: unsupported extension",
            )),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;
        let references = self
            .references
            .clone()
            .into_iter()
            .map(|r| r.into())
            .collect::<Vec<_>>();

        let drop_tables = protogen::DropTables {
            references,
            if_exists: self.if_exists,
        };
        let plan_type = protogen::LogicalPlanExtensionType::DropTables(drop_tables);

        let lp_extension = protogen::LogicalPlanExtension {
            inner: Some(plan_type),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
